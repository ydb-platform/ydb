# mdiff.py - diff and patch routines for mercurial
#
# Copyright 2005, 2006 Olivia Mackall <olivia@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import re
import struct
import typing
import zlib

from typing import (
    Callable,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    cast,
)

from .i18n import _
from . import (
    diffhelper,
    encoding,
    error,
    policy,
    pycompat,
    util,
)
from .interfaces import (
    modules as intmod,
)

from .utils import dateutil

bdiff: intmod.BDiff = policy.importmod('bdiff')
mpatch: intmod.MPatch = policy.importmod('mpatch')

blocks = bdiff.blocks
fixws = bdiff.fixws
patches = mpatch.patches
patchedsize = mpatch.patchedsize
textdiff = bdiff.bdiff
splitnewlines = bdiff.splitnewlines

if typing.TYPE_CHECKING:
    HunkLines = list[bytes]
    """Lines of a hunk- a header, followed by line additions and deletions."""

    HunkRange = tuple[int, int, int, int]
    """HunkRange represents the range information of a hunk.

    The tuple (s1, l1, s2, l2) forms the header '@@ -s1,l1 +s2,l2 @@'."""

    Range = tuple[int, int]
    """A (lowerbound, upperbound) range tuple."""

    TypedBlock = tuple[intmod.BDiffBlock, bytes]
    """A bdiff block with its type."""


# TODO: this looks like it could be an attrs, which might help pytype
class diffopts:
    """context is the number of context lines
    text treats all files as text
    showfunc enables diff -p output
    git enables the git extended patch format
    nodates removes dates from diff headers
    nobinary ignores binary files
    noprefix disables the 'a/' and 'b/' prefixes (ignored in plain mode)
    ignorews ignores all whitespace changes in the diff
    ignorewsamount ignores changes in the amount of whitespace
    ignoreblanklines ignores changes whose lines are all blank
    upgrade generates git diffs to avoid data loss
    """

    _HAS_DYNAMIC_ATTRIBUTES = True

    defaults = {
        b'context': 3,
        b'text': False,
        b'showfunc': False,
        b'git': False,
        b'nodates': False,
        b'nobinary': False,
        b'noprefix': False,
        b'index': 0,
        b'ignorews': False,
        b'ignorewsamount': False,
        b'ignorewseol': False,
        b'ignoreblanklines': False,
        b'upgrade': False,
        b'showsimilarity': False,
        b'worddiff': False,
        b'xdiff': False,
    }

    def __init__(self, **opts):
        opts = pycompat.byteskwargs(opts)
        for k in self.defaults.keys():
            v = opts.get(k)
            if v is None:
                v = self.defaults[k]
            setattr(self, pycompat.sysstr(k), v)

        try:
            self.context = int(self.context)
        except ValueError:
            raise error.InputError(
                _(b'diff context lines count must be an integer, not %r')
                % pycompat.bytestr(self.context)
            )

    def copy(self, **kwargs):
        opts = {k: getattr(self, pycompat.sysstr(k)) for k in self.defaults}
        opts = pycompat.strkwargs(opts)
        opts.update(kwargs)
        return diffopts(**opts)

    def __bytes__(self):
        return b", ".join(
            b"%s: %r" % (k, getattr(self, pycompat.sysstr(k)))
            for k in self.defaults
        )

    __str__ = encoding.strmethod(__bytes__)


defaultopts = diffopts()


def wsclean(opts: diffopts, text: bytes, blank: bool = True) -> bytes:
    if opts.ignorews:
        text = bdiff.fixws(text, True)
    elif opts.ignorewsamount:
        text = bdiff.fixws(text, False)
    if blank and opts.ignoreblanklines:
        text = re.sub(b'\n+', b'\n', text).strip(b'\n')
    if opts.ignorewseol:
        text = re.sub(br'[ \t\r\f]+\n', br'\n', text)
    return text


def splitblock(
    base1: int,
    lines1: Iterable[bytes],
    base2: int,
    lines2: Iterable[bytes],
    opts: diffopts,
) -> Iterable[TypedBlock]:
    # The input lines matches except for interwoven blank lines. We
    # transform it into a sequence of matching blocks and blank blocks.
    lines1 = [(wsclean(opts, l) and 1 or 0) for l in lines1]
    lines2 = [(wsclean(opts, l) and 1 or 0) for l in lines2]
    s1, e1 = 0, len(lines1)
    s2, e2 = 0, len(lines2)
    while s1 < e1 or s2 < e2:
        i1, i2, btype = s1, s2, b'='
        if i1 >= e1 or lines1[i1] == 0 or i2 >= e2 or lines2[i2] == 0:
            # Consume the block of blank lines
            btype = b'~'
            while i1 < e1 and lines1[i1] == 0:
                i1 += 1
            while i2 < e2 and lines2[i2] == 0:
                i2 += 1
        else:
            # Consume the matching lines
            while i1 < e1 and lines1[i1] == 1 and lines2[i2] == 1:
                i1 += 1
                i2 += 1
        yield (base1 + s1, base1 + i1, base2 + s2, base2 + i2), btype
        s1 = i1
        s2 = i2


def hunkinrange(hunk: tuple[int, int], linerange: Range) -> bool:
    """Return True if `hunk` defined as (start, length) is in `linerange`
    defined as (lowerbound, upperbound).

    >>> hunkinrange((5, 10), (2, 7))
    True
    >>> hunkinrange((5, 10), (6, 12))
    True
    >>> hunkinrange((5, 10), (13, 17))
    True
    >>> hunkinrange((5, 10), (3, 17))
    True
    >>> hunkinrange((5, 10), (1, 3))
    False
    >>> hunkinrange((5, 10), (18, 20))
    False
    >>> hunkinrange((5, 10), (1, 5))
    False
    >>> hunkinrange((5, 10), (15, 27))
    False
    """
    start, length = hunk
    lowerbound, upperbound = linerange
    return lowerbound < start + length and start < upperbound


def blocksinrange(
    blocks: Iterable[TypedBlock], rangeb: Range
) -> tuple[list[TypedBlock], Range]:
    """filter `blocks` like (a1, a2, b1, b2) from items outside line range
    `rangeb` from ``(b1, b2)`` point of view.

    Return `filteredblocks, rangea` where:

    * `filteredblocks` is list of ``block = (a1, a2, b1, b2), stype`` items of
      `blocks` that are inside `rangeb` from ``(b1, b2)`` point of view; a
      block ``(b1, b2)`` being inside `rangeb` if
      ``rangeb[0] < b2 and b1 < rangeb[1]``;
    * `rangea` is the line range w.r.t. to ``(a1, a2)`` parts of `blocks`.
    """
    lbb, ubb = rangeb
    lba, uba = None, None
    filteredblocks = []
    for block in blocks:
        (a1, a2, b1, b2), stype = block
        if lbb >= b1 and ubb <= b2 and stype == b'=':
            # rangeb is within a single "=" hunk, restrict back linerange1
            # by offsetting rangeb
            lba = lbb - b1 + a1
            uba = ubb - b1 + a1
        else:
            if b1 <= lbb < b2:
                if stype == b'=':
                    lba = a2 - (b2 - lbb)
                else:
                    lba = a1
            if b1 < ubb <= b2:
                if stype == b'=':
                    uba = a1 + (ubb - b1)
                else:
                    uba = a2
        if hunkinrange((b1, (b2 - b1)), rangeb):
            filteredblocks.append(block)
    if lba is None or uba is None or uba < lba:
        raise error.InputError(_(b'line range exceeds file size'))
    return filteredblocks, (lba, uba)


def chooseblocksfunc(opts: diffopts | None = None) -> intmod.BDiffBlocksFnc:
    if (
        opts is None
        or not opts.xdiff
        or not getattr(bdiff, 'xdiffblocks', None)
    ):
        return bdiff.blocks
    else:
        return bdiff.xdiffblocks


def allblocks(
    text1: bytes,
    text2: bytes,
    opts: diffopts | None = None,
    lines1: Sequence[bytes] | None = None,
    lines2: Sequence[bytes] | None = None,
) -> Iterable[TypedBlock]:
    """Return (block, type) tuples, where block is an mdiff.blocks
    line entry. type is '=' for blocks matching exactly one another
    (bdiff blocks), '!' for non-matching blocks and '~' for blocks
    matching only after having filtered blank lines.
    line1 and line2 are text1 and text2 split with splitnewlines() if
    they are already available.
    """
    if opts is None:
        opts = defaultopts
    if opts.ignorews or opts.ignorewsamount or opts.ignorewseol:
        text1 = wsclean(opts, text1, False)
        text2 = wsclean(opts, text2, False)
    diff = chooseblocksfunc(opts)(text1, text2)
    for i, s1 in enumerate(diff):
        # The first match is special.
        # we've either found a match starting at line 0 or a match later
        # in the file.  If it starts later, old and new below will both be
        # empty and we'll continue to the next match.
        if i > 0:
            s = diff[i - 1]
        else:
            s = (0, 0, 0, 0)
        s = (s[1], s1[0], s[3], s1[2])

        # bdiff sometimes gives huge matches past eof, this check eats them,
        # and deals with the special first match case described above
        if s[0] != s[1] or s[2] != s[3]:
            type = b'!'
            if opts.ignoreblanklines:
                if lines1 is None:
                    lines1 = splitnewlines(text1)
                if lines2 is None:
                    lines2 = splitnewlines(text2)
                old = wsclean(opts, b"".join(lines1[s[0] : s[1]]))
                new = wsclean(opts, b"".join(lines2[s[2] : s[3]]))
                if old == new:
                    type = b'~'
            yield s, type
        yield s1, b'='


def unidiff(
    a: bytes,
    ad: bytes,
    b: bytes,
    bd: bytes,
    fn1: bytes,
    fn2: bytes,
    binary: bool,
    opts: diffopts = defaultopts,
) -> tuple[list[bytes], Iterable[tuple[HunkRange | None, HunkLines]]]:
    """Return a unified diff as a (headers, hunks) tuple.

    If the diff is not null, `headers` is a list with unified diff header
    lines "--- <original>" and "+++ <new>" and `hunks` is a generator yielding
    (hunkrange, hunklines) coming from _unidiff().
    Otherwise, `headers` and `hunks` are empty.

    Set binary=True if either a or b should be taken as a binary file.
    """

    def datetag(date: bytes, fn: bytes | None = None):
        if not opts.git and not opts.nodates:
            return b'\t%s' % date
        if fn and b' ' in fn:
            return b'\t'
        return b''

    sentinel = [], ()
    if not a and not b:
        return sentinel

    if opts.noprefix:
        aprefix = bprefix = b''
    else:
        aprefix = b'a/'
        bprefix = b'b/'

    epoch = dateutil.datestr((0, 0))

    fn1 = util.pconvert(fn1)
    fn2 = util.pconvert(fn2)

    if binary:
        if a and b and len(a) == len(b) and a == b:
            return sentinel
        headerlines = []
        hunks = ((None, [b'Binary file %s has changed\n' % fn1]),)
    elif not a:
        without_newline = not b.endswith(b'\n')
        b = splitnewlines(b)
        if a is None:
            l1 = b'--- /dev/null%s' % datetag(epoch)
        else:
            l1 = b"--- %s%s%s" % (aprefix, fn1, datetag(ad, fn1))
        l2 = b"+++ %s%s" % (bprefix + fn2, datetag(bd, fn2))
        headerlines = [l1, l2]
        size = len(b)
        hunkrange = (0, 0, 1, size)
        hunklines = [b"@@ -0,0 +1,%d @@\n" % size] + [b"+" + e for e in b]
        if without_newline:
            hunklines[-1] += b'\n'
            hunklines.append(diffhelper.MISSING_NEWLINE_MARKER)
        hunks = ((hunkrange, hunklines),)
    elif not b:
        without_newline = not a.endswith(b'\n')
        a = splitnewlines(a)
        l1 = b"--- %s%s%s" % (aprefix, fn1, datetag(ad, fn1))
        if b is None:
            l2 = b'+++ /dev/null%s' % datetag(epoch)
        else:
            l2 = b"+++ %s%s%s" % (bprefix, fn2, datetag(bd, fn2))
        headerlines = [l1, l2]
        size = len(a)
        hunkrange = (1, size, 0, 0)
        hunklines = [b"@@ -1,%d +0,0 @@\n" % size] + [b"-" + e for e in a]
        if without_newline:
            hunklines[-1] += b'\n'
            hunklines.append(diffhelper.MISSING_NEWLINE_MARKER)
        hunks = ((hunkrange, hunklines),)
    else:
        hunks = _unidiff(a, b, opts=opts)
        if not next(hunks):
            return sentinel

        headerlines = [
            b"--- %s%s%s" % (aprefix, fn1, datetag(ad, fn1)),
            b"+++ %s%s%s" % (bprefix, fn2, datetag(bd, fn2)),
        ]

    # The possible bool is consumed from the iterator above in the `next()`
    # call.
    return headerlines, cast(
        "Iterable[tuple[Optional[HunkRange], HunkLines]]", hunks
    )


def _unidiff(
    t1: bytes, t2: bytes, opts: diffopts = defaultopts
) -> Iterator[bool | tuple[HunkRange, HunkLines]]:
    """Yield hunks of a headerless unified diff from t1 and t2 texts.

    Each hunk consists of a (hunkrange, hunklines) tuple where `hunkrange` is a
    tuple (s1, l1, s2, l2) representing the range information of the hunk to
    form the '@@ -s1,l1 +s2,l2 @@' header and `hunklines` is a list of lines
    of the hunk combining said header followed by line additions and
    deletions.

    The hunks are prefixed with a bool.
    """
    l1 = splitnewlines(t1)
    l2 = splitnewlines(t2)

    def contextend(l, len):
        ret = l + opts.context
        if ret > len:
            ret = len
        return ret

    def contextstart(l):
        ret = l - opts.context
        if ret < 0:
            return 0
        return ret

    lastfunc = [0, b'']

    def yieldhunk(
        hunk: tuple[int, int, int, int, list[bytes]]
    ) -> Iterable[tuple[HunkRange, HunkLines]]:
        (astart, a2, bstart, b2, delta) = hunk
        aend = contextend(a2, len(l1))
        alen = aend - astart
        blen = b2 - bstart + aend - a2

        func = b""
        if opts.showfunc:
            lastpos, func = lastfunc
            # walk backwards from the start of the context up to the start of
            # the previous hunk context until we find a line starting with an
            # `identifier' char (alphabetic, _, or $, like GNU diffutils,
            # libxdiff, and git).
            for i in range(astart - 1, lastpos - 1, -1):
                if l1[i][0:1].isalpha() or l1[i][0:1] in (b'_', b'$'):
                    func = b' ' + l1[i].rstrip()
                    # split long function name if ASCII. otherwise we have no
                    # idea where the multi-byte boundary is, so just leave it.
                    if encoding.isasciistr(func):
                        func = func[:41]
                    lastfunc[1] = func
                    break
            # by recording this hunk's starting point as the next place to
            # start looking for function lines, we avoid reading any line in
            # the file more than once.
            lastfunc[0] = astart

        # zero-length hunk ranges report their start line as one less
        if alen:
            astart += 1
        if blen:
            bstart += 1

        hunkrange = astart, alen, bstart, blen
        hunklines = (
            [b"@@ -%d,%d +%d,%d @@%s\n" % (hunkrange + (func,))]
            + delta
            + [b' ' + l1[x] for x in range(a2, aend)]
        )
        # If either file ends without a newline and the last line of
        # that file is part of a hunk, a marker is printed. If the
        # last line of both files is identical and neither ends in
        # a newline, print only one marker. That's the only case in
        # which the hunk can end in a shared line without a newline.
        skip = False
        if not t1.endswith(b'\n') and astart + alen == len(l1) + 1:
            for i in range(len(hunklines) - 1, -1, -1):
                if hunklines[i].startswith((b'-', b' ')):
                    if hunklines[i].startswith(b' '):
                        skip = True
                    hunklines[i] += b'\n'
                    hunklines.insert(i + 1, diffhelper.MISSING_NEWLINE_MARKER)
                    break
        if not skip and not t2.endswith(b'\n') and bstart + blen == len(l2) + 1:
            for i in range(len(hunklines) - 1, -1, -1):
                if hunklines[i].startswith(b'+'):
                    hunklines[i] += b'\n'
                    hunklines.insert(i + 1, diffhelper.MISSING_NEWLINE_MARKER)
                    break
        yield hunkrange, hunklines

    # bdiff.blocks gives us the matching sequences in the files.  The loop
    # below finds the spaces between those matching sequences and translates
    # them into diff output.
    #
    hunk = None
    ignoredlines = 0
    has_hunks = False
    for s, stype in allblocks(t1, t2, opts, l1, l2):
        a1, a2, b1, b2 = s
        if stype != b'!':
            if stype == b'~':
                # The diff context lines are based on t1 content. When
                # blank lines are ignored, the new lines offsets must
                # be adjusted as if equivalent blocks ('~') had the
                # same sizes on both sides.
                ignoredlines += (b2 - b1) - (a2 - a1)
            continue
        delta = []
        old = l1[a1:a2]
        new = l2[b1:b2]

        b1 -= ignoredlines
        b2 -= ignoredlines
        astart = contextstart(a1)
        bstart = contextstart(b1)
        prev = None
        if hunk:
            # join with the previous hunk if it falls inside the context
            if astart < hunk[1] + opts.context + 1:
                prev = hunk
                astart = hunk[1]
                bstart = hunk[3]
            else:
                if not has_hunks:
                    has_hunks = True
                    yield True
                yield from yieldhunk(hunk)
        if prev:
            # we've joined the previous hunk, record the new ending points.
            hunk = (hunk[0], a2, hunk[2], b2, hunk[4])
            delta = hunk[4]
        else:
            # create a new hunk
            hunk = (astart, a2, bstart, b2, delta)

        delta[len(delta) :] = [b' ' + x for x in l1[astart:a1]]
        delta[len(delta) :] = [b'-' + x for x in old]
        delta[len(delta) :] = [b'+' + x for x in new]

    if hunk:
        if not has_hunks:
            has_hunks = True
            yield True
        yield from yieldhunk(hunk)
    elif not has_hunks:
        yield False


def b85diff(to: bytes | None, tn: bytes | None) -> bytes:
    '''print base85-encoded binary diff'''

    def fmtline(line):
        l = len(line)
        if l <= 26:
            l = pycompat.bytechr(ord(b'A') + l - 1)
        else:
            l = pycompat.bytechr(l - 26 + ord(b'a') - 1)
        return b'%c%s\n' % (l, util.b85encode(line, True))

    def chunk(text, csize=52):
        l = len(text)
        i = 0
        while i < l:
            yield text[i : i + csize]
            i += csize

    if to is None:
        to = b''
    if tn is None:
        tn = b''

    if to == tn:
        return b''

    # TODO: deltas
    ret = []
    ret.append(b'GIT binary patch\n')
    ret.append(b'literal %d\n' % len(tn))
    for l in chunk(zlib.compress(tn)):
        ret.append(fmtline(l))
    ret.append(b'\n')

    return b''.join(ret)


DIFF_HEADER = struct.Struct(b">lll")


def patchtext(bin: bytes) -> bytes:
    pos = 0
    t = []
    while pos < len(bin):
        p1, p2, l = DIFF_HEADER.unpack(bin[pos : pos + 12])
        pos += 12
        t.append(bin[pos : pos + l])
        pos += l
    return b"".join(t)


def patch(a, bin):
    if len(a) == 0:
        # skip over trivial delta header
        return util.buffer(bin, 12)
    return mpatch.patches(a, [bin])


# similar to difflib.SequenceMatcher.get_matching_blocks
def get_matching_blocks(a: bytes, b: bytes) -> list[tuple[int, int, int]]:
    return [(d[0], d[2], d[1] - d[0]) for d in bdiff.blocks(a, b)]


def trivialdiffheader(length: int) -> bytes:
    return DIFF_HEADER.pack(0, 0, length) if length else b''


def replacediffheader(oldlen: int, newlen: int) -> bytes:
    return DIFF_HEADER.pack(0, oldlen, newlen)


def full_text_from_delta(
    delta: bytes,
    base_size: int,
    base_func: Callable[[], bytes],
) -> bytes:
    """build a full text from a binary delta and its full text base

    The base is provided as a callback as in some case we won't need it at all.
    """
    # special case deltas which replace entire base; no need to decode
    # base revision. this neatly avoids censored bases, which throw when
    # they're decoded.
    if delta[: DIFF_HEADER.size] == replacediffheader(
        base_size, len(delta) - DIFF_HEADER.size
    ):
        fulltext = delta[DIFF_HEADER.size :]
    else:
        basetext = base_func()
        fulltext = patch(basetext, delta)
    return fulltext


def first_bytes(
    delta: bytes,
    size: int,
    has_base: bool = True,
) -> tuple[int, bytes, int]:
    """
    inherited-bytes, new-bytes, inherited-bytes
    """
    if len(delta) == 0:
        if has_base:
            return (size, b'', 0)
        else:
            return (0, b'', 0)
    elif len(delta) < DIFF_HEADER.size:
        return (0, delta[:size], 0)
    o = DIFF_HEADER.size
    start, old_size, new_size = DIFF_HEADER.unpack(delta[:o])
    if start >= size:
        return (size, b'', 0)
    else:
        cap_size = min(size - start, new_size - start)
        return (start, delta[o : o + cap_size], size - start - cap_size)
