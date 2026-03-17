# archival.py - revision archival for mercurial
#
# Copyright 2006 Vadim Gelfer <vadim.gelfer@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import gzip
import os
import struct
import tarfile
import time
import typing
import zipfile
import zlib

from .i18n import _
from .node import nullrev

from . import (
    error,
    formatter,
    match as matchmod,
    pycompat,
    scmutil,
    util,
    vfs as vfsmod,
)

from .utils import stringutil

if typing.TYPE_CHECKING:
    from . import (
        localrepo,
    )

stringio = util.stringio

# from unzip source code:
_UNX_IFREG = 0x8000
_UNX_IFLNK = 0xA000


def tidyprefix(dest, kind, prefix):
    """choose prefix to use for names in archive.  make sure prefix is
    safe for consumers."""

    if prefix:
        prefix = util.normpath(prefix)
    else:
        if not isinstance(dest, bytes):
            raise ValueError('dest must be bytes if no prefix')
        prefix = os.path.basename(dest)
        lower = prefix.lower()
        for sfx in exts.get(kind, []):
            if lower.endswith(sfx):
                prefix = prefix[: -len(sfx)]
                break
    lpfx = os.path.normpath(util.localpath(prefix))
    prefix = util.pconvert(lpfx)
    if not prefix.endswith(b'/'):
        prefix += b'/'
    # Drop the leading '.' path component if present, so Windows can read the
    # zip files (issue4634)
    if prefix.startswith(b'./'):
        prefix = prefix[2:]
    if prefix.startswith(b'../') or os.path.isabs(lpfx) or b'/../' in prefix:
        raise error.Abort(_(b'archive prefix contains illegal components'))
    return prefix


exts = {
    b'tar': [b'.tar'],
    b'tbz2': [b'.tbz2', b'.tar.bz2'],
    b'tgz': [b'.tgz', b'.tar.gz'],
    b'zip': [b'.zip'],
    b'txz': [b'.txz', b'.tar.xz'],
}


def guesskind(dest):
    for kind, extensions in exts.items():
        if any(dest.endswith(ext) for ext in extensions):
            return kind
    return None


def _rootctx(repo):
    # repo[0] may be hidden
    for rev in repo:
        return repo[rev]
    return repo[nullrev]


# {tags} on ctx includes local tags and 'tip', with no current way to limit
# that to global tags.  Therefore, use {latesttag} as a substitute when
# the distance is 0, since that will be the list of global tags on ctx.
_defaultmetatemplate = br'''
repo: {root}
node: {ifcontains(rev, revset("wdir()"), "{p1node}{dirty}", "{node}")}
branch: {branch|utf8}
{ifeq(latesttagdistance, 0, join(latesttag % "tag: {tag}", "\n"),
      separate("\n",
               join(latesttag % "latesttag: {tag}", "\n"),
               "latesttagdistance: {latesttagdistance}",
               "changessincelatesttag: {changessincelatesttag}"))}
'''[
    1:
]  # drop leading '\n'


def buildmetadata(ctx):
    '''build content of .hg_archival.txt'''
    repo = ctx.repo()

    opts = {
        b'template': repo.ui.config(
            b'experimental', b'archivemetatemplate', _defaultmetatemplate
        )
    }

    out = util.stringio()

    fm = formatter.formatter(repo.ui, out, b'archive', opts)
    fm.startitem()
    fm.context(ctx=ctx)
    fm.data(root=_rootctx(repo).hex())

    if ctx.rev() is None:
        dirty = b''
        if ctx.dirty(missing=True):
            dirty = b'+'
        fm.data(dirty=dirty)
    fm.end()

    return out.getvalue()


class tarit:
    """write archive to tar file or stream.  can write uncompressed,
    or compress with gzip or bzip2."""

    def __init__(self, dest, mtime, kind: str = ''):
        self.mtime = mtime
        self.fileobj = None

        def taropen(
            mode: str, name: bytes = b'', fileobj=None
        ) -> tarfile.TarFile:
            if kind == 'gz':
                mode = mode[0:1]
                if not fileobj:
                    fileobj = open(name, mode + 'b')
                gzfileobj = gzip.GzipFile(
                    name,
                    mode + 'b',
                    zlib.Z_BEST_COMPRESSION,
                    fileobj,
                    mtime=mtime,
                )
                self.fileobj = gzfileobj
                return tarfile.TarFile.taropen(name, "w", gzfileobj)
            else:
                try:
                    return tarfile.open(name, mode + kind, fileobj)
                except tarfile.CompressionError as e:
                    raise error.Abort(stringutil.forcebytestr(e))

        if isinstance(dest, bytes):
            self.z = taropen('w:', name=dest)
        else:
            self.z = taropen('w|', fileobj=dest)

    def addfile(self, name, mode, islink, data):
        name = pycompat.fsdecode(name)
        i = tarfile.TarInfo(name)
        i.mtime = self.mtime
        i.size = len(data)
        if islink:
            i.type = tarfile.SYMTYPE
            i.mode = 0o777
            i.linkname = pycompat.fsdecode(data)
            data = None
            i.size = 0
        else:
            i.mode = mode
            data = stringio(data)
        self.z.addfile(i, data)

    def done(self):
        self.z.close()
        if self.fileobj:
            self.fileobj.close()


class zipit:
    """write archive to zip file or stream.  can write uncompressed,
    or compressed with deflate."""

    def __init__(self, dest, mtime, compress: bool = True):
        if isinstance(dest, bytes):
            dest = pycompat.fsdecode(dest)
        self.z = zipfile.ZipFile(
            dest, 'w', zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED
        )

        # Python's zipfile module emits deprecation warnings if we try
        # to store files with a date before 1980.
        epoch = 315532800  # calendar.timegm((1980, 1, 1, 0, 0, 0, 1, 1, 0))
        if mtime < epoch:
            mtime = epoch

        self.mtime = mtime
        self.date_time = time.gmtime(mtime)[:6]

    def addfile(self, name, mode, islink, data):
        i = zipfile.ZipInfo(pycompat.fsdecode(name), self.date_time)
        i.compress_type = self.z.compression  # pytype: disable=attribute-error
        # unzip will not honor unix file modes unless file creator is
        # set to unix (id 3).
        i.create_system = 3
        ftype = _UNX_IFREG
        if islink:
            mode = 0o777
            ftype = _UNX_IFLNK
        i.external_attr = (mode | ftype) << 16
        # add "extended-timestamp" extra block, because zip archives
        # without this will be extracted with unexpected timestamp,
        # if TZ is not configured as GMT
        i.extra += struct.pack(
            b'<hhBl',
            0x5455,  # block type: "extended-timestamp"
            1 + 4,  # size of this block
            1,  # "modification time is present"
            int(self.mtime),
        )  # last modification (UTC)
        self.z.writestr(i, data)

    def done(self):
        self.z.close()


class fileit:
    '''write archive as files in directory.'''

    def __init__(self, name, mtime):
        self.basedir = name
        self.opener = vfsmod.vfs(self.basedir)
        self.mtime = mtime

    def addfile(self, name, mode, islink, data):
        if islink:
            self.opener.symlink(data, name)
            return
        f = self.opener(name, b"w", atomictemp=False)
        f.write(data)
        f.close()
        destfile = os.path.join(self.basedir, name)
        os.chmod(destfile, mode)
        if self.mtime is not None:
            os.utime(destfile, (self.mtime, self.mtime))

    def done(self):
        pass


archivers = {
    b'files': fileit,
    b'tar': tarit,
    b'tbz2': lambda name, mtime: tarit(name, mtime, 'bz2'),
    b'tgz': lambda name, mtime: tarit(name, mtime, 'gz'),
    b'txz': lambda name, mtime: tarit(name, mtime, 'xz'),
    b'uzip': lambda name, mtime: zipit(name, mtime, False),
    b'zip': zipit,
}


def archive(
    repo: localrepo.localrepository,
    dest,  # TODO: should be bytes, but could be Callable
    node,
    kind: bytes,
    decode: bool = True,
    match=None,
    prefix: bytes = b'',
    mtime: float | None = None,
    subrepos: bool = False,
) -> int:
    """create archive of repo as it was at node.

    dest can be name of directory, name of archive file, a callable, or file
    object to write archive to. If it is a callable, it will called to open
    the actual file object before the first archive member is written.

    kind is type of archive to create.

    decode tells whether to put files through decode filters from
    hgrc.

    match is a matcher to filter names of files to write to archive.

    prefix is name of path to put before every archive member.

    mtime is the modified time, in seconds, or None to use the changeset time.

    subrepos tells whether to include subrepos.
    """

    if kind not in archivers:
        raise error.Abort(_(b"unknown archive type '%s'") % kind)

    if kind == b'files':
        if prefix:
            raise error.Abort(_(b'cannot give prefix when archiving to files'))
    else:
        prefix = tidyprefix(dest, kind, prefix)

    archiver = None
    ctx = repo[node]

    def opencallback():
        """Return the archiver instance, creating it if necessary.

        This function is called when the first actual entry is created.
        It may be called multiple times from different layers.
        When serving the archive via hgweb, no errors should happen after
        this point.
        """
        nonlocal archiver
        if archiver is None:
            if callable(dest):
                output = dest()
            else:
                output = dest
            archiver = archivers[kind](output, mtime or ctx.date()[0])
            assert archiver is not None

            if repo.ui.configbool(b"ui", b"archivemeta"):
                metaname = b'.hg_archival.txt'
                if match(metaname):
                    write(metaname, 0o644, False, lambda: buildmetadata(ctx))
        return archiver

    def write(name, mode, islink, getdata):
        if archiver is None:
            opencallback()
        assert archiver is not None, "archive should be opened by now"

        data = getdata()
        if decode:
            data = repo.wwritedata(name, data)
        archiver.addfile(prefix + name, mode, islink, data)

    if not match:
        match = scmutil.matchall(repo)

    files = list(ctx.manifest().walk(match))
    total = len(files)
    if total:
        files.sort()
        scmutil.prefetchfiles(
            repo, [(ctx.rev(), scmutil.matchfiles(repo, files))]
        )
        progress = repo.ui.makeprogress(
            _(b'archiving'), unit=_(b'files'), total=total
        )
        progress.update(0)
        for f in files:
            ff = ctx.flags(f)
            write(f, 0o755 if b'x' in ff else 0o644, b'l' in ff, ctx[f].data)
            progress.increment(item=f)
        progress.complete()

    if subrepos:
        for subpath in sorted(ctx.substate):
            sub = ctx.workingsub(subpath)
            submatch = matchmod.subdirmatcher(subpath, match)
            subprefix = prefix + subpath + b'/'
            total += sub.archive(opencallback, subprefix, submatch, decode)

    if total == 0:
        raise error.Abort(_(b'no files match the archive pattern'))

    assert archiver is not None, "archive should have been opened before"
    archiver.done()
    return total
