
from collections import namedtuple as _namedtuple

"""
Token sequences alignment and diffing based on the longest common substrings of
"high tokens". This essentially a non-optimal and reasonably fast single local
sequence alignment between two sequences of integers/token ids.

Based on and heavily modified from Python's difflib.py from the 3.X tip:
https://hg.python.org/cpython/raw-file/0a69b1e8b7fe/Lib/difflib.py

license: PSF. See seq.ABOUT file for details.
"""


Match = _namedtuple('Match', 'a b size')


def find_longest_match(a, b, alo, ahi, blo, bhi, b2j, len_good, matchables):
    """
    Find longest matching block of a and b in a[alo:ahi] and b[blo:bhi].

    `b2j` is a mapping of b high token ids -> list of position in b
    `len_good` is such that token ids smaller than `_good_good` are treated as
    good, non-junk tokens. `matchables` is a set of matchable positions.
    Positions absent from this set are ignored.

    Return (i,j,k) Match tuple where:
        "i" in the start in "a"
        "j" in the start in "b"
        "k" in the size of the match

    and such that a[i:i+k] is equal to b[j:j+k], where
        alo <= i <= i+k <= ahi
        blo <= j <= j+k <= bhi

    and for all (i',j',k') matchable token positions meeting those conditions,
        k >= k'
        i <= i'
        and if i == i', j <= j'

    In other words, of all maximal matching blocks, return one that starts
    earliest in a, and of all those maximal matching blocks that start earliest
    in a, return the one that starts earliest in b.

    First the longest matching block (aka contiguous substring) is determined
    where no junk element appears in the block. Then that block is extended as
    far as possible by matching other tokens including junk on both sides. So
    the resulting block never matches on junk.

    If no blocks match, return (alo, blo, 0).
    """
    besti, bestj, bestsize = alo, blo, 0
    b2j_get = b2j.get

    # find longest matchable junk-free match
    # during an iteration of the loop, j2len[j] = length of longest
    # junk-free match ending with a[i-1] and b[j]
    j2len = {}
    j2lenget = j2len.get
    nothing = []
    for i in range(alo, ahi):
        newj2len = {}
        # we cannot do LCS on junk or non matchable
        cura = a[i]
        if cura < len_good and i in matchables:
            # look at all instances of a[i] in b; note that because
            # b2j has no junk token, the loop is skipped if a[i] is junk
            for j in b2j_get(cura, nothing):
                # a[i] matches b[j]
                if j < blo:
                    continue
                if j >= bhi:
                    break
                k = newj2len[j] = j2lenget(j - 1, 0) + 1
                if k > bestsize:
                    besti, bestj, bestsize = i - k + 1, j - k + 1, k
        j2len = newj2len
        j2lenget = j2len.get

    return extend_match(besti, bestj, bestsize, a, b, alo, ahi, blo, bhi, matchables)


def extend_match(besti, bestj, bestsize, a, b, alo, ahi, blo, bhi, matchables):
    """
    Extend a match identifier by (besti, bestj, bestsize) with any matching
    tokens on each end. Return a new Match.
    """
    if bestsize:
        while (besti > alo and bestj > blo
               and a[besti - 1] == b[bestj - 1]
               and (besti - 1) in matchables):

            besti -= 1
            bestj -= 1
            bestsize += 1

        while (besti + bestsize < ahi and bestj + bestsize < bhi
               and a[besti + bestsize] == b[bestj + bestsize]
               and (besti + bestsize) in matchables):

            bestsize += 1

    return Match(besti, bestj, bestsize)


def match_blocks(a, b, a_start, a_end, b2j, len_good, matchables=frozenset(), *args, **kwargs):
    """
    Return a list of matching block Match triples describing matching
    subsequences of `a` in `b` starting from the `a_start` position in `a` up to
    the `a_end` position in `a`.

    `b2j` is a mapping of b "high" token ids -> list of positions in b, e.g. a
    posting list.

    `len_good` is such that token ids smaller than `len_good` are treated as
    important, non-junk tokens.

    `matchables` is a set of matchable positions. Positions absent from this set
    are ignored.

    Each triple is of the form (i, j, n), and means that a[i:i+n] == b[j:j+n].
    The triples are monotonically increasing in i and in j.  It is also
    guaranteed that adjacent triples never describe adjacent equal blocks.
    Instead adjacent blocks are merged and collapsed in a single block.
    """

    # This non-recursive algorithm is using a list as a queue of blocks. We
    # still need to look at and append partial results to matching_blocks in a
    # loop. The matches are sorted at the end.

    queue = [(a_start, a_end, 0, len(b))]
    queue_append = queue.append
    queue_pop = queue.pop
    matching_blocks = []
    matching_blocks_append = matching_blocks.append
    while queue:
        alo, ahi, blo, bhi = queue_pop()
        i, j, k = x = find_longest_match(
            a, b, alo, ahi, blo, bhi, b2j, len_good, matchables)
        # a[alo:i] vs b[blo:j] unknown
        # a[i:i+k] same as b[j:j+k]
        # a[i+k:ahi] vs b[j+k:bhi] unknown
        if k:  # if k is 0, there was no matching block as the size is 0
            matching_blocks_append(x)
            if alo < i and blo < j:
                # there is unprocessed things remaining to the left
                queue_append((alo, i, blo, j))
            if i + k < ahi and j + k < bhi:
                # there is unprocessed things remaining to the right
                queue_append((i + k, ahi, j + k, bhi))

    matching_blocks.sort()

    # collapse adjacent blocks
    i1 = j1 = k1 = 0
    non_adjacent = []
    non_adjacent_append = non_adjacent.append
    for i2, j2, k2 in matching_blocks:
        # Is this block adjacent to i1, j1, k1?
        if i1 + k1 == i2 and j1 + k1 == j2:
            # Yes, so collapse them -- this just increases the length of the
            # first block by the length of the second, and the first block so
            # lengthened remains the block to compare against.
            k1 += k2
        else:
            # Not adjacent: keep it unless this is the first block (k1==0 means
            # it's the dummy we started with), and make the second block the new
            # block to compare against.
            if k1:
                non_adjacent_append((i1, j1, k1))
            i1, j1, k1 = i2, j2, k2
    if k1:
        non_adjacent_append((i1, j1, k1))

    return [Match._make(na) for na in non_adjacent]
