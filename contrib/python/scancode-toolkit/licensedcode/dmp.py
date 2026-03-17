

from collections import namedtuple
import sys
import time


"""
Computes the difference between two texts. Originally based on
Diff Match and Patch

Copyright 2018 The diff-match-patch Authors.
original author fraser@google.com (Neil Fraser)

https://github.com/google/diff-match-patch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Changes
2019-05-14: This file has been substantially modified.
            All non-diff code has been removed.
            Most methods have been moved to plain functions
            A new difflib-like match_blocks function has been added
            that works from sequences of ints.
"""


TRACE = False


def logger_debug(*args): pass


if TRACE :
    import logging

    logger = logging.getLogger(__name__)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))

    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)


# The data structure representing a diff is an array of tuples:
# [(DIFF_DELETE, "Hello"), (DIFF_INSERT, "Goodbye"), (DIFF_EQUAL, " world.")]
# which means: delete "Hello", add "Goodbye" and keep " world."
DIFF_DELETE = -1
DIFF_INSERT = 1
DIFF_EQUAL = 0


Match = namedtuple('Match', 'a b size')

def match_blocks(a, b, a_start, a_end, *args, **kwargs):
    """
    Return a list of matching block Match triples describing matching
    subsequences of `a` in `b` starting from the `a_start` position in `a` up to
    the `a_end` position in `a`.
    """
    if TRACE:
        logger_debug('a_start', a_start, 'a_end', a_end)
    # convert sequences to strings
    text1 = int2unicode(a[a_start:a_end])
    text2 = int2unicode(b)

    df = Differ(timeout=0.01)
    diffs = df.difference(text1, text2)
    diffs = trim(diffs)

    apos = a_start
    bpos = 0
    matches = []
    for op, matched_text in diffs:
        size = len(matched_text)
        if not size:
            continue
        if op == DIFF_EQUAL:
            matches.append(Match(apos, bpos, size))
            apos += size
            bpos += size
        elif op == DIFF_INSERT:
            bpos += size

        elif op == DIFF_DELETE:
            apos += size

    return matches


def int2unicode(nums):
    """
    Convert an array of positive integers to a unicode string.
    """
    return u''.join(chr(i + 1) for i in nums)


def trim(diffs):
    """
    Remove trailing INSERT and DELETE from a list of diffs
    """
    # FIXME: this may be best done in the main loop?
    while diffs:
        op, _ = diffs[-1]
        if op in (DIFF_DELETE, DIFF_INSERT):
            diffs.pop()
        else:
            break
    return diffs


class Differ(object):
    def __init__(self, timeout=0.1):
        # Number of seconds to compute a diff before giving up (0 for infinity).
        self.timeout = timeout

    def difference(self, text1, text2, deadline=None):
        """
        Find the differences between two texts.  Simplifies the problem by
        stripping any common prefix or suffix off the texts before diffing.

        Args:
          text1: Old string to be diffed.
          text2: New string to be diffed.
          deadline: Optional time when the diff should be complete by.  Used
            internally for recursive calls.  Users should set timeout instead.

        Returns:
          Array of changes.
        """
        if text1 == None or text2 == None:
            raise ValueError('Illegal empty inputs')

        # Check for equality (speedup).
        if text1 == text2:
            if text1:
                return [(DIFF_EQUAL, text1)]
            return []

        # Set a deadline by which time the diff must be complete.
        if deadline == None:
            # Unlike in most languages, Python counts time in seconds.
            if not self.timeout:
                deadline = sys.maxsize
            else:
                deadline = time.time() + self.timeout

        # Trim off common prefix (speedup).
        commonlength = common_prefix(text1, text2)
        commonprefix = text1[:commonlength]
        text1 = text1[commonlength:]
        text2 = text2[commonlength:]

        # Trim off common suffix (speedup).
        commonlength = common_suffix(text1, text2)
        if commonlength == 0:
            commonsuffix = ''
        else:
            commonsuffix = text1[-commonlength:]
            text1 = text1[:-commonlength]
            text2 = text2[:-commonlength]

        # Compute the diff on the middle block.
        diffs = self.compute(text1, text2, deadline)

        # Restore the prefix and suffix.
        if commonprefix:
            diffs[:0] = [(DIFF_EQUAL, commonprefix)]
        if commonsuffix:
            diffs.append((DIFF_EQUAL, commonsuffix))
        diffs = merge(diffs)
        return diffs

    def compute(self, text1, text2, deadline):
        """
        Find the differences between two texts.  Assumes that the texts do not
        have any common prefix or suffix.

        Args:
          text1: Old string to be diffed.
          text2: New string to be diffed.
          deadline: Time when the diff should be complete by.

        Returns:
          Array of changes.
        """
        if not text1:
            # Just add some text (speedup).
            return [(DIFF_INSERT, text2)]

        if not text2:
            # Just delete some text (speedup).
            return [(DIFF_DELETE, text1)]

        len_text1 = len(text1)
        len_text2 = len(text2)

        reversed_diff = len_text1 > len_text2

        if reversed_diff:
            longtext, shorttext = text1, text2
            len_shorttext = len_text2
        else:
            shorttext, longtext = text1, text2
            len_shorttext = len_text1

        i = longtext.find(shorttext)

        if i != -1:
            # Shorter text is inside the longer text (speedup).
            diffs = [(DIFF_INSERT, longtext[:i]),
                     (DIFF_EQUAL, shorttext),
                     (DIFF_INSERT, longtext[i + len_shorttext:])]
            # Swap insertions for deletions if diff is reversed.
            if reversed_diff:
                diffs[0] = (DIFF_DELETE, diffs[0][1])
                diffs[2] = (DIFF_DELETE, diffs[2][1])
            return diffs

        if len_shorttext == 1:
            # Single character string.
            # After the previous speedup, the character can't be an equality.
            return [(DIFF_DELETE, text1), (DIFF_INSERT, text2)]

        # Check to see if the problem can be split in two.
        hm = half_match(text1, text2, len_text1, len_text2)
        if hm:
            # A half-match was found, sort out the return data.
            (text1_a, text1_b, text2_a, text2_b, mid_common) = hm
            # Send both pairs off for separate processing.
            diffs_a = self.difference(text1_a, text2_a, deadline)
            diffs_b = self.difference(text1_b, text2_b, deadline)
            # Merge the results.
            return diffs_a + [(DIFF_EQUAL, mid_common)] + diffs_b

        return self.bisect(text1, text2, deadline, len_text1, len_text2)

    def bisect(self, text1, text2, deadline, len_text1, len_text2):
        """
        Find the 'middle snake' of a diff, split the problem in two
        and return the recursively constructed diff.
        See Myers 1986 paper: An O(ND) Difference Algorithm and Its Variations.

        Args:
          text1: Old string to be diffed.
          text2: New string to be diffed.
          deadline: Time at which to bail if not yet complete.

        Returns:
          Array of diff tuples.
        """

        max_d = (len_text1 + len_text2 + 1) // 2
        v_offset = max_d
        v_length = 2 * max_d
        v1 = [-1] * v_length
        v1[v_offset + 1] = 0
        v2 = v1[:]
        delta = len_text1 - len_text2
        # If the total number of characters is odd, then the front path will
        # collide with the reverse path.
        front = (delta % 2 != 0)
        # Offsets for start and end of k loop.
        # Prevents mapping of space beyond the grid.
        k1start = 0
        k1end = 0
        k2start = 0
        k2end = 0
        for d in range(max_d):
            # Bail out if deadline is reached.
            if time.time() > deadline:
                break

            # Walk the front path one step.
            for k1 in range(-d + k1start, d + 1 - k1end, 2):
                k1_offset = v_offset + k1

                if k1 == -d or (k1 != d and v1[k1_offset - 1] < v1[k1_offset + 1]):
                    x1 = v1[k1_offset + 1]
                else:
                    x1 = v1[k1_offset - 1] + 1

                y1 = x1 - k1

                while (x1 < len_text1 and y1 < len_text2 and text1[x1] == text2[y1]):
                    x1 += 1
                    y1 += 1

                v1[k1_offset] = x1

                if x1 > len_text1:
                    # Ran off the right of the graph.
                    k1end += 2

                elif y1 > len_text2:
                    # Ran off the bottom of the graph.
                    k1start += 2

                elif front:
                    k2_offset = v_offset + delta - k1

                    if k2_offset >= 0 and k2_offset < v_length and v2[k2_offset] != -1:
                        # Mirror x2 onto top-left coordinate system.
                        x2 = len_text1 - v2[k2_offset]

                        if x1 >= x2:
                            # Overlap detected.
                            return self.bisect_split(text1, text2, x1, y1, deadline)

            # Walk the reverse path one step.
            for k2 in range(-d + k2start, d + 1 - k2end, 2):
                k2_offset = v_offset + k2

                if k2 == -d or (k2 != d and v2[k2_offset - 1] < v2[k2_offset + 1]):
                    x2 = v2[k2_offset + 1]
                else:
                    x2 = v2[k2_offset - 1] + 1

                y2 = x2 - k2

                while (x2 < len_text1 and y2 < len_text2 and text1[-x2 - 1] == text2[-y2 - 1]):
                    x2 += 1
                    y2 += 1

                v2[k2_offset] = x2

                if x2 > len_text1:
                    # Ran off the left of the graph.
                    k2end += 2
                elif y2 > len_text2:
                    # Ran off the top of the graph.
                    k2start += 2
                elif not front:
                    k1_offset = v_offset + delta - k2
                    if k1_offset >= 0 and k1_offset < v_length and v1[k1_offset] != -1:
                        x1 = v1[k1_offset]
                        y1 = v_offset + x1 - k1_offset
                        # Mirror x2 onto top-left coordinate system.
                        x2 = len_text1 - x2
                        if x1 >= x2:
                            # Overlap detected.
                            return self.bisect_split(text1, text2, x1, y1, deadline)

        # Diff took too long and hit the deadline or
        # number of diffs equals number of characters, no commonality at all.
        return [(DIFF_DELETE, text1), (DIFF_INSERT, text2)]

    def bisect_split(self, text1, text2, x, y, deadline):
        """
        Given the location of the 'middle snake', split the diff in two parts
        and recurse.

        Args:
          text1: Old string to be diffed.
          text2: New string to be diffed.
          x: Index of split point in text1.
          y: Index of split point in text2.
          deadline: Time at which to bail if not yet complete.

        Returns:
          Array of diff tuples.
        """
        text1a = text1[:x]
        text2a = text2[:y]
        text1b = text1[x:]
        text2b = text2[y:]

        # Compute both diffs serially.
        diffs = self.difference(text1a, text2a, deadline)
        diffsb = self.difference(text1b, text2b, deadline)

        return diffs + diffsb


def half_match(text1, text2, len_text1, len_text2):
    """
    Do the two texts share a substring which is at least half the length of
    the longer text?
    This speedup can produce non-minimal diffs.

    Args:
      text1: First string.
      text2: Second string.

    Returns:
      Five element Array, containing the prefix of text1, the suffix of text1,
      the prefix of text2, the suffix of text2 and the common middle.  Or None
      if there was no match.
    """
    reversed_diff = len_text1 > len_text2

    if reversed_diff:
        longtext, shorttext = text1, text2
        len_longtext, len_shorttext = len_text1, len_text2
    else:
        shorttext, longtext = text1, text2
        len_shorttext, len_longtext = len_text1, len_text2

    if len_longtext < 4 or len_shorttext * 2 < len_longtext:
        # Pointless.
        return None

    # First check if the second quarter is the seed for a half-match.
    hm1 = half_match_i(longtext, shorttext, (len_longtext + 3) // 4, len_longtext)

    # Check again based on the third quarter.
    hm2 = half_match_i(longtext, shorttext, (len_longtext + 1) // 2, len_longtext)

    if not hm1 and not hm2:
        return None

    elif not hm2:
        hm = hm1

    elif not hm1:
        hm = hm2

    else:
        # Both matched.  Select the longest.
        if len(hm1[4]) > len(hm2[4]):
            hm = hm1
        else:
            hm = hm2

    # A half-match was found, sort out the return data.
    if reversed_diff:
        text1_a, text1_b, text2_a, text2_b, mid_common = hm
    else:
        text2_a, text2_b, text1_a, text1_b, mid_common = hm

    return text1_a, text1_b, text2_a, text2_b, mid_common


def half_match_i(longtext, shorttext, i, len_longtext):
    """
    Does a substring of shorttext exist within longtext such that the substring
    is at least half the length of longtext?

    Args:
      longtext: Longer string.
      shorttext: Shorter string.
      i: Start index of quarter length substring within longtext.

    Returns:
      Five element Array, containing:
       - the prefix of longtext,
       - the suffix of longtext,
       - the prefix of shorttext,
       - the suffix of shorttext
       - the common middle.
      Or None if there was no match.
    """
    seed = longtext[i:i + len_longtext // 4]
    best_common = ''
    j = shorttext.find(seed)
    while j != -1:
        prefixLength = common_prefix(longtext[i:], shorttext[j:])
        suffixLength = common_suffix(longtext[:i], shorttext[:j])

        if len(best_common) < suffixLength + prefixLength:
            best_common = (shorttext[j - suffixLength:j] + shorttext[j:j + prefixLength])
            best_longtext_a = longtext[:i - suffixLength]
            best_longtext_b = longtext[i + prefixLength:]
            best_shorttext_a = shorttext[:j - suffixLength]
            best_shorttext_b = shorttext[j + prefixLength:]
        j = shorttext.find(seed, j + 1)

    if len(best_common) * 2 >= len_longtext:
        return (
            best_longtext_a, best_longtext_b,
            best_shorttext_a, best_shorttext_b,
            best_common)


def cleanup_efficiency(diffs, editcost=4):
    """
    Reduce the number of edits by eliminating operationally trivial
    equalities.

    Args:
      diffs: Array of diff tuples.
    """
    changes = False
    # Stack of indices where equalities are found.
    equalities = []
    # Always equal to diffs[equalities[-1]][1]
    last_equality = None
    # Index of current position.
    pointer = 0
    # Is there an insertion operation before the last equality.
    pre_ins = False
    # Is there a deletion operation before the last equality.
    pre_del = False
    # Is there an insertion operation after the last equality.
    post_ins = False
    # Is there a deletion operation after the last equality.
    post_del = False

    while pointer < len(diffs):
        if diffs[pointer][0] == DIFF_EQUAL:  # Equality found.
            if (len(diffs[pointer][1]) < editcost and (post_ins or post_del)):
                # Candidate found.
                equalities.append(pointer)
                pre_ins = post_ins
                pre_del = post_del
                last_equality = diffs[pointer][1]
            else:
                # Not a candidate, and can never become one.
                equalities = []
                last_equality = None

            post_ins = post_del = False
        else:  # An insertion or deletion.
            if diffs[pointer][0] == DIFF_DELETE:
                post_del = True
            else:
                post_ins = True

            # Five types to be split:
            # <ins>A</ins><del>B</del>XY<ins>C</ins><del>D</del>
            # <ins>A</ins>X<ins>C</ins><del>D</del>
            # <ins>A</ins><del>B</del>X<ins>C</ins>
            # <ins>A</del>X<ins>C</ins><del>D</del>
            # <ins>A</ins><del>B</del>X<del>C</del>

            if last_equality and (
                (pre_ins and pre_del and post_ins and post_del)
                or
                ((len(last_equality) < editcost / 2)
                 and (pre_ins + pre_del + post_ins + post_del) == 3)):

                # Duplicate record.
                diffs.insert(equalities[-1], (DIFF_DELETE, last_equality))
                # Change second copy to insert.
                diffs[equalities[-1] + 1] = (DIFF_INSERT, diffs[equalities[-1] + 1][1])
                # Throw away the equality we just deleted.
                equalities.pop()
                last_equality = None

                if pre_ins and pre_del:
                    # No changes made which could affect previous entry, keep going.
                    post_ins = post_del = True
                    equalities = []
                else:
                    if equalities:
                        # Throw away the previous equality.
                        equalities.pop()
                    if equalities:
                        pointer = equalities[-1]
                    else:
                        pointer = -1
                    post_ins = post_del = False
                changes = True
        pointer += 1

    if changes:
        diffs = merge(diffs)
    return diffs


def common_prefix(text1, text2):
    """
    Determine the common prefix of two strings.

    Args:
      text1: First string.
      text2: Second string.

    Returns:
      The number of characters common to the start of each string.
    """
    # Quick check for common null cases.
    if not text1 or not text2 or text1[0] != text2[0]:
        return 0
    # Binary search.
    # Performance analysis: https://neil.fraser.name/news/2007/10/09/
    pointermin = 0

    # TODO: move as args
    len_text1 = len(text1)
    len_text2 = len(text2)

    pointermax = min(len_text1, len_text2)
    pointermid = pointermax
    pointerstart = 0

    while pointermin < pointermid:
        if text1[pointerstart:pointermid] == text2[pointerstart:pointermid]:
            pointermin = pointermid
            pointerstart = pointermin
        else:
            pointermax = pointermid

        pointermid = (pointermax - pointermin) // 2 + pointermin

    return pointermid


def common_suffix(text1, text2):
    """
    Determine the common suffix of two strings.

    Args:
      text1: First string.
      text2: Second string.

    Returns:
      The number of characters common to the end of each string.
    """
    # Quick check for common null cases.
    if not text1 or not text2 or text1[-1] != text2[-1]:
        return 0
    # Binary search.
    # Performance analysis: https://neil.fraser.name/news/2007/10/09/
    pointermin = 0

    # TODO: move as args
    len_text1 = len(text1)
    len_text2 = len(text2)

    pointermax = min(len_text1, len_text2)
    pointermid = pointermax
    pointerend = 0

    while pointermin < pointermid:
        if (text1[-pointermid:len_text1 - pointerend] == text2[-pointermid:len(text2) - pointerend]):
            pointermin = pointermid
            pointerend = pointermin
        else:
            pointermax = pointermid
        pointermid = (pointermax - pointermin) // 2 + pointermin
    return pointermid


def merge(diffs):
    """
    Reorder and merge like edit sections in place.  Merge equalities.
    Any edit section can move as long as it doesn't cross an equality.
    Return the merged diffs sequence.
    Args:
      diffs: Array of diff tuples.
    """
    diffs.append((DIFF_EQUAL, ''))  # Add a dummy entry at the end.
    pointer = 0
    count_delete = 0
    count_insert = 0
    text_delete = ''
    text_insert = ''

    while pointer < len(diffs):

        if diffs[pointer][0] == DIFF_INSERT:
            count_insert += 1
            text_insert += diffs[pointer][1]
            pointer += 1

        elif diffs[pointer][0] == DIFF_DELETE:
            count_delete += 1
            text_delete += diffs[pointer][1]
            pointer += 1

        elif diffs[pointer][0] == DIFF_EQUAL:

            # Upon reaching an equality, check for prior redundancies.
            if count_delete + count_insert > 1:
                if count_delete != 0 and count_insert != 0:

                    # Factor out any common prefixies.
                    commonlength = common_prefix(text_insert, text_delete)
                    if commonlength != 0:

                        x = pointer - count_delete - count_insert - 1
                        if x >= 0 and diffs[x][0] == DIFF_EQUAL:
                            diffs[x] = (
                                diffs[x][0],
                                diffs[x][1] + text_insert[:commonlength])
                        else:
                            diffs.insert(0, (DIFF_EQUAL, text_insert[:commonlength]))
                            pointer += 1

                        text_insert = text_insert[commonlength:]
                        text_delete = text_delete[commonlength:]

                    # Factor out any common suffixies.
                    commonlength = common_suffix(text_insert, text_delete)
                    if commonlength != 0:
                        diffs[pointer] = (
                            diffs[pointer][0],
                            text_insert[-commonlength:] + diffs[pointer][1])

                        text_insert = text_insert[:-commonlength]
                        text_delete = text_delete[:-commonlength]

                # Delete the offending records and add the merged ones.
                new_ops = []

                if len(text_delete) != 0:
                    new_ops.append((DIFF_DELETE, text_delete))

                if len(text_insert) != 0:
                    new_ops.append((DIFF_INSERT, text_insert))

                pointer -= count_delete + count_insert
                diffs[pointer:pointer + count_delete + count_insert] = new_ops
                pointer += len(new_ops) + 1

            elif pointer != 0 and diffs[pointer - 1][0] == DIFF_EQUAL:
                # Merge this equality with the previous one.
                diffs[pointer - 1] = (
                    diffs[pointer - 1][0],
                    diffs[pointer - 1][1] + diffs[pointer][1])
                del diffs[pointer]

            else:
                pointer += 1

            count_insert = 0
            count_delete = 0
            text_delete = ''
            text_insert = ''

    if diffs[-1][1] == '':
        diffs.pop()  # Remove the dummy entry at the end.

    # Second pass: look for single edits surrounded on both sides by equalities
    # which can be shifted sideways to eliminate an equality.
    # e.g: A<ins>BA</ins>C -> <ins>AB</ins>AC
    changes = False
    pointer = 1
    # Intentionally ignore the first and last element (don't need checking).
    while pointer < len(diffs) - 1:
        if (diffs[pointer - 1][0] == DIFF_EQUAL and diffs[pointer + 1][0] == DIFF_EQUAL):

            # This is a single edit surrounded by equalities.
            if diffs[pointer][1].endswith(diffs[pointer - 1][1]):
                # Shift the edit over the previous equality.
                if diffs[pointer - 1][1] != "":
                    diffs[pointer] = (
                        diffs[pointer][0],
                        diffs[pointer - 1][1] + diffs[pointer][1][:-len(diffs[pointer - 1][1])])

                    diffs[pointer + 1] = (
                        diffs[pointer + 1][0],
                        diffs[pointer - 1][1] + diffs[pointer + 1][1])

                del diffs[pointer - 1]
                changes = True

            elif diffs[pointer][1].startswith(diffs[pointer + 1][1]):
                # Shift the edit over the next equality.
                diffs[pointer - 1] = (
                    diffs[pointer - 1][0],
                    diffs[pointer - 1][1] + diffs[pointer + 1][1])

                diffs[pointer] = (
                    diffs[pointer][0],
                    diffs[pointer][1][len(diffs[pointer + 1][1]):] + diffs[pointer + 1][1])

                del diffs[pointer + 1]
                changes = True
        pointer += 1

    # If shifts were made, the diff needs reordering and another shift sweep.
    if changes:
        diffs = merge(diffs)
    return diffs


def levenshtein_distance(diffs):
    """
    Compute the Levenshtein distance; the number of inserted, deleted or
    substituted characters.

    Args:
      diffs: Array of diff tuples.

    Returns:
      Number of changes.
    """
    levenshtein = 0
    insertions = 0
    deletions = 0
    for (op, data) in diffs:
        if op == DIFF_INSERT:
            insertions += len(data)
        elif op == DIFF_DELETE:
            deletions += len(data)
        elif op == DIFF_EQUAL:
            # A deletion and an insertion is one substitution.
            levenshtein += max(insertions, deletions)
            insertions = 0
            deletions = 0
    levenshtein += max(insertions, deletions)
    return levenshtein
