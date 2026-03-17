'''
Simple Diff for Python version 1.0

Annotate two versions of a list with the values that have been
changed between the versions, similar to unix's `diff` but with
a dead-simple Python interface.

(C) Paul Butler 2008-2012 <http://www.paulbutler.org/>
May be used and distributed under the zlib/libpng license
<http://www.opensource.org/licenses/zlib-license.php>
'''

__all__ = ['diff', 'string_diff', 'html_diff']
__version__ = '1.1'


def diff(old, new):
    '''
    Find the differences between two lists. Returns a list of pairs, where the
    first value is in ['+','-','='] and represents an insertion, deletion, or
    no change for that list. The second value of the pair is the list
    of elements.

    Params:
        old     the old list of immutable, comparable values (ie. a list
                of strings)
        new     the new list of immutable, comparable values
   
    Returns:
        A list of pairs, with the first part of the pair being one of three
        strings ('-', '+', '=') and the second part being a list of values from
        the original old and/or new lists. The first part of the pair
        corresponds to whether the list of values is a deletion, insertion, or
        unchanged, respectively.

    Examples:
        >>> diff([1,2,3,4],[1,3,4])
        [('=', [1]), ('-', [2]), ('=', [3, 4])]

        >>> diff([1,2,3,4],[2,3,4,1])
        [('-', [1]), ('=', [2, 3, 4]), ('+', [1])]

        >>> diff('The quick brown fox jumps over the lazy dog'.split(),
        ...      'The slow blue cheese drips over the lazy carrot'.split())
        ... # doctest: +NORMALIZE_WHITESPACE
        [('=', ['The']),
         ('-', ['quick', 'brown', 'fox', 'jumps']),
         ('+', ['slow', 'blue', 'cheese', 'drips']),
         ('=', ['over', 'the', 'lazy']),
         ('-', ['dog']),
         ('+', ['carrot'])]

    '''

    # Create a map from old values to their indices
    old_index_map = dict()
    for i, val in enumerate(old):
        old_index_map.setdefault(val,list()).append(i)

    # Find the largest substring common to old and new.
    # We use a dynamic programming approach here.
    # 
    # We iterate over each value in the `new` list, calling the
    # index `inew`. At each iteration, `overlap[i]` is the
    # length of the largest suffix of `old[:i]` equal to a suffix
    # of `new[:inew]` (or unset when `old[i]` != `new[inew]`).
    #
    # At each stage of iteration, the new `overlap` (called
    # `_overlap` until the original `overlap` is no longer needed)
    # is built from the old one.
    #
    # If the length of overlap exceeds the largest substring
    # seen so far (`sub_length`), we update the largest substring
    # to the overlapping strings.

    overlap = dict()
    # `sub_start_old` is the index of the beginning of the largest overlapping
    # substring in the old list. `sub_start_new` is the index of the beginning
    # of the same substring in the new list. `sub_length` is the length that
    # overlaps in both.
    # These track the largest overlapping substring seen so far, so naturally
    # we start with a 0-length substring.
    sub_start_old = 0
    sub_start_new = 0
    sub_length = 0

    for inew, val in enumerate(new):
        _overlap = dict()
        for iold in old_index_map.get(val,list()):
            # now we are considering all values of iold such that
            # `old[iold] == new[inew]`.
            _overlap[iold] = (iold and overlap.get(iold - 1, 0)) + 1
            if(_overlap[iold] > sub_length):
                # this is the largest substring seen so far, so store its
                # indices
                sub_length = _overlap[iold]
                sub_start_old = iold - sub_length + 1
                sub_start_new = inew - sub_length + 1
        overlap = _overlap

    if sub_length == 0:
        # If no common substring is found, we return an insert and delete...
        return (old and [('-', old)] or []) + (new and [('+', new)] or [])
    else:
        # ...otherwise, the common substring is unchanged and we recursively
        # diff the text before and after that substring
        return diff(old[ : sub_start_old], new[ : sub_start_new]) + \
               [('=', new[sub_start_new : sub_start_new + sub_length])] + \
               diff(old[sub_start_old + sub_length : ],
                       new[sub_start_new + sub_length : ])


def string_diff(old, new):
    '''
    Returns the difference between the old and new strings when split on
    whitespace. Considers punctuation a part of the word

    This function is intended as an example; you'll probably want
    a more sophisticated wrapper in practice.

    Params:
        old     the old string
        new     the new string

    Returns:
        the output of `diff` on the two strings after splitting them
        on whitespace (a list of change instructions; see the docstring
        of `diff`)

    Examples:
        >>> string_diff('The quick brown fox', 'The fast blue fox')
        ... # doctest: +NORMALIZE_WHITESPACE
        [('=', ['The']),
         ('-', ['quick', 'brown']),
         ('+', ['fast', 'blue']),
         ('=', ['fox'])]

    '''
    return diff(old.split(), new.split())


def html_diff(old, new):
    '''
    Returns the difference between two strings (as in stringDiff) in
    HTML format. HTML code in the strings is NOT escaped, so you
    will get weird results if the strings contain HTML.

    This function is intended as an example; you'll probably want
    a more sophisticated wrapper in practice.

    Params:
        old     the old string
        new     the new string

    Returns:
        the output of the diff expressed with HTML <ins> and <del>
        tags.

    Examples:
        >>> html_diff('The quick brown fox', 'The fast blue fox')
        'The <del>quick brown</del> <ins>fast blue</ins> fox'
    '''
    con = {'=': (lambda x: x),
           '+': (lambda x: "<ins>" + x + "</ins>"),
           '-': (lambda x: "<del>" + x + "</del>")}
    return " ".join([(con[a])(" ".join(b)) for a, b in string_diff(old, new)])


def check_diff(old, new):
    '''
    This tests that diffs returned by `diff` are valid. You probably won't
    want to use this function, but it's provided for documentation and
    testing.

    A diff should satisfy the property that the old input is equal to the
    elements of the result annotated with '-' or '=' concatenated together.
    Likewise, the new input is equal to the elements of the result annotated
    with '+' or '=' concatenated together. This function compares `old`,
    `new`, and the results of `diff(old, new)` to ensure this is true.

    Tests:
        >>> check_diff('ABCBA', 'CBABA')
        >>> check_diff('Foobarbaz', 'Foobarbaz')
        >>> check_diff('Foobarbaz', 'Boobazbam')
        >>> check_diff('The quick brown fox', 'Some quick brown car')
        >>> check_diff('A thick red book', 'A quick blue book')
        >>> check_diff('dafhjkdashfkhasfjsdafdasfsda', 'asdfaskjfhksahkfjsdha')
        >>> check_diff('88288822828828288282828', '88288882882828282882828')
        >>> check_diff('1234567890', '24689')
    '''
    old = list(old)
    new = list(new)
    result = diff(old, new)
    _old = [val for (a, vals) in result if (a in '=-') for val in vals]
    assert old == _old, 'Expected %s, got %s' % (old, _old)
    _new = [val for (a, vals) in result if (a in '=+') for val in vals]
    assert new == _new, 'Expected %s, got %s' % (new, _new)

