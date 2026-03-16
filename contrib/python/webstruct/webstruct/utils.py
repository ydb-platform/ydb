# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function
import re
import subprocess
from functools import partial
from itertools import chain
from six.moves import range

import tldextract
import lxml.html
from lxml.etree import iterwalk


def merge_dicts(*dicts):
    """
    >>> sorted(merge_dicts({'foo': 'bar'}, {'bar': 'baz'}).items())
    [('bar', 'baz'), ('foo', 'bar')]
    """
    res = {}
    for d in dicts:
        res.update(d)
    return res


def get_combined_keys(dicts):
    """
    >>> sorted(get_combined_keys([{'foo': 'egg'}, {'bar': 'spam'}]))
    ['bar', 'foo']
    """
    seen_keys = set()
    for dct in dicts:
        seen_keys.update(dct.keys())
    return seen_keys


def flatten(x):
    """flatten(sequence) -> list

    Return a single, flat list which contains all elements retrieved
    from the sequence and all recursively contained sub-sequences
    (iterables).

    Examples::

        >>> [1, 2, [3,4], (5,6)]
        [1, 2, [3, 4], (5, 6)]
        >>> flatten([[[1,2,3], (42,None)], [4,5], [6], 7, (8,9,10)])
        [1, 2, 3, 42, None, 4, 5, 6, 7, 8, 9, 10]
    """
    result = []
    for el in x:
        if isinstance(el, (list, tuple)):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result


EXTRA_SPACE_BEFORE_RE = re.compile(r' ([,:;.!?"\)])')
EXTRA_SPACE_AFTER_RE = re.compile(r'([\(]) ')
def smart_join(tokens):
    """
    Join tokens without adding unneeded spaces before punctuation::

        >>> smart_join(['Hello', ',', 'world', '!'])
        'Hello, world!'

        >>> smart_join(['(', '303', ')', '444-7777'])
        '(303) 444-7777'

    """
    text = " ".join(tokens)
    text = EXTRA_SPACE_BEFORE_RE.sub(r"\1", text)
    text = EXTRA_SPACE_AFTER_RE.sub(r"\1", text)
    return text


def replace_html_tags(root, tag_replaces):
    """
    Replace lxml elements' tag.

    >>> from lxml.html import fragment_fromstring, document_fromstring, tostring
    >>> root = fragment_fromstring('<h1>head 1</h1>')
    >>> replace_html_tags(root, {'h1': 'strong'})
    >>> tostring(root).decode()
    '<strong>head 1</strong>'

    >>> root = document_fromstring('<h1>head 1</h1> <H2>head 2</H2>')
    >>> replace_html_tags(root, {'h1': 'strong', 'h2': 'strong', 'h3': 'strong', 'h4': 'strong'})
    >>> tostring(root).decode()
    '<html><body><strong>head 1</strong> <strong>head 2</strong></body></html>'
    """
    for _, elem in iterwalk(root):
        if elem.tag in tag_replaces:
            elem.tag = tag_replaces[elem.tag]


def kill_html_tags(doc, tagnames, keep_child=True):
    """
    >>> from lxml.html import fragment_fromstring, tostring
    >>> root = fragment_fromstring('<div><h1>head 1</h1></div>')
    >>> kill_html_tags(root, ['h1'])
    >>> tostring(root).decode()
    '<div>head 1</div>'

    >>> root = fragment_fromstring('<div><h1>head 1</h1></div>')
    >>> kill_html_tags(root, ['h1'], False)
    >>> tostring(root).decode()
    '<div></div>'
    """
    tagnames = set(tagnames)
    for _, elem in iterwalk(doc):
        if elem.tag in tagnames:
            if keep_child:
                elem.drop_tag()
            else:
                elem.drop_tree()


def html_document_fromstring(data, encoding=None):
    """ Load HTML document from string using lxml.html.HTMLParser """
    parser = lxml.html.HTMLParser(encoding=encoding)
    return lxml.html.document_fromstring(data, parser=parser)


def run_command(args, verbose=True):
    """
    Execute a command in a subprocess, terminate it if exception occurs,
    raise CalledProcessError exception if command returned non-zero exit code.

    If ``verbose == True`` then print output as it appears using "print".
    Unlike ``subprocess.check_call`` it doesn't assume that stdout
    has a file descriptor - this allows printing to work in IPython notebook.

    Example:

    >>> run_command(["python", "-c", "print(1+2)"])
    3
    >>> run_command(["python", "-c", "print(1+2)"], verbose=False)
    """
    out = subprocess.check_output(args, stderr=subprocess.STDOUT)
    if verbose:
        print(re.sub(r'[\n\r]+$', '\n', out.decode()).rstrip())


def alphanum_key(s):
    """ Key func for sorting strings according to numerical value. """
    return [int(c) if c.isdigit() else c for c in re.split('([0-9]+)', s)]


human_sorted = partial(sorted, key=alphanum_key)
human_sorted.__doc__ = "``sorted`` that uses :func:`alphanum_key` as a key function"


class BestMatch(object):
    """
    Class for finding best non-overlapping matches in a sequence of tokens.
    Override :meth:`get_sorted_ranges` method to define which results are best.
    """
    def __init__(self, known):

        self.known = known
        if hasattr(known, 'iterkeys'):
            keys_iter = known.iterkeys()
        else:
            keys_iter = known
        self.max_length = max(len(key.split()) for key in keys_iter)

    def find_ranges(self, tokens):
        ranges = self._find_matches(tokens)
        ranges = self._remove_overlapping(ranges, tokens)
        return sorted(ranges)  # sort by position

    def get_sorted_ranges(self, ranges, tokens):
        raise NotImplementedError()

    def _find_matches(self, tokens):
        # find all matching ranges
        res = []
        i = 0
        while i < len(tokens):
            max_length = min(self.max_length, max(len(tokens)-i, 0))
            for length in range(max_length, 0, -1):
                lookup = " ".join(tokens[i:i+length])
                if lookup in self.known:
                    res.append((i, length+i, lookup))
                    break
            i += 1
        return res

    def _remove_overlapping(self, ranges, tokens):
        # remove overlapping sequences, keeping the best
        res = []
        filled_indices = set()
        for begin, end, lookup in self.get_sorted_ranges(ranges, tokens):
            indices = set(range(begin, end))
            if not indices & filled_indices:
                res.append((begin, end, lookup))
                filled_indices |= indices
        return res


class LongestMatch(BestMatch):
    """
    Class for finding longest non-overlapping matches in a sequence of tokens.

    >>> known = {'North Las', 'North Las Vegas', 'North Pole', 'Vegas USA', 'Las Vegas', 'USA', "Toronto"}
    >>> lm = LongestMatch(known)
    >>> lm.max_length
    3
    >>> tokens = ["Toronto", "to", "North", "Las", "Vegas", "USA"]
    >>> for start, end, matched_text in lm.find_ranges(tokens):
    ...     print(start, end, tokens[start:end], matched_text)
    0 1 ['Toronto'] Toronto
    2 5 ['North', 'Las', 'Vegas'] North Las Vegas
    5 6 ['USA'] USA


    :class:`LongestMatch` also accepts a dict instead of a list/set for
    a ``known`` argument. In this case dict keys are used:

    >>> lm = LongestMatch({'North': 'direction', 'North Las Vegas': 'location'})
    >>> tokens = ["Toronto", "to", "North", "Las", "Vegas", "USA"]
    >>> for start, end, matched_text in lm.find_ranges(tokens):
    ...     print(start, end, tokens[start:end], matched_text)
    2 5 ['North', 'Las', 'Vegas'] North Las Vegas
    """

    def get_sorted_ranges(self, ranges, tokens):
        return sorted(ranges, key=lambda k: k[1]-k[0], reverse=True)


def substrings(txt, min_length, max_length, pad=''):
    """
    >>> substrings("abc", 1, 100)
    ['a', 'ab', 'abc', 'b', 'bc', 'c']
    >>> substrings("abc", 2, 100)
    ['ab', 'abc', 'bc']
    >>> substrings("abc", 1, 2)
    ['a', 'ab', 'b', 'bc', 'c']
    >>> substrings("abc", 1, 3, '$')
    ['$a', 'a', '$ab', 'ab', '$abc', 'abc', 'abc$', 'b', 'bc', 'bc$', 'c', 'c$']
    """
    res = []
    for start in range(len(txt)):
        remaining_length = len(txt) - start
        for length in range(min_length, min(max_length+1, remaining_length+1)):
            token = txt[start:start+length]
            if start == 0 and pad:
                res.append(pad+token)
            res.append(token)
            if length == remaining_length and pad:
                res.append(token+pad)
    return res


def train_test_split_noshuffle(*arrays, **options):
    """Split arrays or matrices into train and test subsets without shuffling.

    It allows to write

    ::

        X_train, X_test, y_train, y_test = train_test_split_noshuffle(X, y, test_size=test_size)

    instead of

    ::

        X_train, X_test = X[:-test_size], X[-test_size:]
        y_train, y_test = y[:-test_size], y[-test_size:]

    Parameters
    ----------
    *arrays : sequence of lists

    test_size : float, int, or None (default is None)
        If float, should be between 0.0 and 1.0 and represent the
        proportion of the dataset to include in the test split. If
        int, represents the absolute number of test samples. If None,
        test size is set to 0.25.

    Returns
    -------
    splitting : list of lists, length=2 * len(arrays)
        List containing train-test split of input array.

    Examples
    --------
    >>> train_test_split_noshuffle([1,2,3], ['a', 'b', 'c'], test_size=1)
    [[1, 2], [3], ['a', 'b'], ['c']]
    >>> train_test_split_noshuffle([1,2,3,4], ['a', 'b', 'c', 'd'], test_size=0.5)
    [[1, 2], [3, 4], ['a', 'b'], ['c', 'd']]

    """
    n_arrays = len(arrays)
    if n_arrays == 0:
        raise ValueError("At least one array required as input")

    test_size = options.pop('test_size', None)
    train_size = options.pop('train_size', None)

    if test_size is None and train_size is None:
        test_size = 0.25

    if isinstance(test_size, float):
        n_samples = len(arrays[0])
        test_size = int(n_samples*test_size)

    return list(chain.from_iterable(
        (a[:-test_size], a[-test_size:]) for a in arrays
    ))


def get_domain(url):
    """
    >>> get_domain("http://example.com/path")
    'example.com'
    >>> get_domain("https://hello.example.com/foo/bar")
    'example.com'
    >>> get_domain("http://hello.example.co.uk/foo?bar=1")
    'example.co.uk'
    """
    return tldextract.extract(url).registered_domain