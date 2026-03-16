import heapq
import itertools
import json
import os


def get_mem_usage():
    """
    Return memory usage of the current process, in bytes.
    Requires psutil Python package.
    """
    import psutil
    proc = psutil.Process(os.getpid())
    return proc.memory_info().rss


def combinations_of_all_lengths(it):
    """
    Return an iterable with all possible combinations of items from ``it``:

        >>> for comb in combinations_of_all_lengths('ABC'):
        ...     print("".join(comb))
        A
        B
        C
        AB
        AC
        BC
        ABC

    """
    return itertools.chain(
        *(itertools.combinations(it, num+1) for num in range(len(it)))
    )


def longest_common_substring(data):
    """
    Return a longest common substring of a list of strings:

        >>> longest_common_substring(["apricot", "rice", "cricket"])
        'ric'
        >>> longest_common_substring(["apricot", "banana"])
        'a'
        >>> longest_common_substring(["foo", "bar", "baz"])
        ''
        >>> longest_common_substring(["", "foo"])
        ''
        >>> longest_common_substring(["apricot"])
        'apricot'
        >>> longest_common_substring([])
        ''

    See http://stackoverflow.com/questions/2892931/.
    """
    if len(data) == 1:
        return data[0]
    if not data or len(data[0]) == 0:
        return ''
    substr = ''
    for i in range(len(data[0])):
        for j in range(len(data[0])-i+1):
            if j > len(substr) and all(data[0][i:i+j] in x for x in data):
                substr = data[0][i:i+j]
    return substr


def json_write(filename, obj, **json_options):
    """ Create file ``filename`` with ``obj`` serialized to JSON """

    json_options.setdefault('ensure_ascii', False)
    json_options.setdefault('indent', 2)
    with open(filename, 'w', encoding='utf8') as f:
        json.dump(obj, f, **json_options)


def json_read(filename, **json_options):
    """ Read an object from a json file ``filename`` """
    with open(filename, 'r', encoding='utf8') as f:
        return json.load(f, **json_options)


def largest_elements(iterable, key, n=1):
    """
    Return a list of large elements of the ``iterable``
    (according to ``key`` function).

    ``n`` is a number of top element values to consider; when n==1
    (default) only largest elements are returned; when n==2 - elements
    with one of the top-2 values, etc.

    >>> s = [-4, 3, 5, 7, 4, -7]
    >>> largest_elements(s, abs)
    [7, -7]
    >>> largest_elements(s, abs, 2)
    [5, 7, -7]
    >>> largest_elements(s, abs, 3)
    [-4, 5, 7, 4, -7]

    """
    it1, it2 = itertools.tee(iterable)
    top_keys = set(heapq.nlargest(n, set(map(key, it1))))
    return [el for el in it2 if key(el) in top_keys]


def word_splits(word, min_reminder=3, max_prefix_length=5):
    """
    Return all splits of a word (taking in account min_reminder and
    max_prefix_length).
    """
    max_split = min(max_prefix_length, len(word)-min_reminder)
    split_indexes = range(1, 1+max_split)
    return [(word[:i], word[i:]) for i in split_indexes]


def kwargs_repr(kwargs=None, dont_show_value=None):
    """
    >>> kwargs_repr(dict(foo="123", a=5, x=8))
    "a=5, foo='123', x=8"
    >>> kwargs_repr(dict(foo="123", a=5, x=8), dont_show_value=['foo'])
    'a=5, foo=<...>, x=8'
    >>> kwargs_repr()
    ''
    """
    kwargs = kwargs or {}
    dont_show_value = set(dont_show_value or [])
    repr_parts = []
    for k, v in sorted(kwargs.items()):
        repr_v = repr(v) if k not in dont_show_value else "<...>"
        repr_parts.append(f"{k}={repr_v}")
    return ", ".join(repr_parts)


def with_progress(iterable, desc=None, total=None, leave=True):
    """
    Return an iterator which prints the iteration progress using tqdm package.
    Return iterable intact if tqdm is not available.
    """
    try:
        from tqdm import tqdm

        # workarounds for tqdm bugs
        def _it(iterable, desc, total, leave):
            if total is None:
                try:
                    total = len(iterable)
                except Exception:
                    total = 0
            yield from tqdm(iterable, desc=desc, total=total, leave=leave)
            if leave:
                print("")

        return _it(iterable, desc, total, leave)

    except ImportError:
        return iterable
