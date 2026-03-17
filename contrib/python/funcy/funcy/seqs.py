import sys
from itertools import islice, chain, tee, groupby, filterfalse, accumulate, \
                      takewhile as _takewhile, dropwhile as _dropwhile
from collections.abc import Sequence
from collections import defaultdict, deque
import operator

from .primitives import EMPTY
from .types import is_seqcont
from .funcmakers import make_func, make_pred


__all__ = [
    'count', 'cycle', 'repeat', 'repeatedly', 'iterate',
    'take', 'drop', 'first', 'second', 'nth', 'last', 'rest', 'butlast', 'ilen',
    'map', 'filter', 'lmap', 'lfilter', 'remove', 'lremove', 'keep', 'lkeep', 'without', 'lwithout',
    'concat', 'lconcat', 'chain', 'cat', 'lcat', 'flatten', 'lflatten', 'mapcat', 'lmapcat',
    'interleave', 'interpose', 'distinct', 'ldistinct',
    'dropwhile', 'takewhile', 'split', 'lsplit', 'split_at', 'lsplit_at', 'split_by', 'lsplit_by',
    'group_by', 'group_by_keys', 'group_values', 'count_by', 'count_reps',
    'partition', 'lpartition', 'chunks', 'lchunks', 'partition_by', 'lpartition_by',
    'with_prev', 'with_next', 'pairwise', 'lzip',
    'reductions', 'lreductions', 'sums', 'lsums', 'accumulate',
]

_map, _filter = map, filter

def _lmap(f, *seqs):
    return list(map(f, *seqs))

def _lfilter(f, seq):
    return list(filter(f, seq))


# Re-export
from itertools import count, cycle, repeat

def repeatedly(f, n=EMPTY):
    """Takes a function of no args, presumably with side effects,
       and returns an infinite (or length n) iterator of calls to it."""
    _repeat = repeat(None) if n is EMPTY else repeat(None, n)
    return (f() for _ in _repeat)

def iterate(f, x):
    """Returns an infinite iterator of `x, f(x), f(f(x)), ...`"""
    while True:
        yield x
        x = f(x)


def take(n, seq):
    """Returns a list of first n items in the sequence,
       or all items if there are fewer than n."""
    return list(islice(seq, n))

def drop(n, seq):
    """Skips first n items in the sequence, yields the rest."""
    return islice(seq, n, None)

def first(seq):
    """Returns the first item in the sequence.
       Returns None if the sequence is empty."""
    return next(iter(seq), None)

def second(seq):
    """Returns second item in the sequence.
       Returns None if there are less than two items in it."""
    return first(rest(seq))

def nth(n, seq):
    """Returns nth item in the sequence or None if no such item exists."""
    try:
        return seq[n]
    except IndexError:
        return None
    except TypeError:
        return next(islice(seq, n, None), None)

def last(seq):
    """Returns the last item in the sequence or iterator.
       Returns None if the sequence is empty."""
    try:
        return seq[-1]
    except IndexError:
        return None
    except TypeError:
        item = None
        for item in seq:
            pass
        return item

def rest(seq):
    """Skips first item in the sequence, yields the rest."""
    return drop(1, seq)

def butlast(seq):
    """Iterates over all elements of the sequence but last."""
    it = iter(seq)
    try:
        prev = next(it)
    except StopIteration:
        pass
    else:
        for item in it:
            yield prev
            prev = item

def ilen(seq):
    """Consumes an iterable not reading it into memory
       and returns the number of items."""
    # NOTE: implementation borrowed from http://stackoverflow.com/a/15112059/753382
    counter = count()
    deque(zip(seq, counter), maxlen=0)  # (consume at C speed)
    return next(counter)


# TODO: tree-seq equivalent

def lmap(f, *seqs):
    """An extended version of builtin map() returning a list.
       Derives a mapper from string, int, slice, dict or set."""
    return _lmap(make_func(f), *seqs)

def lfilter(pred, seq):
    """An extended version of builtin filter() returning a list.
       Derives a predicate from string, int, slice, dict or set."""
    return _lfilter(make_pred(pred), seq)

def map(f, *seqs):
    """An extended version of builtin map().
       Derives a mapper from string, int, slice, dict or set."""
    return _map(make_func(f), *seqs)

def filter(pred, seq):
    """An extended version of builtin filter().
       Derives a predicate from string, int, slice, dict or set."""
    return _filter(make_pred(pred), seq)

def lremove(pred, seq):
    """Creates a list if items passing given predicate."""
    return list(remove(pred, seq))

def remove(pred, seq):
    """Iterates items passing given predicate."""
    return filterfalse(make_pred(pred), seq)

def lkeep(f, seq=EMPTY):
    """Maps seq with f and keeps only truthy results.
       Simply lists truthy values in one argument version."""
    return list(keep(f, seq))

def keep(f, seq=EMPTY):
    """Maps seq with f and iterates truthy results.
       Simply iterates truthy values in one argument version."""
    if seq is EMPTY:
        return _filter(bool, f)
    else:
        return _filter(bool, map(f, seq))

def without(seq, *items):
    """Iterates over sequence skipping items."""
    for value in seq:
        if value not in items:
            yield value

def lwithout(seq, *items):
    """Removes items from sequence, preserves order."""
    return list(without(seq, *items))


def lconcat(*seqs):
    """Concatenates several sequences."""
    return list(chain(*seqs))
concat = chain

def lcat(seqs):
    """Concatenates the sequence of sequences."""
    return list(cat(seqs))
cat = chain.from_iterable

def flatten(seq, follow=is_seqcont):
    """Flattens arbitrary nested sequence.
       Unpacks an item if follow(item) is truthy."""
    for item in seq:
        if follow(item):
            yield from flatten(item, follow)
        else:
            yield item

def lflatten(seq, follow=is_seqcont):
    """Iterates over arbitrary nested sequence.
       Dives into when follow(item) is truthy."""
    return list(flatten(seq, follow))

def lmapcat(f, *seqs):
    """Maps given sequence(s) and concatenates the results."""
    return lcat(map(f, *seqs))

def mapcat(f, *seqs):
    """Maps given sequence(s) and chains the results."""
    return cat(map(f, *seqs))

def interleave(*seqs):
    """Yields first item of each sequence, then second one and so on."""
    return cat(zip(*seqs))

def interpose(sep, seq):
    """Yields items of the sequence alternating with sep."""
    return drop(1, interleave(repeat(sep), seq))

def takewhile(pred, seq=EMPTY):
    """Yields sequence items until first predicate fail.
       Stops on first falsy value in one argument version."""
    if seq is EMPTY:
        pred, seq = bool, pred
    else:
        pred = make_pred(pred)
    return _takewhile(pred, seq)

def dropwhile(pred, seq=EMPTY):
    """Skips the start of the sequence passing pred (or just truthy),
       then iterates over the rest."""
    if seq is EMPTY:
        pred, seq = bool, pred
    else:
        pred = make_pred(pred)
    return _dropwhile(pred, seq)


def ldistinct(seq, key=EMPTY):
    """Removes duplicates from sequences, preserves order."""
    return list(distinct(seq, key))

def distinct(seq, key=EMPTY):
    """Iterates over sequence skipping duplicates"""
    seen = set()
    # check if key is supplied out of loop for efficiency
    if key is EMPTY:
        for item in seq:
            if item not in seen:
                seen.add(item)
                yield item
    else:
        key = make_func(key)
        for item in seq:
            k = key(item)
            if k not in seen:
                seen.add(k)
                yield item


def split(pred, seq):
    """Lazily splits items which pass the predicate from the ones that don't.
       Returns a pair (passed, failed) of respective iterators."""
    pred = make_pred(pred)
    yes, no = deque(), deque()
    splitter = (yes.append(item) if pred(item) else no.append(item) for item in seq)

    def _split(q):
        while True:
            while q:
                yield q.popleft()
            try:
                next(splitter)
            except StopIteration:
                return

    return _split(yes), _split(no)

def lsplit(pred, seq):
    """Splits items which pass the predicate from the ones that don't.
       Returns a pair (passed, failed) of respective lists."""
    pred = make_pred(pred)
    yes, no = [], []
    for item in seq:
        if pred(item):
            yes.append(item)
        else:
            no.append(item)
    return yes, no


def split_at(n, seq):
    """Lazily splits the sequence at given position,
       returning a pair of iterators over its start and tail."""
    a, b = tee(seq)
    return islice(a, n), islice(b, n, None)

def lsplit_at(n, seq):
    """Splits the sequence at given position,
       returning a tuple of its start and tail."""
    a, b = split_at(n, seq)
    return list(a), list(b)

def split_by(pred, seq):
    """Lazily splits the start of the sequence,
       consisting of items passing pred, from the rest of it."""
    a, b = tee(seq)
    return takewhile(pred, a), dropwhile(pred, b)

def lsplit_by(pred, seq):
    """Splits the start of the sequence,
       consisting of items passing pred, from the rest of it."""
    a, b = split_by(pred, seq)
    return list(a), list(b)


def group_by(f, seq):
    """Groups given sequence items into a mapping f(item) -> [item, ...]."""
    f = make_func(f)
    result = defaultdict(list)
    for item in seq:
        result[f(item)].append(item)
    return result


def group_by_keys(get_keys, seq):
    """Groups items having multiple keys into a mapping key -> [item, ...].
       Item might be repeated under several keys."""
    get_keys = make_func(get_keys)
    result = defaultdict(list)
    for item in seq:
        for k in get_keys(item):
            result[k].append(item)
    return result


def group_values(seq):
    """Takes a sequence of (key, value) pairs and groups values by keys."""
    result = defaultdict(list)
    for key, value in seq:
        result[key].append(value)
    return result


def count_by(f, seq):
    """Counts numbers of occurrences of values of f()
       on elements of given sequence."""
    f = make_func(f)
    result = defaultdict(int)
    for item in seq:
        result[f(item)] += 1
    return result


def count_reps(seq):
    """Counts number occurrences of each value in the sequence."""
    result = defaultdict(int)
    for item in seq:
        result[item] += 1
    return result


# For efficiency we use separate implementation for cutting sequences (those capable of slicing)
def _cut_seq(drop_tail, n, step, seq):
    limit = len(seq)-n+1 if drop_tail else len(seq)
    return (seq[i:i+n] for i in range(0, limit, step))

def _cut_iter(drop_tail, n, step, seq):
    it = iter(seq)
    pool = take(n, it)
    while True:
        if len(pool) < n:
            break
        yield pool
        pool = pool[step:]
        pool.extend(islice(it, step))
    if not drop_tail:
        for item in _cut_seq(drop_tail, n, step, pool):
            yield item

def _cut(drop_tail, n, step, seq=EMPTY):
    if seq is EMPTY:
        step, seq = n, step
    if isinstance(seq, Sequence):
        return _cut_seq(drop_tail, n, step, seq)
    else:
        return _cut_iter(drop_tail, n, step, seq)

def partition(n, step, seq=EMPTY):
    """Lazily partitions seq into parts of length n.
       Skips step items between parts if passed. Non-fitting tail is ignored."""
    return _cut(True, n, step, seq)

def lpartition(n, step, seq=EMPTY):
    """Partitions seq into parts of length n.
       Skips step items between parts if passed. Non-fitting tail is ignored."""
    return list(partition(n, step, seq))

def chunks(n, step, seq=EMPTY):
    """Lazily chunks seq into parts of length n or less.
       Skips step items between parts if passed."""
    return _cut(False, n, step, seq)

def lchunks(n, step, seq=EMPTY):
    """Chunks seq into parts of length n or less.
       Skips step items between parts if passed."""
    return list(chunks(n, step, seq))

def partition_by(f, seq):
    """Lazily partition seq into continuous chunks with constant value of f."""
    f = make_func(f)
    for _, items in groupby(seq, f):
        yield items

def lpartition_by(f, seq):
    """Partition seq into continuous chunks with constant value of f."""
    return _lmap(list, partition_by(f, seq))


def with_prev(seq, fill=None):
    """Yields each item paired with its preceding: (item, prev)."""
    a, b = tee(seq)
    return zip(a, chain([fill], b))

def with_next(seq, fill=None):
    """Yields each item paired with its following: (item, next)."""
    a, b = tee(seq)
    next(b, None)
    return zip(a, chain(b, [fill]))

# An itertools recipe
# NOTE: this is the same as ipartition(2, 1, seq) only faster and with distinct name
def pairwise(seq):
    """Yields all pairs of neighboring items in seq."""
    a, b = tee(seq)
    next(b, None)
    return zip(a, b)

if sys.version_info >= (3, 10):
    def lzip(*seqs, strict=False):
        """List zip() version."""
        return list(zip(*seqs, strict=strict))
else:
    def lzip(*seqs, strict=False):
        """List zip() version."""
        if strict and len(seqs) > 1:
            return list(_zip_strict(*seqs))
        return list(zip(*seqs))

    def _zip_strict(*seqs):
        try:
            # Try compare lens if they are available and use a fast zip() builtin
            len_1 = len(seqs[0])
            for i, s in enumerate(seqs, start=1):
                len_i = len(s)
                if len_i != len_1:
                    short_i, long_i = (1, i) if len_1 < len_i else (i, 1)
                    raise _zip_strict_error(short_i, long_i)
        except TypeError:
            return _zip_strict_iters(*seqs)
        else:
            return zip(*seqs)

    def _zip_strict_iters(*seqs):
        iters = [iter(s) for s in seqs]
        while True:
            values, stop_i, val_i = [], 0, 0
            for i, it in enumerate(iters, start=1):
                try:
                    values.append(next(it))
                    if not val_i:
                        val_i = i
                except StopIteration:
                    if not stop_i:
                        stop_i = i

            if stop_i:
                if val_i:
                    raise _zip_strict_error(stop_i, val_i)
                break
            yield tuple(values)

    def _zip_strict_error(short_i, long_i):
        if short_i == 1:
            return ValueError("zip() argument %d is longer than argument 1" % long_i)
        else:
            start = "argument 1" if short_i == 2 else "argument 1-%d" % (short_i - 1)
            return ValueError("zip() argument %d is shorter than %s" % (short_i, start))


def _reductions(f, seq, acc):
    last = acc
    for x in seq:
        last = f(last, x)
        yield last

def reductions(f, seq, acc=EMPTY):
    """Yields intermediate reductions of seq by f."""
    if acc is EMPTY:
        return accumulate(seq) if f is operator.add else accumulate(seq, f)
    return _reductions(f, seq, acc)

def lreductions(f, seq, acc=EMPTY):
    """Lists intermediate reductions of seq by f."""
    return list(reductions(f, seq, acc))

def sums(seq, acc=EMPTY):
    """Yields partial sums of seq."""
    return reductions(operator.add, seq, acc)

def lsums(seq, acc=EMPTY):
    """Lists partial sums of seq."""
    return lreductions(operator.add, seq, acc)
