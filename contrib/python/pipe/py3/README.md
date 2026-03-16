# Pipe — Infix programming toolkit

[![PyPI](https://img.shields.io/pypi/v/pipe)
 ![Monthly downloads](https://img.shields.io/pypi/dm/pipe)
 ![Supported Python Version](https://img.shields.io/pypi/pyversions/pipe.svg)
](https://pypi.org/project/pipe)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/JulienPalard/pipe/tests.yml?branch=main)](https://github.com/JulienPalard/pipe/actions)

Module enabling a sh like infix syntax (using pipes).


# Introduction

As an example, here is the solution for the [2nd Euler Project
problem](https://projecteuler.net/problem=2):

> Find the sum of all the even-valued terms in Fibonacci which do not
  exceed four million.

Given `fib` a generator of Fibonacci numbers:

```python
sum(fib() | where(lambda x: x % 2 == 0) | take_while(lambda x: x < 4000000))
```

Each pipes is lazy evalatated, can be aliased, and partially
initialized, so it could be rewritten as:

```python
is_even = where(lambda x: x % 2 == 0)
sum(fib() | is_even | take_while(lambda x: x < 4000000)
```


# Installing

To install the library, you can just run the following command:

```shell
# Linux/macOS
python3 -m pip install pipe

# Windows
py -3 -m pip install pipe
```


# Using

The basic syntax is to use a `|` like in a shell:

```python
>>> from itertools import count
>>> from pipe import select, take
>>> sum(count() | select(lambda x: x ** 2) | take(10))
285
>>>
```

Some pipes take an argument:

```python
>>> from pipe import where
>>> sum([1, 2, 3, 4] | where(lambda x: x % 2 == 0))
6
>>>
```

Some do not need one:

```python
>>> from pipe import traverse
>>> for i in [1, [2, 3], 4] | traverse:
...     print(i)
1
2
3
4
>>>
```

In which case it's allowed to use the calling parenthesis:

```python
>>> from pipe import traverse
>>> for i in [1, [2, 3], 4] | traverse():
...     print(i)
1
2
3
4
>>>
```


## Existing Pipes in this module

Alphabetical list of available pipes; when several names are listed
for a given pipe, these are aliases.

### `batched`

Like Python 3.12 `itertool.batched`:

```python
>>> from pipe import batched
>>> list("ABCDEFG" | batched(3))
[('A', 'B', 'C'), ('D', 'E', 'F'), ('G',)]
>>>
```

### `chain`

Chain a sequence of iterables:

```python
>>> from pipe import chain
>>> list([[1, 2], [3, 4], [5]] | chain)
[1, 2, 3, 4, 5]
>>>
```

Warning : chain only unfold iterable containing ONLY iterables:

```python
[1, 2, [3]] | chain
```
Gives a `TypeError: chain argument #1 must support iteration`
Consider using traverse.


### `chain_with(other)`

Like itertools.chain, yields elements of the given iterable,
then yields elements of its parameters

```python
>>> from pipe import chain_with
>>> list((1, 2, 3) | chain_with([4, 5], [6]))
[1, 2, 3, 4, 5, 6]
>>>
```

### `dedup(key=None)`

Deduplicate values, using the given `key` function if provided.

```python
>>> from pipe import dedup
>>> list([-1, 0, 0, 0, 1, 2, 3] | dedup)
[-1, 0, 1, 2, 3]
>>> list([-1, 0, 0, 0, 1, 2, 3] | dedup(key=abs))
[-1, 0, 2, 3]
>>>
```


### `enumerate(start=0)`

The builtin `enumerate()` as a Pipe:

```python
>>> from pipe import enumerate
>>> list(['apple', 'banana', 'citron'] | enumerate)
[(0, 'apple'), (1, 'banana'), (2, 'citron')]
>>> list(['car', 'truck', 'motorcycle', 'bus', 'train'] | enumerate(start=6))
[(6, 'car'), (7, 'truck'), (8, 'motorcycle'), (9, 'bus'), (10, 'train')]
>>>
```


### `filter(predicate)`

Alias for `where(predicate)`, see `where(predicate)`.


### `groupby(key=None)`

Like `itertools.groupby(sorted(iterable, key = keyfunc), keyfunc)`

```python
>>> from pipe import groupby, map
>>> items = range(10)
>>> ' / '.join(items | groupby(lambda x: "Odd" if x % 2 else "Even")
...                  | select(lambda x: "{}: {}".format(x[0], ', '.join(x[1] | map(str)))))
'Even: 0, 2, 4, 6, 8 / Odd: 1, 3, 5, 7, 9'
>>>
```


### `islice()`

Just the `itertools.islice` function as a Pipe:

```python
>>> from pipe import islice
>>> list((1, 2, 3, 4, 5, 6, 7, 8, 9) | islice(2, 8, 2))
[3, 5, 7]
>>>
```

### `izip()`

Just the `itertools.izip` function as a Pipe:

```python
>>> from pipe import izip
>>> list(range(0, 10) | izip(range(1, 11)))
[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9), (9, 10)]
>>>
```

### `map()`, `select()`

Apply a conversion expression given as parameter
to each element of the given iterable

```python
>>> list([1, 2, 3] | map(lambda x: x * x))
[1, 4, 9]

>>> list([1, 2, 3] | select(lambda x: x * x))
[1, 4, 9]
>>>
```

### `netcat`

The netcat Pipe sends and receive bytes over TCP:

```python
data = [
    b"HEAD / HTTP/1.0\r\n",
    b"Host: python.org\r\n",
    b"\r\n",
]
for packet in data | netcat("python.org", 80):
    print(packet.decode("UTF-8"))
```

Gives:

```
HTTP/1.1 301 Moved Permanently
Content-length: 0
Location: https://python.org/
Connection: close
```

### ```permutations(r=None)```

Returns all possible permutations:

```python
>>> from pipe import permutations
>>> for item in 'ABC' | permutations(2):
...     print(item)
('A', 'B')
('A', 'C')
('B', 'A')
('B', 'C')
('C', 'A')
('C', 'B')
>>>
```

```python
>>> for item in range(3) | permutations:
...     print(item)
(0, 1, 2)
(0, 2, 1)
(1, 0, 2)
(1, 2, 0)
(2, 0, 1)
(2, 1, 0)
>>>
```

### `reverse`

Like Python's built-in `reversed` function.

```python
>>> from pipe import reverse
>>> list([1, 2, 3] | reverse)
[3, 2, 1]
>>>
```

### `select(fct)`

Alias for `map(fct)`, see `map(fct)`.


### `skip()`

Skips the given quantity of elements from the given iterable, then yields

```python
>>> from pipe import skip
>>> list((1, 2, 3, 4, 5) | skip(2))
[3, 4, 5]
>>>
```


### `skip_while(predicate)`

Like itertools.dropwhile, skips elements of the given iterable
while the predicate is true, then yields others:

```python
>>> from pipe import skip_while
>>> list([1, 2, 3, 4] | skip_while(lambda x: x < 3))
[3, 4]
>>>
```

### `sort(key=None, reverse=False)`

Like Python's built-in "sorted" primitive.

```python
>>> from pipe import sort
>>> ''.join("python" | sort)
'hnopty'
>>> [5, -4, 3, -2, 1] | sort(key=abs)
[1, -2, 3, -4, 5]
>>>
```

### `t`

Like Haskell's operator ":":

```python
>>> from pipe import t
>>> for i in 0 | t(1) | t(2):
...     print(i)
0
1
2
>>>
```

### `tail(n)`

Yields the given quantity of the last elements of the given iterable.

```python
>>> from pipe import tail
>>> for i in (1, 2, 3, 4, 5) | tail(3):
...     print(i)
3
4
5
>>>
```

### `take(n)`

Yields the given quantity of elements from the given iterable, like `head`
in shell script.

```python
>>> from pipe import take
>>> for i in count() | take(5):
...     print(i)
0
1
2
3
4
>>>
```

### `take_while(predicate)`

Like `itertools.takewhile`, yields elements of the
given iterable while the predicate is true:

```python
>>> from pipe import take_while
>>> for i in count() | take_while(lambda x: x ** 2 < 100):
...     print(i)
0
1
2
3
4
5
6
7
8
9
>>>
```

### `tee`

tee outputs to the standard output and yield unchanged items, useful for
debugging a pipe stage by stage:

```python
>>> from pipe import tee
>>> sum(["1", "2", "3", "4", "5"] | tee | map(int) | tee)
'1'
1
'2'
2
'3'
3
'4'
4
'5'
5
15
>>>
```

The `15` at the end is the `sum` returning.


### `transpose()`

Transposes the rows and columns of a matrix.

```python
>>> from pipe import transpose
>>> [[1, 2, 3], [4, 5, 6], [7, 8, 9]] | transpose
[(1, 4, 7), (2, 5, 8), (3, 6, 9)]
>>>
```

### `traverse`

Recursively unfold iterables:

```python
>>> list([[1, 2], [[[3], [[4]]], [5]]] | traverse)
[1, 2, 3, 4, 5]
>>> squares = (i * i for i in range(3))
>>> list([[0, 1, 2], squares] | traverse)
[0, 1, 2, 0, 1, 4]
>>>
```

### `uniq(key=None)`


Like dedup() but only deduplicate consecutive values, using the given
`key` function if provided (or else the identity).

```python
>>> from pipe import uniq
>>> list([1, 1, 2, 2, 3, 3, 1, 2, 3] | uniq)
[1, 2, 3, 1, 2, 3]
>>> list([1, -1, 1, 2, -2, 2, 3, 3, 1, 2, 3] | uniq(key=abs))
[1, 2, 3, 1, 2, 3]
>>>
```

### `where(predicate)`, `filter(predicate)`

Only yields the matching items of the given iterable:

```python
>>> list([1, 2, 3] | where(lambda x: x % 2 == 0))
[2]
>>>
```

Don't forget they can be aliased:

```python
>>> positive = where(lambda x: x > 0)
>>> negative = where(lambda x: x < 0)
>>> sum([-10, -5, 0, 5, 10] | positive)
15
>>> sum([-10, -5, 0, 5, 10] | negative)
-15
>>>
```

## Constructing your own

You can construct your pipes using the `Pipe` class like:

```python
from pipe import Pipe
square = Pipe(lambda iterable: (x ** 2 for x in iterable))
map = Pipe(lambda iterable, fct: builtins.map(fct, iterable)
>>>
```

As you can see it's often very short to write, and with a bit of luck
the function you're wrapping already takes an iterable as the first
argument, making the wrapping straight forward:

```python
>>> from collections import deque
>>> from pipe import Pipe
>>> end = Pipe(deque)
>>>
```

and that's it `itrable | end(3)` is `deque(iterable, 3)`:

```python
>>> list(range(100) | end(3))
[97, 98, 99]
>>>
```

In case it gets more complicated one can use `Pipe` as a decorator to
a function taking an iterable as the first argument, and any other
optional arguments after:

```python
>>> from statistics import mean

>>> @Pipe
... def running_average(iterable, width):
...     items = deque(maxlen=width)
...     for item in iterable:
...         items.append(item)
...         yield mean(items)

>>> list(range(20) | running_average(width=2))
[0, 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5]
>>> list(range(20) | running_average(width=10))
[0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5]
>>>
```


## One-off pipes

Sometimes you just want a one-liner, when creating a pipe you can specify the function's positional and named arguments directly

```python
>>> from itertools import combinations

>>> list(range(5) | Pipe(combinations, 2))
[(0, 1), (0, 2), (0, 3), (0, 4), (1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)]
>>>
```

a simple running sum with initial starting value

```python
>>> from itertools import accumulate

>>> list(range(10) | Pipe(accumulate, initial=1))
[1, 1, 2, 4, 7, 11, 16, 22, 29, 37, 46]
>>>
```

or filter your data based on some criteria

```python
>>> from itertools import compress

list(range(20) | Pipe(compress, selectors=[1, 0] * 10))
[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
>>> list(range(20) | Pipe(compress, selectors=[0, 1] * 10))
[1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
>>>
```

## Euler project samples

> Find the sum of all the multiples of 3 or 5 below 1000.

```python
>>> sum(count() | where(lambda x: x % 3 == 0 or x % 5 == 0) | take_while(lambda x: x < 1000))
233168
>>>
```

> Find the sum of all the even-valued terms in Fibonacci which do not
> exceed four million.

```python
sum(fib() | where(lambda x: x % 2 == 0) | take_while(lambda x: x < 4000000))
```

> Find the difference between the sum of the squares of the first one
> hundred natural numbers and the square of the sum.

```python
>>> square = map(lambda x: x ** 2)
>>> sum(range(101)) ** 2 - sum(range(101) | square)
25164150
>>>
```


# Going deeper
## Partial Pipes

A `pipe` can be parametrized without being evaluated:

```python
>>> running_average_of_two = running_average(2)
>>> list(range(20) | running_average_of_two)
[0, 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5]
>>>
```

For multi-argument pipes then can be partially initialized, you can think of curying:

```python
some_iterable | some_pipe(1, 2, 3)
some_iterable | Pipe(some_func, 1, 2, 3)
```

is strictly equivalent to:

```python
some_iterable | some_pipe(1)(2)(3)
```

So it can be used to specialize pipes, first a dummy example:

```python
>>> @Pipe
... def addmul(iterable, to_add, to_mul):
...     """Computes (x + to_add) * to_mul to every items of the input."""
...     for i in iterable:
...         yield (i + to_add) * to_mul

>>> mul = addmul(0)  # This partially initialize addmul with to_add=0
>>> list(range(10) | mul(10))
[0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

```

Which also works with keyword arguments:

```python
>>> add = addmul(to_mul=1)  # This partially initialize addmul with `to_mul=1`
>>> list(range(10) | add(10))
[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
>>>
```


But now for something interesting:

```python
>>> import re
>>> @Pipe
... def grep(iterable, pattern, flags=0):
...     for line in iterable:
...         if re.match(pattern, line, flags=flags):
...             yield line
...
>>> lines = ["Hello", "hello", "World", "world"]
>>> for line in lines | grep("H"):
...     print(line)
Hello
>>>
```

Now let's reuse it in two ways, first with a pattern:

```python
>>> lowercase_only = grep("[a-z]+$")
>>> for line in lines | lowercase_only:
...     print(line)
hello
world
>>>
```

Or now with a flag:

```python
>>> igrep = grep(flags=re.IGNORECASE)
>>> for line in lines | igrep("hello"):
...    print(line)
...
Hello
hello
>>>
```


## Lazy evaluation

Pipe uses generators all the way down, so it is naturally lazy.

In the following examples we'll use
[itertools.count](https://docs.python.org/fr/3/library/itertools.html#itertools.count):
an infinite generator of integers.

We'll make use of the `tee` pipe too, which prints every values that
passe through it.

The following example does nothing, nothing is printed by `tee` so no
value passed through it. It's nice because generating an infinite
sequence of squares is "slow".

```python
>>> result = count() | tee | select(lambda x: x ** 2)
>>>
```

Chaining more pipes still won't make previous ones start generating
values, in the following example not a single value is pulled out of
`count`:

```python
>>> result = count() | tee | select(lambda x: x ** 2)
>>> first_results = result | take(10)
>>> only_odd_ones = first_results | where(lambda x: x % 2)
>>>
```

Same without variables:

```python
>>> result = (count() | tee
...                   | select(lambda x: x ** 2)
...                   | take(10)
...                   | where(lambda x: x % 2))
>>>
```


Only when values are actually needed, the generators starts to work.

In the following example only two values will be extracted out of `count`:
- `0` which is squared (to `0`), passes the `take(10)` eaily,
  but is dropped by `where`
- `1` which is squared (to `1`), also easily passes the `take(10)`,
  passes the `where`, and passes the `take(1)`.

At this point `take(1)` is satisfied so no other computations need to
be done. Notice `tee` printing `0` and `1` passing through it:

```python
>>> result = (count() | tee
...                   | select(lambda x: x ** 2)
...                   | take(10)
...                   | where(lambda x: x % 2))
>>> print(list(result | take(1)))
0
1
[1]
>>>
```

## Deprecations

In pipe 1.x a lot of functions were returning iterables and a lot
other functions were returning non-iterables, causing confusion. The
one returning non-iterables could only be used as the last function of
a pipe expression, so they are in fact useless:

```python
range(100) | where(lambda x: x % 2 == 0) | add
```

can be rewritten with no less readability as:

```python
sum(range(100) | where(lambda x: x % 2 == 0))
```

so all pipes returning non-iterables were deprecated (raising
warnings), and finally removed in pipe 2.0.


## What should I do?

Oh, you just upgraded pipe, got an exception, and landed here? You
have three solutions:


1) Stop using closing-pipes, replace `...|...|...|...|as_list` to
   `list(...|...|...|)`, that's it, it's even shorter.

2) If "closing pipes" are not an issue for you, and you really like
   them, just reimplement the few you really need, it often take a very
   few lines of code, or copy them from
   [here](https://github.com/JulienPalard/Pipe/blob/dd179c8ff0aa28ee0524f3247e5cb1c51347cba6/pipe.py).

3) If you still rely on a lot of them and are in a hurry, just `pip install pipe<2`.


And start testing your project using the [Python Development
Mode](https://docs.python.org/3/library/devmode.html) so you catch
those warnings before they bite you.


## But I like them, pleassssse, reintroduce them!

This has already been discussed in [#74](https://github.com/JulienPalard/Pipe/issues/74).

An `@Pipe` is often easily implemented in a 1 to 3 lines of code
function, and the `pipe` module does not aim at giving all
possibilities, it aims at giving the `Pipe` decorator.

So if you need more pipes, closing pipes, weird pipes, you-name-it,
feel free to implement them on your project, and consider the
already-implemented ones as examples on how to do it.

See the `Constructing your own` paragraph below.
