# Infix programming toolkit

Module enabling a sh like infix syntax (using pipes).


# Introduction

As an example, here is the solution for the 2nd Euler Project exercise:

> Find the sum of all the even-valued terms in Fibonacci which do not
  exceed four million.

Given fib a generator of Fibonacci numbers:

    euler2 = (fib() | where(lambda x: x % 2 == 0)
                    | take_while(lambda x: x < 4000000)
                    | add)


# Deprecations of pipe 1.x

In pipe 1.x a lot of functions were returning iterables and a lot
other functions were returning non-iterables, causing confusion. The
one returning non-iterables could only be used as the last function of
a pipe expression, so they are in fact useless:

    range(100) | where(lambda x: x % 2 == 0) | add

can be rewritten with no less readability as:

    sum(range(100) | where(lambda x: x % 2 == 0))

so all pipes returning a non-pipe are now deprecated and will be removed in pipe 2.0.


# Vocabulary

- A Pipe: a Pipe is a 'pipeable' function, something that you can pipe to,
  In the code '[1, 2, 3] | add' add is a Pipe
- A Pipe function: A standard function returning a Pipe so it can be used like
  a normal Pipe but called like in : [1, 2, 3] | concat("#")


# Syntax

I don't like `import * `but for the following examples in an REPL it
will be OK, so:

    >>> from pipe import *

The basic syntax is to use a Pipe like in a shell:

    >>> sum(range(100) | select(lambda x: x ** 2) | where(lambda x: x < 100))
    285

Some pipes take an argument, some do not need one:

    >>> sum([1, 2, 3, 4] | where(lambda x: x % 2 == 0))
    6

    >>> sum([1, [2, 3], 4] | traverse)
    10

A Pipe as a function is nothing more than a function returning
a specialized Pipe.


# Constructing your own

You can construct your pipes using Pipe class initialized with lambdas like:

    stdout = Pipe(lambda x: sys.stdout.write(str(x)))
    select = Pipe(lambda iterable, pred: (pred(x) for x in iterable))

Or using decorators:

    @Pipe
    def stdout(x):
        sys.stdout.write(str(x))


# Existing Pipes in this module

    tee
        tee outputs to the standard output and yield unchanged items, usefull for
        debugging
        >>> sum([1, 2, 3, 4, 5] | tee)
        1
        2
        3
        4
        5
        15

    chain
        Chain a sequence of iterables:
        >>> list([[1, 2], [3, 4], [5]] | chain)
        [1, 2, 3, 4, 5]

        Warning : chain only unfold iterable containing ONLY iterables:
          [1, 2, [3]] | chain
        Gives a TypeError: chain argument #1 must support iteration
        Consider using traverse.

    traverse
        Recursively unfold iterables:
        >>> list([[1, 2], [[[3], [[4]]], [5]]] | traverse)
        [1, 2, 3, 4, 5]
        >>> squares = (i * i for i in range(3))
        >>> list([[0, 1, 2], squares] | traverse)
        [0, 1, 2, 0, 1, 4]

    map()
        Apply a conversion expression given as parameter
        to each element of the given iterable
        >>> list([1, 2, 3] | map(lambda x: x * x))
        [1, 4, 9]

    select()
        An alias for map().
        >>> list([1, 2, 3] | select(lambda x: x * x))
        [1, 4, 9]

    where()
        Only yields the matching items of the given iterable:
        >>> list([1, 2, 3] | where(lambda x: x % 2 == 0))
        [2]

    take_while()
        Like itertools.takewhile, yields elements of the
        given iterable while the predicat is true:
        >>> list([1, 2, 3, 4] | take_while(lambda x: x < 3))
        [1, 2]

    skip_while()
        Like itertools.dropwhile, skips elements of the given iterable
        while the predicat is true, then yields others:
        >>> list([1, 2, 3, 4] | skip_while(lambda x: x < 3))
        [3, 4]

    chain_with()
        Like itertools.chain, yields elements of the given iterable,
        then yields elements of its parameters
        >>> list((1, 2, 3) | chain_with([4, 5], [6]))
        [1, 2, 3, 4, 5, 6]

    take()
        Yields the given quantity of elemenets from the given iterable, like head
        in shell script.
        >>> list((1, 2, 3, 4, 5) | take(2))
        [1, 2]

    tail()
        Yiels the given quantity of the last elements of the given iterable.
        >>> list((1, 2, 3, 4, 5) | tail(3))
        [3, 4, 5]

    skip()
        Skips the given quantity of elements from the given iterable, then yields
        >>> list((1, 2, 3, 4, 5) | skip(2))
        [3, 4, 5]

    islice()
        Just the itertools.islice
        >>> list((1, 2, 3, 4, 5, 6, 7, 8, 9) | islice(2, 8, 2))
        [3, 5, 7]

    izip()
        Just the itertools.izip
        >>> list((1, 2, 3, 4, 5, 6, 7, 8, 9)
        ...  | izip([9, 8, 7, 6, 5, 4, 3, 2, 1]))
        [(1, 9), (2, 8), (3, 7), (4, 6), (5, 5), (6, 4), (7, 3), (8, 2), (9, 1)]

    groupby()
        Like itertools.groupby(sorted(iterable, key = keyfunc), keyfunc)
        (1, 2, 3, 4, 5, 6, 7, 8, 9) \
                | groupby(lambda x: x % 2 and "Even" or "Odd")
                | select(lambda x: "%s : %s" % (x[0], (x[1] | concat(', '))))
                | concat(' / ')
        'Even : 1, 3, 5, 7, 9 / Odd : 2, 4, 6, 8'

    sort()
        Like Python's built-in "sorted" primitive. Allows cmp (Python 2.x
        only), key, and reverse arguments. By default sorts using the
        identity function as the key.

        >>> ''.join("python" | sort)
        'hnopty'
        >>> list([5, -4, 3, -2, 1] | sort(key=abs))
        [1, -2, 3, -4, 5]

    dedup()
        Deduplicate values, using the given key function if provided (or else
        the identity)

        >>> list([1, 1, 2, 2, 3, 3, 1, 2, 3] | dedup)
        [1, 2, 3]
        >>> list([1, 1, 2, 2, 3, 3, 1, 2, 3] | dedup(key=lambda n:n % 2))
        [1, 2]

    uniq()
        Like dedup() but only deduplicate consecutive values, using the given
        key function if provided (or else the identity)

        >>> list([1, 1, 2, 2, 3, 3, 1, 2, 3] | uniq)
        [1, 2, 3, 1, 2, 3]
        >>> list([1, 1, 2, 2, 3, 3, 1, 2, 3] | uniq(key=lambda n:n % 2))
        [1, 2, 3, 2, 3]

    reverse
        Like Python's built-in "reversed" primitive.
        >>> list([1, 2, 3] | reverse)
        [3, 2, 1]

    strip
        Like Python's strip-method for str.
        >>> '  abc   ' | strip
        'abc'
        >>> '.,[abc] ] ' | strip('.,[] ')
        'abc'

    rstrip
        Like Python's rstrip-method for str.
        >>> '  abc   ' | rstrip
        '  abc'
        >>> '.,[abc] ] ' | rstrip('.,[] ')
        '.,[abc'

    lstrip
        Like Python's lstrip-method for str.
        >>> 'abc   ' | lstrip
        'abc   '
        >>> '.,[abc] ] ' | lstrip('.,[] ')
        'abc] ] '

    t
        Like Haskell's operator ":"
        >>> list(0 | t(1) | t(2)) == list(range(3))
        True

    permutations()
        Returns all possible permutations
        >>> list('ABC' | permutations(2))
        [('A', 'B'), ('A', 'C'), ('B', 'A'), ('B', 'C'), ('C', 'A'), ('C', 'B')]

        >>> list(range(3) | permutations)
        [(0, 1, 2), (0, 2, 1), (1, 0, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0)]

    transpose()
        Transposes the rows and columns of a matrix
        >>> [[1, 2, 3], [4, 5, 6], [7, 8, 9]] | transpose
        [(1, 4, 7), (2, 5, 8), (3, 6, 9)]


# Euler project samples

> Find the sum of all the multiples of 3 or 5 below 1000.

    euler1 = (itertools.count() | select(lambda x: x * 3) | take_while(lambda x: x < 1000) | add) \
           + (itertools.count() | select(lambda x: x * 5) | take_while(lambda x: x < 1000) | add) \
           - (itertools.count() | select(lambda x: x * 15) | take_while(lambda x: x < 1000) | add)
    assert euler1 == 233168

> Find the sum of all the even-valued terms in Fibonacci which do not exceed four million.

    euler2 = fib() | where(lambda x: x % 2 == 0) | take_while(lambda x: x < 4000000) | add
    assert euler2 == 4613732

> Find the difference between the sum of the squares of the first one hundred natural numbers and the square of the sum.

    square = lambda x: x * x
    euler6 = square(itertools.count(1) | take(100) | add) - (itertools.count(1) | take(100) | select(square) | add)
    assert euler6 == 25164150
