
SimpleDiff 1.0
==============

A Python module to annotate two versions of a list with the
values that have been changed between the versions, similar
to unix's `diff` but with a dead-simple Python interface.

Install
-------

SimpleDiff can be installed from `pip` or `easy_install`

    $ pip install simplediff

Test
----

You can use `test.py` to run the included doctests

    $ python test.py

No output means that all the tests have passed.

Use
---

The module exposes three functions, `diff`, `string_diff`,
and `html_diff`.

### `diff`

`diff` is the core of the module and provides basic diffing
functionality on list objects. The lists could represent
lines of a file, tokens from a tokenizer, or just about
anything else. `diff` is agnostic towards the content of the
list, all it cares about is which elements between the
`old` and `new` list are equal.

    >>> from simplediff import diff

Because strings share an accessor interface with lists in
Python, `diff` automatically works on strings at a character
level.

    >>> diff('shark', 'spork')
    ... # doctest: +NORMALIZE_WHITESPACE
    [('=', 's'),
     ('-', 'ha'),
     ('+', 'po'),
     ('=', 'rk')]

The output in this case is quite simple. `('=', 's')` indicates
that the `s` is in both strings. `('-', 'ha')` indicates that
`ha` appears in the first string but not the second.
`('+', 'po')` indicates that `po` appears in the second string
but not the first. Finally, `rk` appears in both.

This is a bit of an over-simplification, though. Consider this
case:

    >>> diff('aaaabbb', 'bbbaaaa')
    ... # doctest: +NORMALIZE_WHITESPACE
    [('+', 'bbb'),
     ('=', 'aaaa'),
     ('-', 'bbb')]

Note that although 'bbb' is contained in both strings, it is
an insertion and then a deletion in the resulting diff.
Note also that it was the longer string, 'aaaa', that was
chosen as unchanged; SimpleDiff tries to maximize the
length of the unchanged data and minimize the number of
inserts and deletes.

Since `diff`'s arguments are really lists, you can easily
tokenize a string (in this case, by whitespace) and provide
that as input.

    >>> diff('I like icecream'.split(),
    ...      'I do not like icecream'.split())
    ... # doctest: +NORMALIZE_WHITESPACE
    [('=', ['I']),
     ('+', ['do', 'not']),
     ('=', ['like', 'icecream'])]

Note that the result is now lists of strings, not strings.

### `string_diff` and `html_diff`

`string_diff` and `html_diff` take strings, tokenize them
on whitespace, and return the `diff` output or HTML code
respectively. These are meant as an example implementation,
they're probably not what you want in practice.

    >>> from simplediff import string_diff, html_diff

    >>> string_diff('I like icecream',
    ...             'I do not like icecream')
    ... # doctest: +NORMALIZE_WHITESPACE
    [('=', ['I']),
     ('+', ['do', 'not']),
     ('=', ['like', 'icecream'])]

This is the same output as with `diff`, but note that this
time we didn't have to call `.split()` on the input strings.

    >>> html_diff('I like vanilla cupcakes',
    ...           'I do not like cupcakes')
    ... # doctest: +NORMALIZE_WHITESPACE
    'I <ins>do not</ins> like <del>vanilla</del> cupcakes'

See the doctests in `simplediff/__init__.py` for more
examples.

License
-------

SimpleDiff is copyright 2008-2012 by Paul Butler. It may
be used and redistributed under a liberal zlib/libpng-like
license provided in the `LICENSE` file.
