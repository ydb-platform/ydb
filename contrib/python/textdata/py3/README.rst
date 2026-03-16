
| |travisci| |version| |versions| |impls| |wheel| |coverage|

.. |travisci| image:: https://travis-ci.org/jonathaneunice/textdata.svg?branch=master
    :alt: Travis CI build status
    :target: https://travis-ci.org/jonathaneunice/textdata

.. |version| image:: http://img.shields.io/pypi/v/textdata.svg?style=flat
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/textdata

.. |versions| image:: https://img.shields.io/pypi/pyversions/textdata.svg
    :alt: Supported versions
    :target: https://pypi.org/project/textdata

.. |impls| image:: https://img.shields.io/pypi/implementation/textdata.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/textdata

.. |wheel| image:: https://img.shields.io/pypi/wheel/textdata.svg
    :alt: Wheel packaging support
    :target: https://pypi.org/project/textdata

.. |coverage| image:: https://img.shields.io/badge/test_coverage-99%25-blue.svg
    :alt: Test line coverage
    :target: https://pypi.org/project/textdata

One often needs to state data in program source. Python, however, needs its
program lines indented *just so*. Multi-line strings therefore often have extra
spaces and newline characters you didn't really want. Many developers "fix"
this by using Python ``list`` literals, but that's tedious, verbose, and often
less legible.

The ``textdata`` package makes it easy to have clean, nicely-whitespaced
data specified in your program, but to get the data without extra syntax
cluttering things up. It's permissive of the layouts needed to make Python
code look and work right, without reflecting those requirements in the
resulting data.

Text (Strings and Lists)
------------------------

.. code-block:: pycon

    >>> lines("""
    ...     There was an old woman who lived in a shoe.
    ...     She had so many children, she didn't know what to do;
    ...     She gave them some broth without any bread;
    ...     Then whipped them all soundly and put them to bed.
    ... """)
    ['There was an old woman who lived in a shoe.',
     "She had so many children, she didn't know what to do;",
     'She gave them some broth without any bread;',
     'Then whipped them all soundly and put them to bed.']

Note that the "extra" newlines and leading spaces have been
taken care of and discarded. Or do you want that as just one
string? Okay:

.. code-block:: pycon

    >>> text("""
    ...     There was an old woman who lived in a shoe.
    ...     She had so many children, she didn't know what to do;
    ...     She gave them some broth without any bread;
    ...     Then whipped them all soundly and put them to bed.
    ... """)
    "There was an old woman who lived in a shoe.\nShe ...put them to bed."

Here ``text()`` does the same stripping of pointless whitespace at the beginning
and end of lines, returning the data as a clean, convenient string. Or if you
don't want most of the line endings, try ``textline`` on the same input to get a
single no-breaks line.

Words and Phrases
-----------------

Other times, the data you need is almost, but not quite, a series of
words. A list of names, a list of colors--values that are mostly
single words, but sometimes have an embedded spaces. ``textdata`` has you
covered:

.. code-block:: pycon

    >>> words(' Billy Bobby "Mr. Smith" "Mrs. Jones"  ')
    ['Billy', 'Bobby', 'Mr. Smith', 'Mrs. Jones']

Embedded quotes (either single or double) can be used to construct
"words" (or phrases) containing whitespace (including tabs and newlines).

``words``, like the other ``textdata`` facilities, allows you to
comment individual lines that would otherwise muck up string literals:

.. code-block:: pycon

    exclude = words("""
        __pycache__ *.pyc *.pyo     # compilation artifacts
        .hg* .git*                  # repository artifacts
        .coverage                   # code tool artifacts
        .DS_Store                   # platform artifacts
    """)

Yields:

.. code-block:: pycon

    ['__pycache__', '*.pyc', '*.pyo', '.hg*', '.git*',
     '.coverage', '.DS_Store']

Paragraphs
----------

Instead of words, you might wan to collect "paragraphs"--contiguous runs of text
lines delineated by blank lines. Markdown and RST document formats, for example,
use this convention.

.. code-block:: pycon

    >>> rhyme = """
        Hey diddle diddle,

        The cat and the fiddle,
        The cow jumped over the moon.
        The little dog laughed,
        To see such sport,

        And the dish ran away with the spoon.
    """
    >>> paras(rhyme)
    [['Hey diddle diddle,'],
     ['The cat and the fiddle,',
      'The cow jumped over the moon.',
      'The little dog laughed,',
      'To see such sport,'],
     ['And the dish ran away with the spoon.']]

Or if you'd like paras, but each paragraph in a single string:

.. code-block:: pycon

    >>> paras(rhyme, join="\n")
    ['Hey diddle diddle,',
     'The cat and the fiddle,\nThe cow jumped over the moon.\nThe little dog laughed,\nTo see such sport,',
     'And the dish ran away with the spoon.']

Dictionaries
------------

Or maybe you want a ``dict``. The ``attrs`` function makes it easy to
grab::

.. code-block:: pycon

    >>> attrs("a=1 b=2 c='something more'")
    {'a': 1, 'b': 2, 'c': 'something more'}

If you want to cut and paste data directly from JavaScript, JSON, HTML, CSS, or
XML, easy peasy! No text editing required.

.. code-block:: pycon

    >>> # JavaScript
    >>> attrs("a: 1, b: 2, c: 'something more'")
    {'a': 1, 'b': 2, 'c': 'something more'}

    >>> # JSON
    >>> attrs('"a": 1, "b": 2, "c": "something more"')
    {'a': 1, 'b': 2, 'c': 'something more'}

    >>> # HTML or XML
    >>> attrs('a="1" b="2" c="something more"')
    {'a': '1', 'b': '2', 'c': 'something more'}

    >>> # above returns strings, because values quoted, which denotes strings
    >>> # 'full' evaluation needed to transform strings into values
    >>> attrs('a="1" b="2" c="something more"', evaluate='full')
    {'a': 1, 'b': 2, 'c': 'something more'}

    >>> # CSS
    >>> attrs("a: 1; b: 2; c: 'something more'")
    {'a': 1, 'b': 2, 'c': 'something more'}


Tables
------

Or maybe you have tabular data.

.. code-block:: pycon

    >>> tabledata = """
    ...     name  age  strengths
    ...     ----  ---  ---------------
    ...     Joe   12   woodworking
    ...     Jill  12   slingshot
    ...     Meg   13   snark, snapchat
    ... """

    >>> table(tabledata)
    [['name', 'age', 'strengths'],
     ['Joe', 12, 'woodworking'],
     ['Jill', 12, 'slingshot'],
     ['Meg', 13, 'snark, snapchat']]

    >>> records(tabledata)
    [{'name': 'Joe', 'age': 12, 'strengths': 'woodworking'},
     {'name': 'Jill', 'age': 12, 'strengths': 'slingshot'},
     {'name': 'Meg', 'age': 13, 'strengths': 'snark, snapchat'}]

This works even if you have a table with a lot of extra fluff:

.. code-block:: pycon

    >>> fancy = """
    ... +------+-----+-----------------+
    ... | name | age | strengths       |
    ... +------+-----+-----------------+
    ... | Joe  |  12 | woodworking     |
    ... | Jill |  12 | slingshot       |
    ... | Meg  |  13 | snark, snapchat |
    ... +------+-----+-----------------+
    ... """
    >>> assert table(tabledata) == table(fancy)
    >>> assert records(tabledata) == records(fancy)

It works with tables formatted in a variety of ways including Markdown, RST,
ANSI/Unicode line drawing characters, plain text columns and borders.... You'd
might think table parsing would be a dicey proposition, prone to failure, but
``textdata`` has *dozens* of tests, including rather complex cases, showing
it's a reliable, high-probability heuristic.

In Summary
----------

``textdata`` is all about conveniently grabbing the data you want from text
files and program source, and doing it in a highly functional, convenient,
well-tested way. Take it for a spin today!

See `the full documentation
at Read the Docs <https://textdata.readthedocs.org/en/latest/>`_.
