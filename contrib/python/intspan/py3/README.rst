
| |travisci| |version| |versions| |impls| |wheel| |coverage| |br-coverage|

.. |travisci| image:: https://api.travis-ci.org/jonathaneunice/intspan.svg
    :target: http://travis-ci.org/jonathaneunice/intspan

.. |version| image:: http://img.shields.io/pypi/v/intspan.svg?style=flat
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/intspan

.. |versions| image:: https://img.shields.io/pypi/pyversions/intspan.svg
    :alt: Supported versions
    :target: https://pypi.org/project/intspan

.. |impls| image:: https://img.shields.io/pypi/implementation/intspan.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/intspan

.. |wheel| image:: https://img.shields.io/pypi/wheel/intspan.svg
    :alt: Wheel packaging support
    :target: https://pypi.org/project/intspan

.. |coverage| image:: https://img.shields.io/badge/test_coverage-100%25-6600CC.svg
    :alt: Test line coverage
    :target: https://pypi.org/project/intspan

.. |br-coverage| image:: https://img.shields.io/badge/branch_coverage-100%25-6600CC.svg
    :alt: Test branch coverage
    :target: https://pypi.org/project/intspan

``intspan`` is a ``set`` subclass that conveniently represents sets of integers.
Sets can be created from, and displayed as, integer spans such as
``1-3,14,29,92-97`` rather than exhaustive member listings. Compare::

    intspan('1-3,14,29,92-97')
    [1, 2, 3, 14, 29, 92, 93, 94, 95, 96, 97]

Or worse, the unsorted, non-intuitive listings that crop up with Python's
native unordered sets, such as::

    set([96, 1, 2, 3, 97, 14, 93, 92, 29, 94, 95])

While they all indicate the same values, ``intspan`` output is much more compact
and comprehensible. It better divulges the contiguous nature of segments of the
collection, making it easier for humans to quickly determine the "shape" of the
data and ascertain "what's missing?"

When iterating, ``pop()``-ing an item, or converting to a list, ``intspan``
behaves as if it were an ordered--in fact, sorted--collection. A key
implication is that, regardless of the order in which items are added,
an ``intspan`` will always be rendered in the most compact, organized
form possible.

The main draw is having a convenient way to specify, manage, and see output in
terms of ranges--for example, rows to process in a spreadsheet. It can also help
you quickly identify or report which items were *not* successfully processed in
a large dataset.

There is also an ordered ``intspanlist`` type that helps specify the
ordering of a set of elements.

See the full details on `Read the Docs
<http://intspan.readthedocs.org/en/latest/>`_.
