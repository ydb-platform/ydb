sqlakeyset: offset-free paging for sqlalchemy
=============================================

.. image:: https://img.shields.io/circleci/build/gh/djrobstep/sqlakeyset?label=tests
    :alt: Tests
    :target: https://circleci.com/gh/djrobstep/sqlakeyset
    
.. image:: https://img.shields.io/pypi/v/sqlakeyset
    :alt: PyPI
    :target: https://pypi.org/project/sqlakeyset/
    
.. image:: https://img.shields.io/conda/vn/conda-forge/sqlakeyset.svg
    :alt: conda-forge
    :target: https://anaconda.org/conda-forge/sqlakeyset

.. image:: https://readthedocs.org/projects/sqlakeyset/badge/
   :alt: Documentation @ readthedocs
   :target: https://sqlakeyset.readthedocs.io/

sqlakeyset implements keyset-based paging for SQLAlchemy (both ORM and core). **Now with full SQLAlchemy 2 support and type hints!**

This library is tested with PostgreSQL, MariaDB/MySQL and SQLite. It should work with many other SQLAlchemy-supported databases, too; but caveat emptor - you should verify the results are correct.

Python version compatibility
----------------------------

**Notice:** In accordance with Python end-of-life dates, we've stopped supporting Python versions earlier than 3.8.

If you really need it, the latest version to support Python 2 is 0.1.1559103842, Python 3.4 is 1.0.1679209451, and Python 3.7 is 2.0.1733532871; but you'll miss out on all the latest features and bugfixes from newer versions. You should be upgrading anyway!

Background
----------

A lot of people use SQL's ``OFFSET`` syntax to implement paging of query results. The trouble with that is, the more pages you get through, the slower your query gets. Also, if the results you're paging through change frequently, it's possible to skip over or repeat results between pages. Keyset paging avoids these problems: Selecting even the millionth page is as fast as selecting the first.


Getting Started
---------------

Here's how it works with a typical SQLAlchemy 2.0-style query (or SQLAlchemy 1.3 Core):

.. code-block:: python

    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import Session
    from sqlakeyset import select_page

    from models import Book

    engine = create_engine('postgresql:///books')
    with Session(engine) as s:
        q = select(Book).order_by(Book.author, Book.title, Book.id)

        # gets the first page
        page1 = select_page(s, q, per_page=20)

        # gets the key for the next page
        next_page = page1.paging.next

        # gets the second page
        page2 = select_page(s, q, per_page=20, page=next_page)

        # returning to the first page, getting the key
        previous_page = page2.paging.previous

        # the first page again, backwards from the previous page
        page1 = select_page(s, q, per_page=20, page=previous_page)

        # what if new items were added at the start?
        if page1.paging.has_previous:

            # go back even further
            previous_page = page1.paging.previous
            page1 = select_page(s, q, per_page=20, page=previous_page)


If you're still using legacy (i.e. SQLAlchemy 1.3-style) ORM queries, you can
use ``get_page`` instead, which has an identical API other than the omission of
the session/connection argument:

.. code-block:: python

    from sqlakeyset import get_page
    with Session(engine) as s:
        q = s.query(Book).order_by(Book.author, Book.title, Book.id)
        page1 = get_page(q, per_page=20)
        # ...

We also support asyncio, and the API is near-identical - just import from
``sqlakeyset.asyncio`` and pass an ``AsyncSession``:

.. code-block:: python

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlakeyset.asyncio import select_page
    engine = create_async_engine('postgresql:///books')
    async with AsyncSession(engine) as s:
        q = select(Book).order_by(Book.author, Book.title, Book.id)
        page1 = await select_page(s, q, per_page=20)
        # ...


Under the Hood
--------------

sqlakeyset does the following to your query in order to get the paged contents:

- adds a where clause, to get only rows after the specified row key.
- if getting the previous page, reverses the ``order by`` direction in order the get the rows *before* the specified bookmark.
- adds a limit clause, to fetch only enough items to fill the page, plus one additional (this additional row is used only to test for the existence of further pages after the current one, and is discarded from the results).
- returns the page contents as an ordinary list that has an attached ``.paging`` attribute with the paging information for this and related pages.


Page objects
------------

Paged items/rows are returned in a ``Page`` object, which is a vanilla python list extended by an attached ``Paging`` object containing paging information.

Properties such as `next` and `previous` return a pair containing the ordering key for the row, and a boolean to specify if the direction is forwards or backwards. We refer to such a pair ``(keyset, backwards)`` as a *marker*.

In our above example, the marker specifying the second page might look like:

.. code-block:: python

    ('Joseph Heller', 'Catch 22', 123), False

The `False` means the query will fetch the page *after* the row containing Catch 22. This tuple contains two elements, title and id, to match the order by clause of the query.

The page before this row would be specified as:

.. code-block:: python

    ('Joseph Heller', 'Catch 22', 123), True

The first and last pages are fetched with `None` instead of a tuple, so for the first page (this is also the default if the page parameter is not specified):

.. code-block:: python

    None, False

And the last page:

.. code-block:: python

    None, True

Keyset Serialization
--------------------

You will probably want to turn these markers into strings for passing around. ``sqlakeyset`` includes code to do this, and calls the resulting strings *bookmarks*. To get a serialized bookmark, just add ``bookmark_`` to the name of the property that holds the keyset you want.

Most commonly you'll want ``next`` and ``previous``, so:

.. code-block:: python

    >>> page.paging.bookmark_previous
    <i:1~i:2015~s:Bad Blood~i:34
    >>> page.paging.bookmark_next
    >i:1~i:2014~s:Shake It Off~i:31

``sqlakeyset`` uses the python csv row serializer to serialize the bookmark values (using ``~`` instead of a ``,`` as the separator). Direction is indicated by ``>`` (forwards/next), or ``<`` (backwards/previous) at the start of the string.

Limitations
-----------

- **Golden Rule:** Always ensure your keysets are unique per row. If you violate this condition you risk skipped rows and other nasty problems. The simplest way to do this is to always include your primary key column(s) at the end of your ordering columns.

- Any rows containing null values in their keysets **will be omitted from the results**, so your ordering columns should be ``NOT NULL``. (This is a consequence of the fact that comparisons against ``NULL`` are always false in SQL.) This may change in the future if we work out an alternative implementation; but for now we recommend using ``coalesce`` as a workaround if you need to sort by nullable columns:

.. code-block:: python

    from sqlalchemy import func
    with Session(engine) as s:
        # If Book.cost can be NULL:
        q = select(Book).order_by(func.coalesce(Book.cost, 0), Book.id)
        # Assuming cost is non-negative, page1 will start with books where cost is null:
        page1 = select_page(s, q, per_page=20)

- If you're using the in-built keyset serialization, this only handles basic data/column types so far (strings, ints, floats, datetimes, dates, booleans, and a few others). The serialization can be extended to serialize more advanced types as necessary (documentation on this is forthcoming).


Documentation
-------------

Other than this README, there is some more detailed API documentation autogenerated from docstrings, which you can `read online at readthedocs.io <https://sqlakeyset.readthedocs.io/>`_ or build yourself with e.g. ``make -C doc html``.


Installation
------------

Assuming you have `pip <https://pip.pypa.io>`_ installed, all you need to do is install as follows:

.. code-block:: shell

    $ pip install sqlakeyset

This will install sqlakeyset and also sqlalchemy if not already installed. Obviously you'll need the necessary database driver for your chosen database to be installed also.
