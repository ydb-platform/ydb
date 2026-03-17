Psycopg 3: PostgreSQL database adapter for Python
=================================================

Psycopg 3 is a modern implementation of a PostgreSQL adapter for Python.

This distribution contains the pure Python package ``psycopg``.

.. Note::

    Despite the lack of number in the package name, this package is the
    successor of psycopg2_.

    Please use the psycopg2 package if you are maintaining an existing program
    using psycopg2 as a dependency. If you are developing something new,
    Psycopg 3 is the most current implementation of the adapter.

    .. _psycopg2: https://pypi.org/project/psycopg2/


Installation
------------

In short, run the following::

    pip install --upgrade pip           # to upgrade pip
    pip install "psycopg[binary,pool]"  # to install package and dependencies

If something goes wrong, and for more information about installation, please
check out the `Installation documentation`__.

.. __: https://www.psycopg.org/psycopg3/docs/basic/install.html#


Hacking
-------

For development information check out `the project readme`__.

.. __: https://github.com/psycopg/psycopg#readme


Copyright (C) 2020 The Psycopg Team
