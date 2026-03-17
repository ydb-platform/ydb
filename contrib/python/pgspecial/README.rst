Meta-commands for Postgres
--------------------------

|Build Status|  |PyPI|

This package provides an API to execute meta-commands (AKA "special", or
"backslash commands") on PostgreSQL.

Quick Start
-----------

This is a python package. It can be installed with:

::

    $ pip install pgspecial


Usage
-----

Once this library is included into your project, you will most likely use the
following imports:

.. code-block:: python

    from pgspecial.main import PGSpecial
    from pgspecial.namedqueries import NamedQueries
    from psycopg2.extensions import cursor

Then you will create and use an instance of PGSpecial:

.. code-block:: python

        pgspecial = PGSpecial()
        for result in pgspecial.execute(cur: cursor, sql):
            # Do something

If you want to import named queries from an existing config file, it is
convenient to initialize and keep around the class variable in
``NamedQueries``:

.. code-block:: python

    from configobj import ConfigObj

    NamedQueries.instance = NamedQueries.from_config(
        ConfigObj('~/.config_file_name'))

Contributions:
--------------

If you're interested in contributing to this project, first of all I would like
to extend my heartfelt gratitude. I've written a small doc to describe how to
get this running in a development setup.

https://github.com/dbcli/pgspecial/blob/master/DEVELOP.rst

Please feel free to file an issue if you need help.

Projects using it:
------------------

This library is used by the following projects:

pgcli_: A REPL for Postgres.

`ipython-sql`_: %%sql magic for IPython

OmniDB_: An web tool for database management

If you find this module useful and include it in your project, I'll be happy
to know about it and list it here.

.. |Build Status| image:: https://github.com/dbcli/pgspecial/workflows/pgspecial/badge.svg
    :target: https://github.com/dbcli/pgspecial/actions?query=workflow%3Apgspecial

.. |PyPI| image:: https://badge.fury.io/py/pgspecial.svg
    :target: https://pypi.python.org/pypi/pgspecial/
    :alt: Latest Version

.. _pgcli: https://github.com/dbcli/pgcli
.. _`ipython-sql`: https://github.com/catherinedevlin/ipython-sql
.. _OmniDB: https://github.com/OmniDB/OmniDB
