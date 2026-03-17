Flake8 Extension to lint for quotes.
===========================================

.. image:: https://travis-ci.org/zheller/flake8-quotes.svg?branch=master
   :target: https://travis-ci.org/zheller/flake8-quotes
   :alt: Build Status

Major update in 2.0.0
---------------------
We automatically encourage avoiding escaping quotes as per `PEP 8 <https://www.python.org/dev/peps/pep-0008/#string-quotes>`_. To disable this, use ``--no-avoid-escape`` (can be used in configuration file via ``avoid-escape``).

Deprecation notice in 0.3.0
---------------------------
To anticipate multiline support, we are renaming ``--quotes`` to ``--inline-quotes``. Please adjust your configurations appropriately.

Usage
-----

If you are using flake8 it's as easy as:

.. code:: shell

    pip install flake8-quotes

Now you don't need to worry about people like @sectioneight constantly
complaining that you are using double-quotes and not single-quotes.

Warnings
--------

This package adds flake8 warnings with the prefix ``Q0``. You might want to
enable this warning inside your flake8 configuration file. Typically that
will be ``.flake8`` inside the root folder of your project.

.. code:: ini

    select = Q0

The current set of warnings is:

==== =========================================================================
Code Description
---- -------------------------------------------------------------------------
Q000 Remove bad quotes
Q001 Remove bad quotes from multiline string
Q002 Remove bad quotes from docstring
Q003 Change outer quotes to avoid escaping inner quotes
==== =========================================================================

Configuration
-------------

By default, we expect single quotes (') and look for unwanted double quotes ("). To expect double quotes (") and find unwanted single quotes ('), use the CLI option:

.. code:: shell

    flake8 --inline-quotes '"'
    # We also support "double" and "single"
    # flake8 --inline-quotes 'double'
    #
    # We also support configuration for multiline quotes
    # flake8 --inline-quotes '"' --multiline-quotes "'"
    # We also support "'''"
    # flake8 --inline-quotes '"' --multiline-quotes "'''"
    #
    # We also support docstring quotes similarly
    # flake8 --inline-quotes '"' --docstring-quotes "'"
    # flake8 --inline-quotes '"' --docstring-quotes "'''"

    # We also support disabling escaping quotes
    # flake8 --no-avoid-escape

or configuration option in `tox.ini`/`setup.cfg`.

.. code:: ini

    [flake8]
    inline-quotes = "
    # We also support "double" and "single"
    # inline-quotes = double
    #
    # We also support configuration for multiline quotes
    # multiline-quotes = '
    # We also support "'''"
    # multiline-quotes = '''
    #
    # We also support docstring quotes similarly
    # docstring-quotes = '
    # docstring-quotes = '''
    #
    # We also support disabling escaping quotes
    # avoid-escape = False

Caveats
-------

We follow the `PEP8 conventions <https://www.python.org/dev/peps/pep-0008/#string-quotes>`_ to avoid backslashes in the string. So, no matter what configuration you are using (single or double quotes) these are always valid strings

.. code:: python

    s = 'double "quotes" wrapped in singles are ignored'
    s = "single 'quotes' wrapped in doubles are ignored"
