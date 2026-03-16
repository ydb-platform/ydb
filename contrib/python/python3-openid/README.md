_NOTE_: This started out as a fork of the Python OpenID library, with changes
to make it Python 3 compatible. It's now a port of that library, including
cleanups and updates to the code in general.

[![Build Status](https://travis-ci.org/necaris/python3-openid.svg?branch=master)](https://travis-ci.org/necaris/python3-openid)
[![Coverage Status](https://coveralls.io/repos/necaris/python3-openid/badge.svg?branch=master&service=github)](https://coveralls.io/github/necaris/python3-openid?branch=master)

# requirements

- Python 3.5+ (tested on CPython 3.5-3.8, and PyPy3 (although some tests may fail on PyPy))

# installation

The recommended way is to install from PyPI with `pip`:

    pip install python3-openid

Alternatively, you can run the following command from a source checkout:

    python setup.py install

If you want to use MySQL or PostgreSQL storage options, be sure to install
the relevant "extra":

    pip install python3-openid[mysql]

# getting started

The library should follow the existing `python-openid` API as closely as possible.

_NOTE_: documentation will be auto-generated as soon as I can figure out how to
update the documentation tools.

_NOTE_: The examples directory includes an example server and consumer
implementation. See the README file in that directory for more
information on running the examples.

# logging

This library offers a logging hook that will record unexpected
conditions that occur in library code. If a condition is recoverable,
the library will recover and issue a log message. If it is not
recoverable, the library will raise an exception. See the
documentation for the `openid.oidutil` module for more on the logging
hook.

# documentation

The documentation in this library is in Epydoc format, which is
detailed at:

http://epydoc.sourceforge.net/

# contact

Bug reports, suggestions, and feature requests are [very welcome](issues)!

There are also the `#python-openid` and `#openid` channels on FreeNode IRC.

# contributors

- @necaris
- @moreati
- @vstoykov
- @earthday
- @bkmgit
