# pyodbc

[![AppVeyor](https://ci.appveyor.com/api/projects/status/github/mkleehammer/pyodbc?branch=master&svg=true&passingText=Windows%20build&failingText=Windows%20build)](https://ci.appveyor.com/project/mkleehammer/pyodbc)
[![Github Actions - Ubuntu Build](https://github.com/mkleehammer/pyodbc/actions/workflows/ubuntu_build.yml/badge.svg?branch=master)](https://github.com/mkleehammer/pyodbc/actions/workflows/ubuntu_build.yml)
[![PyPI](https://img.shields.io/pypi/v/pyodbc?color=brightgreen)](https://pypi.org/project/pyodbc/)

pyodbc is an open source Python module that makes accessing ODBC databases simple.  It
implements the [DB API 2.0](https://www.python.org/dev/peps/pep-0249) specification but is
packed with even more Pythonic convenience.

The easiest way to install pyodbc is to use pip:

    pip install pyodbc

On Macs, you should probably install unixODBC first if you don't already have an ODBC
driver manager installed, e.g. using `Homebrew`:

    brew install unixodbc
    pip install pyodbc

Similarly, on Unix you should make sure you have an ODBC driver manager installed before
installing pyodbc.  See the [docs](https://github.com/mkleehammer/pyodbc/wiki/Install)
for more information about how to do this on different Unix flavors.  (On Windows, the
ODBC driver manager is built-in.)

Precompiled binary wheels are provided for multiple Python versions on most Windows, macOS,
and Linux platforms.  On other platforms pyodbc will be built from the source code.  Note,
pyodbc contains C++ extensions so when building from source you will need a suitable C++
compiler.  See the [docs](https://github.com/mkleehammer/pyodbc/wiki/Install) for details.

[Documentation](https://github.com/mkleehammer/pyodbc/wiki)

[Release Notes](https://github.com/mkleehammer/pyodbc/releases)

IMPORTANT: Python 2.7 support is being ended.  The pyodbc 4.x versions will be the last to
support Python 2.7.  The pyodbc 5.x versions will support only Python 3.7 and above.
