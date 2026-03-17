demjson
=======

<b>demjson</b> is a [Python language](http://python.org/) module for
encoding, decoding, and syntax-checking [JSON](http://json.org/)
data.  It works under both Python 2 and Python 3.

It comes with a <b>jsonlint</b> script which can be used to validate
your JSON documents for strict conformance to the JSON specification,
and to detect potential data portability issues.  It can also reformat
or pretty-print JSON documents; either by re-indenting or removing
unnecessary whitespace.


What's new
==========

<b>Version 2.2.4</b> fixes problem with jsonlint under Python 3 when
trying to reformat JSON (-f or -F options) and writing the output to
standard output.

<b>Version 2.2.3</b> fixes incorrect return values from the "jsonlint"
command.  Also fixes a minor problem with the included unit tests in
certain Python versions.

<b>Version 2.2.2</b> fixes installation problems with certain Python 3
versions prior to Python 3.4.  No other changes.

<b>Version 2.2.1</b> adds an enhancement for HTML safety, and a few
obscure bug fixes.

<b>Version 2.2</b> fixes compatibility with Python 2.6 and
narrow-Unicode Pythons, fixes bugs with statistics, and adds many
enhancements to the treatment of numbers and floating-point values.

<b>Version 2.0.1</b> is a re-packaging of 2.0, after discovering
problems with incorrect checksums in the PyPI distribution of 2.0.  No
changes were made from 2.0.

<b>Version 2.0</b>, released 2014-05-21, is a MAJOR new version with many
changes and improvements.

Visit http://deron.meranda.us/python/demjson/ for complete details
and documentation.  Additional documentation may also be found
under the "docs/" folder of the source.

The biggest changes in 2.0 include:

  * Now works in Python 3; minimum version supported is Python 2.6
  * Much improved reporting of errors and warnings
  * Extensible with user-supplied hooks
  * Handles many additional Python data types automatically
  * Statistics

There are many more changes, as well as a small number of backwards
incompatibilities.  Where possible these incompatibilities were kept
to a minimum, however it is highly recommended that you read the
change notes thoroughly.


Example use
===========

To use demjson from within your Python programs:

```python
    >>> import demjson

    >>> demjson.encode( ['one',42,True,None] )    # From Python to JSON
    '["one",42,true,null]'

    >>> demjson.decode( '["one",42,true,null]' )  # From JSON to Python
    ['one', 42, True, None]
```

To check a JSON data file for errors or problems:

```bash
    $ jsonlint my.json

    my.json:1:8: Error: Numbers may not have extra leading zeros: '017'
       |  At line 1, column 8, offset 8
    my.json:4:10: Warning: Object contains same key more than once: 'Name'
       |  At line 4, column 10, offset 49
       |  Object started at line 1, column 0, offset 0 (AT-START)
    my.json:9:11: Warning: Integers larger than 53-bits are not portable
       |  At line 9, column 11, offset 142
    my.json: has errors
```


Why use demjson?
================

I wrote demjson before Python had any JSON support in its standard
library.  If all you need is to be able to read or write JSON data,
then you may wish to just use what's built into Python.

However demjson is extremely feature rich and is quite useful in
certain applications.  It is especially good at error checking
JSON data and for being able to parse more of the JavaScript syntax
than is permitted by strict JSON.

A few advantages of demjson are:

 * It works in old Python versions that don't have JSON built in;

 * It generally has better error handling and "lint" checking capabilities;

 * It will automatically use the Python Decimal (bigfloat) class
   instead of a floating-point number whenever there might be an
   overflow or loss of precision otherwise.

 * It can correctly deal with different Unicode encodings, including ASCII.
   It will automatically adapt when to use \u-escapes based on the encoding.

 * It generates more conservative JSON, such as escaping Unicode
   format control characters or line terminators, which should improve
   data portability.

 * In non-strict mode it can also deal with slightly non-conforming
   input that is more JavaScript than JSON (such as allowing comments).

 * It supports a broader set of Python types during conversion.


Installation
============

To install, type:

```bash
   python setup.py install
```

or optionally just copy the file "demjson.py" to whereever you want.
See "docs/INSTALL.txt" for more detailed instructions, including how
to run the self-tests.


More information
================

See the files under the "docs" subdirectory.  The module is also
self-documented, so within the python interpreter type:

```python
    import demjson
    help(demjson)
```

or from a shell command line:

```bash
    pydoc demjson
```

The "jsonlint" command script which gets installed as part of demjson
has built-in usage instructions as well.  Just type:

```bash
   jsonlint --help
```

Complete documentation and additional information is also available on
the project homepage at http://deron.meranda.us/python/demjson/

It is also available on the Python Package Index (PyPI) at
http://pypi.python.org/pypi/demjson/


License
=======

LGPLv3 - See the included "LICENSE.txt" file.

This software is Free Software and is licensed under the terms of the
GNU LGPL (GNU Lesser General Public License).  More information is
found at the top of the demjson.py source file and the included
LICENSE.txt file.

Releases prior to 1.4 were released under a different license, be
sure to check the corresponding LICENSE.txt file included with them.

This software was written by Deron Meranda, http://deron.meranda.us/
