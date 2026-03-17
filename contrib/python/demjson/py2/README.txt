demjson
=======

MORE DOCUMENTATION IS IN THE "docs" SUBDIRECTORY.

'demjson' is a Python language module for encoding, decoding, and
syntax-checking JSON data. It works under both Python 2 and Python 3.

It comes with a "jsonlint" script which can be used to validate your
JSON documents for strict conformance to the JSON specification.  It
can also reformat or pretty-print JSON documents; either by
re-indenting or removing unnecessary whitespace for minimal/canonical
JSON output.

demjson tries to be as closely conforming to the JSON specification,
published as IETF RFC 7159 <http://tools.ietf.org/html/rfc7159>, as
possible.  It can also be used in a non-strict mode where it is much
closer to the JavaScript/ECMAScript syntax (published as ECMA 262).
The demjson module has full Unicode support and can deal with very
large numbers.


Example use
===========

To use demjson from within your Python programs:

    import demjson

    # Convert a Python value into JSON
    demjson.encode( {'Happy': True, 'Other': None} )
          # returns string =>  {"Happy":true,"Other":null}

    # Convert a JSON document into a Python value
    demjson.decode( '{"Happy":true,"Other":null}' )
          # returns dict => {'Other': None, 'Happy': True}

To use the accompaning "jsonlint" command script:

    # To check whether a file contains valid JSON
    jsonlint sample.json

    # To pretty-print (reformat) a JSON file
    jsonlint --format sample.json


Why use demjson rather than the Python standard library?
========================================================

demjson was written before Python had any built-in JSON support in its
standard library, and there were just a handful of third-party
libraries.  None of those at that time were completely compliant with
the RFC, and the best of those required compiled C extensions rather
than being pure Python code.

So I wrote demjson to be:

 * Pure Python, requiring no compiled extension.
 * 100% RFC compliant. It should follow the JSON specification exactly.

It should be noted that Python has since added JSON into its standard
library -- which was actually an absorption of "simplejson", written
by Bob Ippolito, and which had by then been fixed to remove bugs and
improve RFC conformance.

For most uses, the standard Python JSON library should be sufficient.

However demjson may still be useful:

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

 * It supports a broader set of types during conversion, including
   Python's Decimal or UserString.


Installation
============
To install, type:

   python setup.py install

or optionally just copy the file "demjson.py" to whereever you want.
See docs/INSTALL.txt for more detailed instructions, including how to
run the self-tests.


More information
================

See the files under the "docs" subdirectory. The module is
self-documented, so within the python interpreter type:

   import demjson
   help(demjson)

or from a command line:

   pydoc demjson


The "jsonlint" command script which gets installed as part of demjson
has built-in usage instructions as well.  Just type:

   jsonlint --help

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
