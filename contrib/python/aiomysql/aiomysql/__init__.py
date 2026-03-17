"""
aiomysql: A pure-Python MySQL client library for asyncio.

Copyright (c) 2010, 2013-2014 PyMySQL contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

"""

from pymysql.converters import escape_dict, escape_sequence, escape_string
from pymysql.err import (Warning, Error, InterfaceError, DataError,
                         DatabaseError, OperationalError, IntegrityError,
                         InternalError,
                         NotSupportedError, ProgrammingError, MySQLError)

from .connection import Connection, connect
from .cursors import Cursor, SSCursor, DictCursor, SSDictCursor
from .pool import create_pool, Pool
from ._version import version

__version__ = version

__all__ = [

    # Errors
    'Error',
    'DataError',
    'DatabaseError',
    'IntegrityError',
    'InterfaceError',
    'InternalError',
    'MySQLError',
    'NotSupportedError',
    'OperationalError',
    'ProgrammingError',
    'Warning',

    'escape_dict',
    'escape_sequence',
    'escape_string',

    'Connection',
    'Pool',
    'connect',
    'create_pool',
    'Cursor',
    'SSCursor',
    'DictCursor',
    'SSDictCursor'
]

(Connection, Pool, connect, create_pool, Cursor, SSCursor, DictCursor,
 SSDictCursor)  # pyflakes
