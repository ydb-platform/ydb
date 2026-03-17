from __future__ import unicode_literals

import numbers
import six

from flex._compat import Sequence, Mapping


SCHEMES = (
    'http', 'https', 'ws', 'wss',
)


MIMETYPES = (
    'application/json',
)


NULL = 'null'
BOOLEAN = 'boolean'
INTEGER = 'integer'
NUMBER = 'number'
STRING = 'string'
ARRAY = 'array'
OBJECT = 'object'
FILE = 'file'

PRIMITIVE_TYPES = {
    '': (type(None),),
    None: (type(None),),
    NULL: (type(None),),
    BOOLEAN: (bool,),
    INTEGER: six.integer_types,
    NUMBER: (numbers.Number,),
    STRING: (six.binary_type, six.text_type),
    ARRAY: (Sequence,),
    OBJECT: (Mapping,),
}

TRUE_VALUES = set(('true', 'True', '1'))
FALSE_VALUES = set(('false', 'False', '0', ''))

HEADER_TYPES = (
    STRING,
    INTEGER,
    NUMBER,
    BOOLEAN,
    ARRAY,
)


PATH = 'path'
BODY = 'body'
QUERY = 'query'
FORM_DATA = 'formData'
HEADER = 'header'
PARAMETER_IN_VALUES = (
    QUERY,
    HEADER,
    PATH,
    FORM_DATA,
    BODY,
)


CSV = 'csv'
MULTI = 'multi'
SSV = 'ssv'
TSV = 'tsv'
PIPES = 'pipes'

COLLECTION_FORMATS = (
    CSV,
    SSV,
    TSV,
    PIPES,
    MULTI,
)

DELIMETERS = {
    CSV: ',',
    SSV: ' ',
    TSV: '\t',
    PIPES: '|',
}


API_KEY = 'apiKey'
BASIC = 'basic'
OAUTH_2 = 'oath2'
SECURITY_TYPES = (
    API_KEY,
    BASIC,
    OAUTH_2,
)


QUERY = QUERY
HEADER = HEADER
SECURITY_API_KEY_LOCATIONS = (
    QUERY,
    HEADER,
)


IMPLICIT = 'implicit'
PASSWORD = 'password'
APPLICATION = 'application'
ACCESS_CODE = 'accessCode'
SECURITY_FLOWS = (
    IMPLICIT,
    PASSWORD,
    APPLICATION,
    ACCESS_CODE,
)


class Empty(object):

    def __cmp__(self, other):
        raise TypeError('Empty cannot be compared to other values')


"""
Sentinal empty value for use with distinguishing `None` from a key not
being present.
"""
EMPTY = Empty()

UUID = 'uuid'
DATE = 'date'
DATETIME = 'date-time'
EMAIL = 'email'
INT32 = 'int32'
INT64 = 'int64'
URI = 'uri'


FORMATS = (
    ('integer', 'int32'),
    ('integer', 'int64'),
    ('number', 'float'),
    ('number', 'double'),
    ('string', 'byte'),
    ('string', 'date'),
    ('string', 'date-time'),
    ('string', 'email'),
    ('string', 'uri'),
)


# Request Methods
GET = 'get'
PUT = 'put'
POST = 'post'
DELETE = 'delete'
OPTIONS = 'options'
HEAD = 'head'
PATCH = 'patch'

REQUEST_METHODS = (GET, PUT, POST, DELETE, OPTIONS, HEAD, PATCH)

# Environment variables
FLEX_DISABLE_X_NULLABLE = 'FLEX_DISABLE_X_NULLABLE'
