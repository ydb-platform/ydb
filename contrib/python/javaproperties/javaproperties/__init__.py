"""
Read & write Java .properties files

``javaproperties`` provides support for reading & writing Java ``.properties``
files (both the simple line-oriented format and XML) with a simple API based on
the ``json`` module — though, for recovering Java addicts, it also includes a
``Properties`` class intended to match the behavior of Java 8's
``java.util.Properties`` as much as is Pythonically possible.

Visit <https://github.com/jwodder/javaproperties> or
<http://javaproperties.rtfd.io> for more information.
"""

import codecs
from .propclass import Properties
from .propfile import PropertiesFile
from .reading import (
    Comment,
    InvalidUEscapeError,
    KeyValue,
    PropertiesElement,
    Whitespace,
    load,
    loads,
    parse,
    unescape,
)
from .writing import (
    dump,
    dumps,
    escape,
    java_timestamp,
    javapropertiesreplace_errors,
    join_key_value,
    to_comment,
)
from .xmlprops import dump_xml, dumps_xml, load_xml, loads_xml

__version__ = "0.8.2"
__author__ = "John Thorvald Wodder II"
__author_email__ = "javaproperties@varonathe.org"
__license__ = "MIT"
__url__ = "https://github.com/jwodder/javaproperties"

__all__ = [
    "Comment",
    "InvalidUEscapeError",
    "KeyValue",
    "Properties",
    "PropertiesElement",
    "PropertiesFile",
    "Whitespace",
    "dump",
    "dump_xml",
    "dumps",
    "dumps_xml",
    "escape",
    "java_timestamp",
    "javapropertiesreplace_errors",
    "join_key_value",
    "load",
    "load_xml",
    "loads",
    "loads_xml",
    "parse",
    "to_comment",
    "unescape",
]

codecs.register_error("javapropertiesreplace", javapropertiesreplace_errors)
