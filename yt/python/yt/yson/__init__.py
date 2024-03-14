"""
YSON library.

Package supports `YT YSON format <https://ytsaurus.tech/docs/en/user-guide/storage/yson>`_.

Package provides special classes for all yson types, see :mod:`yson_types <yt.yson.yson_types>` module.
Also it provides methods for serialization and deserialization yson data:
:func:`load <yt.yson.parser.load>`, :func:`loads <yt.yson.parser.loads>`,
:func:`dump <yt.yson.writer.dump>`, :func:`dumps <yt.yson.writer.dumps>`.
And finally it provides method :func:`to_yson_type <yt.yson.convert.to_yson_type>` for conversion
python objects to special yson types.

In special variable `TYPE` you can find implementation type of the library.
In equals "BINARY" if c++ bindings found and "PYTHON" otherwise.

Examples:

>>> import yt.yson as yson
>>> yson.loads("{a=10}")
{'a': 10}

>>> yson.dumps(True)
'"true"'

>>> number = yson.YsonInteger(10)
>>> number.attributes["my_attr"] = "hello"
>>> yson.dumps(number)
'<"attr"="hello">10'

>>> boolean = to_yson_type(False, attributes={"my_attr": "my_value"})

"""

from __future__ import print_function
import os

from . import writer  # noqa
from . import parser  # noqa
from . import yson_types  # noqa

TYPE = None
HAS_PARQUET = False

try:
    from yt_yson_bindings import load, loads, dump, dumps # noqa
    TYPE = "BINARY"
except ImportError as error:
    # XXX(asaitgalin): Sometimes module can't be imported because
    # it depends on missing dynamic libraries (e.g. libatomic). In this case
    # diagnostic is printed to stderr.
    message = str(error)
    if "No module named" not in message:
        import sys as _sys
        print("Warning! Failed to import YSON bindings: " + message, file=_sys.stderr)

try:
    from yt_yson_bindings import upload_parquet, dump_parquet # noqa
    HAS_PARQUET = True
except ImportError as error:
    message = str(error)
    if "No module named" not in message:
        import sys as _sys
        if os.environ.get("YT_LOG_LEVEL", "").lower() == "debug":
            print("Warning! Failed to import dump_parquet binding: " + message, file=_sys.stderr)

if TYPE is None:
    from .parser import load, loads  # noqa
    from .writer import dump, dumps  # noqa
    TYPE = "PYTHON"

from .yson_types import (  # noqa
    YsonString, YsonUnicode, YsonInt64, YsonUint64, YsonDouble,
    YsonBoolean, YsonList, YsonMap, YsonEntity, YsonType, YsonStringProxy,
    is_unicode, get_bytes, make_byte_key)

from .convert import to_yson_type, yson_to_json, json_to_yson  # noqa
from .common import YsonError  # noqa


def _loads_from_native_str(string, encoding="utf-8", **kwargs):
    import sys

    if sys.version_info[0] < 3:
        return loads(string, **kwargs)

    if isinstance(string, str):
        string = string.encode(encoding)

    return loads(string, encoding=encoding, **kwargs)


def _dumps_to_native_str(obj, encoding="utf-8", **kwargs):
    import sys

    if sys.version_info[0] < 3:
        return dumps(obj, **kwargs)

    s = dumps(obj, encoding=encoding, **kwargs)
    return s.decode(encoding)
