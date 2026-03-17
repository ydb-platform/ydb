"""
The :mod:`dbus_fast.annotations` module contains :class:`DBusSignature` that
can be used with :class:`typing.Annotated` to provide D-Bus signature annotations
in a manner that is compatible with type hints. This applies to methods decorated
with :meth:`dbus_property <dbus_fast.service.dbus_property>`,
:meth:`dbus_method <dbus_fast.service.dbus_method>` or :meth:`dbus_signal
<dbus_fast.service.dbus_signal>`.

The module also includes some type aliases for common D-Bus types. You can
construct your own annotated types like this::

    from typing import Annotated

    MyDBusStruct = Annotated[tuple[int, str], DBusSignature("(is)")]

Or you can use ``Annotated[...]`` directly without creating an alias.

.. versionadded:: v4.0.0
    Prior to this version, annotations had to be specified as D-Bus signature
    strings.
"""

from dataclasses import dataclass
from typing import Annotated

from dbus_fast.signature import Variant

__all__ = [
    "DBusBool",
    "DBusByte",
    "DBusBytes",
    "DBusDict",
    "DBusDouble",
    "DBusInt16",
    "DBusInt32",
    "DBusInt64",
    "DBusObjectPath",
    "DBusSignature",
    "DBusSignatureType",
    "DBusStr",
    "DBusUInt16",
    "DBusUInt32",
    "DBusUInt64",
    "DBusUnixFd",
    "DBusVariant",
]


@dataclass(frozen=True, slots=True)
class DBusSignature:
    """
    A D-Bus signature annotation.

    This class is used to create D-Bus type annotations. For example::

        from typing import Annotated
        from dbus_fast.annotations import DBusSignature

        MyDBusStruct = Annotated[tuple[int, str], DBusSignature("(is)")]
    """

    signature: str


DBusBool = Annotated[bool, DBusSignature("b")]
"""
D-Bus BOOLEAN type ("b").
"""
DBusByte = Annotated[int, DBusSignature("y")]
"""
D-Bus BYTE type ("y").
"""
DBusInt16 = Annotated[int, DBusSignature("n")]
"""
D-Bus INT16 type ("n").
"""
DBusUInt16 = Annotated[int, DBusSignature("q")]
"""
D-Bus UINT16 type ("q").
"""
DBusInt32 = Annotated[int, DBusSignature("i")]
"""
D-Bus INT32 type ("i").
"""
DBusUInt32 = Annotated[int, DBusSignature("u")]
"""
D-Bus UINT32 type ("u").
"""
DBusInt64 = Annotated[int, DBusSignature("x")]
"""
D-Bus INT64 type ("x").
"""
DBusUInt64 = Annotated[int, DBusSignature("t")]
"""
D-Bus UINT64 type ("t").
"""
DBusDouble = Annotated[float, DBusSignature("d")]
"""
D-Bus DOUBLE type ("d").
"""
DBusStr = Annotated[str, DBusSignature("s")]
"""
D-Bus STRING type ("s").
"""
DBusObjectPath = Annotated[str, DBusSignature("o")]
"""
D-Bus OBJECT_PATH type ("o").
"""
DBusSignatureType = Annotated[str, DBusSignature("g")]
"""
D-Bus SIGNATURE type ("g").

.. note:: This one doesn't follow the established naming convention since
    "DBusSignature" is used for the annotation itself.
"""
DBusVariant = Annotated[Variant, DBusSignature("v")]
"""
D-Bus VARIANT type ("v").
"""
DBusDict = Annotated[dict[str, Variant], DBusSignature("a{sv}")]
"""
D-Bus ARRAY of DICT_ENTRY type with STRING keys and VARIANT values ("a{sv}").
"""
DBusUnixFd = Annotated[int, DBusSignature("h")]
"""
D-Bus UNIX_FD type ("h").
"""
DBusBytes = Annotated[bytes, DBusSignature("ay")]
"""
D-Bus ARRAY of BYTE type ("ay").
"""
