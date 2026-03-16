# -*- coding: utf-8 -*-
"""Descriptor to access VISA attributes.

Not all descriptors are used but they provide a reference regarding the
possible values for each attributes.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import enum
import sys
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    SupportsBytes,
    SupportsInt,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from typing_extensions import ClassVar, DefaultDict

from . import constants, util

if TYPE_CHECKING:
    from .events import Event, IOCompletionEvent  # pragma: no cover
    from .resources import Resource  # pragma: no cover

#: Not available value.
NotAvailable = object()


#: Attribute for all session types.
class AllSessionTypes:  # We use a class to simplify typing
    """Class used as a placeholder to indicate an attribute exist on all resources."""

    pass


#: Map resource to attribute
AttributesPerResource: DefaultDict[
    Union[
        Tuple[constants.InterfaceType, str], Type[AllSessionTypes], constants.EventType
    ],
    Set[Type["Attribute"]],
] = defaultdict(set)

#: Map id to attribute
AttributesByID: Dict[int, Type["Attribute"]] = {}


# --- Descriptor classes ---------------------------------------------------------------

T = TypeVar("T")


class Attribute(Generic[T]):
    """Base class for Attributes to be used as properties."""

    #: List of resource or event types with this attribute.
    #: each element is a tuple (constants.InterfaceType, str)
    resources: ClassVar[
        Union[
            List[Union[Tuple[constants.InterfaceType, str], constants.EventType]],
            Type[AllSessionTypes],
        ]
    ] = []

    #: Name of the Python property to be matched to this attribute.
    py_name: ClassVar[str] = "To be specified"

    #: Name of the VISA Attribute
    visa_name: ClassVar[str] = "To be specified"

    #: Type of the VISA attribute
    visa_type: ClassVar[str] = ""

    #: Numeric constant of the VISA Attribute
    attribute_id: ClassVar[int] = 0

    #: Default value fo the VISA Attribute
    default: ClassVar[Any] = "N/A"

    #: Access
    read: ClassVar[bool] = False
    write: ClassVar[bool] = False
    local: ClassVar[bool] = False

    @classmethod
    def __init_subclass__(cls, **kwargs):
        """Register the subclass with the supported resources."""
        super().__init_subclass__(**kwargs)

        if not cls.__name__.startswith("AttrVI_"):
            return

        cls.attribute_id = getattr(constants, cls.visa_name)
        # Check that the docstring are populated before extending them
        # Cover the case of running with Python with -OO option
        if cls.__doc__ is not None:
            cls.redoc()
        if cls.resources is AllSessionTypes:
            AttributesPerResource[AllSessionTypes].add(cls)
        else:
            for res in cls.resources:
                AttributesPerResource[res].add(cls)
        AttributesByID[cls.attribute_id] = cls

    @classmethod
    def redoc(cls) -> None:
        """Generate a descriptive docstring."""
        assert cls.__doc__ is not None
        cls.__doc__ += "\n:VISA Attribute: %s (%s)" % (cls.visa_name, cls.attribute_id)

    def post_get(self, value: Any) -> T:
        """Override to check or modify the value returned by the VISA function.

        Parameters
        ----------
        value : Any
            Value returned by the VISA library.

        Returns
        -------
        T
            Equivalent python value.

        """
        return value

    def pre_set(self, value: T) -> Any:
        """Override to check or modify the value to be passed to the VISA function.

        Parameters
        ----------
        value : T
            Python value to be passed to VISA library.

        Returns
        -------
        Any
            Equivalent value to pass to the VISA library.

        """
        return value

    @overload
    def __get__(self, instance: None, owner) -> "Attribute":
        pass

    @overload
    def __get__(self, instance: Union["Resource", "Event"], owner) -> T:
        pass

    def __get__(self, instance, owner):
        """Access a VISA attribute and convert to a nice Python representation."""
        if instance is None:
            return self

        if not self.read:
            raise AttributeError("can't read attribute")

        return self.post_get(instance.get_visa_attribute(self.attribute_id))

    def __set__(self, instance: "Resource", value: T) -> None:
        """Set the attribute if writable."""
        if not self.write:
            raise AttributeError("can't write attribute")

        # Here we use the bare integer value of the enumeration hence the type ignore
        instance.set_visa_attribute(
            self.attribute_id,  # type: ignore
            self.pre_set(value),
        )

    @classmethod
    def in_resource(cls, session_type: Tuple[constants.InterfaceType, str]) -> bool:
        """Is the attribute part of a given session type.

        Parameters
        ----------
        session_type : Tuple[constants.InterfaceType, str]
            Type of session for which to check the presence of an attribute.

        Returns
        -------
        bool
            Is the attribute present.

        """
        if cls.resources is AllSessionTypes:
            return True
        return session_type in cls.resources  # type: ignore


class EnumAttribute(Attribute):
    """Class for attributes with values that map to a PyVISA Enum."""

    #: Enum type with valid values.
    enum_type: ClassVar[Type[enum.IntEnum]]

    @classmethod
    def redoc(cls) -> None:
        """Add the enum member to the docstring."""
        assert cls.__doc__ is not None
        super(EnumAttribute, cls).redoc()
        cls.__doc__ += "\n:type: :class:%s.%s" % (
            cls.enum_type.__module__,
            cls.enum_type.__name__,
        )

    def post_get(self, value: Any) -> enum.IntEnum:
        """Convert the VISA value to the proper enum member."""
        return self.enum_type(value)

    def pre_set(self, value: enum.IntEnum) -> Any:
        """Validate the value passed against the enum."""
        # Python 3.8 raise if a non-Enum is used for value
        try:
            value = self.enum_type(value)
        except ValueError:
            raise ValueError(
                "%r is an invalid value for attribute %s, "
                "should be a %r" % (value, self.visa_name, self.enum_type)
            )
        return value


class FlagAttribute(Attribute):
    """Class for attributes with Flag values that map to a PyVISA Enum."""

    #: Enum type with valid values.
    enum_type: ClassVar[Type[enum.IntFlag]]

    @classmethod
    def redoc(cls) -> None:
        """Add the enum member to the docstring."""
        assert cls.__doc__ is not None
        super(FlagAttribute, cls).redoc()
        cls.__doc__ += "\n:type: :class:%s.%s" % (
            cls.enum_type.__module__,
            cls.enum_type.__name__,
        )

    def post_get(self, value: Any) -> enum.IntFlag:
        """Convert the VISA value to the proper enum member."""
        return self.enum_type(value)

    def pre_set(self, value: enum.IntFlag) -> Any:
        """Validate the value passed against the enum."""
        # Python 3.8 raise if a non-Enum is used for value
        try:
            value = self.enum_type(value)
        except ValueError:
            raise ValueError(
                "%r is an invalid value for attribute %s, "
                "should be a %r" % (value, self.visa_name, self.enum_type)
            )
        return value


class IntAttribute(Attribute):
    """Class for attributes with integers values."""

    @classmethod
    def redoc(cls) -> None:
        """Add the type of the returned value."""
        assert cls.__doc__ is not None
        super(IntAttribute, cls).redoc()
        cls.__doc__ += "\n:type: int"

    def post_get(self, value: SupportsInt) -> int:
        """Convert the returned value to an integer."""
        return int(value)


class RangeAttribute(IntAttribute):
    """Class for integer attributes with values within a range."""

    #: Range for the value, and iterable of extra values.
    min_value: int
    max_value: int
    values: Optional[List[int]] = None

    @classmethod
    def redoc(cls) -> None:
        """Specify the range of validity for the attribute."""
        assert cls.__doc__ is not None
        super(RangeAttribute, cls).redoc()
        cls.__doc__ += "\n:range: %s <= value <= %s" % (cls.min_value, cls.max_value)
        if cls.values:
            cls.__doc__ += " or in %s" % cls.values

    def pre_set(self, value: int) -> int:
        """Check that the value falls in the specified range."""
        if not self.min_value <= value <= self.max_value:
            if not self.values:
                raise ValueError(
                    "%r is an invalid value for attribute %s, "
                    "should be between %r and %r"
                    % (value, self.visa_name, self.min_value, self.max_value)
                )
            elif value not in self.values:
                raise ValueError(
                    "%r is an invalid value for attribute %s, "
                    "should be between %r and %r or %r"
                    % (
                        value,
                        self.visa_name,
                        self.min_value,
                        self.max_value,
                        self.values,
                    )
                )
        return value


class ValuesAttribute(Attribute):
    """Class for attributes with values in a list."""

    #: Valid values
    values: list = []

    @classmethod
    def redoc(cls) -> None:
        """Add the allowed values to the docs."""
        assert cls.__doc__ is not None
        super(ValuesAttribute, cls).redoc()
        cls.__doc__ += "\n:values: %s" % cls.values

    def pre_set(self, value: int) -> int:
        """Validate the value against the allowed values."""
        if value not in self.values:
            raise ValueError(
                "%r is an invalid value for attribute %s, "
                "should be in %s" % (value, self.visa_name, self.values)
            )
        return value


class BooleanAttribute(Attribute):
    """Class for attributes with boolean values."""

    py_type: bool

    @classmethod
    def redoc(cls) -> None:
        """Add the type to the docs."""
        assert cls.__doc__ is not None
        super(BooleanAttribute, cls).redoc()
        cls.__doc__ += "\n:type: bool"

    def post_get(self, value: constants.VisaBoolean) -> bool:
        """Convert to a Python boolean."""
        return value == constants.VisaBoolean.true

    def pre_set(self, value: bool) -> constants.VisaBoolean:
        """Convert to a VISA boolean."""
        return constants.VisaBoolean.true if value else constants.VisaBoolean.false


class CharAttribute(Attribute):
    """Class for attributes with char values."""

    py_type = str

    @classmethod
    def redoc(cls) -> None:
        """Specify the valid characters."""
        assert cls.__doc__ is not None
        super(CharAttribute, cls).redoc()
        cls.__doc__ += "\nASCII Character\n:type: str | bytes"

    def post_get(self, value: int) -> str:
        """Convert the integer to a character."""
        return chr(value)

    def pre_set(self, value: Union[str, bytes]) -> int:
        """Convert a character to an integer."""
        return ord(value)


# --- Session attributes ---------------------------------------------------------------
# Attributes are in the same order as in the constants.ResourceAttribute enum


class AttrVI_ATTR_RM_SESSION(Attribute):
    """Specifies the session of the Resource Manager used to open this session."""

    resources = AllSessionTypes

    py_name = ""

    visa_name = "VI_ATTR_RM_SESSION"

    visa_type = "ViSession"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_INTF_TYPE(EnumAttribute):
    """Interface type of the given session."""

    resources = AllSessionTypes

    py_name = "interface_type"

    visa_name = "VI_ATTR_INTF_TYPE"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.InterfaceType


class AttrVI_ATTR_INTF_NUM(RangeAttribute):
    """Board number for the given interface."""

    resources = AllSessionTypes

    py_name = "interface_number"

    visa_name = "VI_ATTR_INTF_NUM"

    visa_type = "ViUInt16"

    default = 0

    read, write, local = True, False, False

    min_value, max_value, values = 0x0, 0xFFFF, None


class AttrVI_ATTR_INTF_INST_NAME(Attribute):
    """Human-readable text that describes the given interface."""

    resources = AllSessionTypes

    py_name = ""

    visa_name = "VI_ATTR_INTF_INST_NAME"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_RSRC_CLASS(Attribute):
    """Resource class as defined by the canonical resource name.

    Possible values are: INSTR, INTFC, SOCKET, RAW...

    """

    resources = AllSessionTypes

    py_name = "resource_class"

    visa_name = "VI_ATTR_RSRC_CLASS"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_RSRC_NAME(Attribute):
    """Unique identifier for a resource compliant with the address structure."""

    resources = AllSessionTypes

    py_name = "resource_name"

    visa_name = "VI_ATTR_RSRC_NAME"

    visa_type = "ViRsrc"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_RSRC_IMPL_VERSION(RangeAttribute):
    """Resource version that identifies the revisions or implementations of a resource.

    This attribute value is defined by the individual manufacturer and increments
    with each new revision. The format of the value has the upper 12 bits as
    the major number of the version, the next lower 12 bits as the minor number
    of the version, and the lowest 8 bits as the sub-minor number of the version.

    """

    resources = AllSessionTypes

    py_name = "implementation_version"

    visa_name = "VI_ATTR_RSRC_IMPL_VERSION"

    visa_type = "ViVersion"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = 0, 4294967295, None


class AttrVI_ATTR_RSRC_LOCK_STATE(EnumAttribute):
    """Current locking state of the resource.

    The resource can be unlocked, locked with an exclusive lock,
    or locked with a shared lock.

    """

    resources = AllSessionTypes

    py_name = "lock_state"

    visa_name = "VI_ATTR_RSRC_LOCK_STATE"

    visa_type = "ViAccessMode"

    default = constants.AccessModes.no_lock

    read, write, local = True, False, False

    enum_type = constants.AccessModes


class AttrVI_ATTR_RSRC_SPEC_VERSION(RangeAttribute):
    """Version of the VISA specification to which the implementation is compliant.

    The format of the value has the upper 12 bits as the major number of the version,
    the next lower 12 bits as the minor number of the version, and the lowest 8 bits
    as the sub-minor number of the version. The current VISA specification defines
    the value to be 00300000h.

    """

    resources = AllSessionTypes

    py_name = "spec_version"

    visa_name = "VI_ATTR_RSRC_SPEC_VERSION"

    visa_type = "ViVersion"

    default = 0x00300000

    read, write, local = True, False, True

    min_value, max_value, values = 0, 4294967295, None


class AttrVI_ATTR_RSRC_MANF_NAME(Attribute):
    """Manufacturer name of the vendor that implemented the VISA library.

    This attribute is not related to the device manufacturer attributes.

    Note  The value of this attribute is for display purposes only and not for
    programmatic decisions, as the value can differ between VISA implementations
    and/or revisions.

    """

    resources = AllSessionTypes

    py_name = "resource_manufacturer_name"

    visa_name = "VI_ATTR_RSRC_MANF_NAME"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_RSRC_MANF_ID(RangeAttribute):
    """VXI manufacturer ID of the vendor that implemented the VISA library.

    This attribute is not related to the device manufacturer attributes.

    """

    resources = AllSessionTypes

    py_name = ""

    visa_name = "VI_ATTR_RSRC_MANF_ID"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 0x3FFF, None


class AttrVI_ATTR_TMO_VALUE(RangeAttribute):
    """Timeout in milliseconds for all resource I/O operations.

    This value is used when accessing the device associated with the given
    session.

    Special values:

    - **immediate** (``VI_TMO_IMMEDIATE``): 0
        (for convenience, any value smaller than 1 is considered as 0)
    - **infinite** (``VI_TMO_INFINITE``): ``float('+inf')``
        (for convenience, None is considered as ``float('+inf')``)

    To set an **infinite** timeout, you can also use:

    >>> del instrument.timeout

    A timeout value of VI_TMO_IMMEDIATE means that operations should never
    wait for the device to respond. A timeout value of VI_TMO_INFINITE disables
    the timeout mechanism.

    """

    resources = AllSessionTypes

    py_name = "timeout"

    visa_name = "VI_ATTR_TMO_VALUE"

    visa_type = "ViUInt32"

    default = 2000

    read, write, local = True, True, True

    min_value, max_value, values = 0, 0xFFFFFFFF, None

    def pre_set(self, value: Optional[Union[int, float]]) -> int:
        """Convert the timeout to an integer recognized by the VISA library."""
        timeout = util.cleanup_timeout(value)
        return timeout

    def post_get(self, value: int) -> Union[int, float]:  # type: ignore
        """Convert VI_TMO_INFINTE into float("+inf")."""
        if value == constants.VI_TMO_INFINITE:
            return float("+inf")
        return value

    def __delete__(self, instance: "Resource") -> None:
        """Set an infinite timeout upon deletion."""
        instance.set_visa_attribute(
            constants.ResourceAttribute.timeout_value, constants.VI_TMO_INFINITE
        )


class AttrVI_ATTR_MAX_QUEUE_LENGTH(RangeAttribute):
    """Maximum number of events that can be queued at any time on the given session.

    Events that occur after the queue has become full will be discarded.

    """

    resources = AllSessionTypes

    py_name = ""

    visa_name = "VI_ATTR_MAX_QUEUE_LENGTH"

    visa_type = "ViUInt32"

    default = 50

    read, write, local = True, True, True

    min_value, max_value, values = 0x1, 0xFFFFFFFF, None


class AttrVI_ATTR_USER_DATA(RangeAttribute):
    """Maximum number of events that can be queued at any time on the given session.

    Events that occur after the queue has become full will be discarded.

    """

    resources = AllSessionTypes

    py_name = ""

    visa_name = "VI_ATTR_USER_DATA"

    visa_type = "ViUInt64" if constants.is_64bits else "ViUInt32"

    default = 0

    read, write, local = True, True, True

    min_value, max_value, values = (
        0x0,
        0xFFFFFFFFFFFFFFFF if constants.is_64bits else 0xFFFFFFFF,
        None,
    )


class AttrVI_ATTR_TRIG_ID(EnumAttribute):
    """Identifier for the current triggering mechanism."""

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.pxi, "BACKPLANE"),
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.vxi, "BACKPLANE"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_TRIG_ID"

    visa_type = "ViInt16"

    default = constants.VI_TRIG_SW

    read, write, local = True, True, True

    enum_type = constants.TriggerID


class AttrVI_ATTR_SEND_END_EN(BooleanAttribute):
    """Should END be asserted during the transfer of the last byte of the buffer."""

    # TODO find out if USB RAW should be listed
    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = "send_end"

    visa_name = "VI_ATTR_SEND_END_EN"

    visa_type = "ViBoolean"

    default = True

    read, write, local = True, True, True


class AttrVI_ATTR_SUPPRESS_END_EN(BooleanAttribute):
    """Whether to suppress the END indicator termination.

    Only relevant in viRead and related operations.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_SUPPRESS_END_EN"

    visa_type = "ViBoolean"

    default = False

    read, write, local = True, True, True


class AttrVI_ATTR_TERMCHAR_EN(BooleanAttribute):
    """Should the read operation terminate when a termination character is received.

    This attribute is ignored if VI_ATTR_ASRL_END_IN is set to VI_ASRL_END_TERMCHAR.
    This attribute is valid for both raw I/O (viRead) and formatted I/O (viScanf).

    For message based resource this automatically handled by the `read_termination`
    property.

    """

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_TERMCHAR_EN"

    visa_type = "ViBoolean"

    default = False

    read, write, local = True, True, True


class AttrVI_ATTR_TERMCHAR(CharAttribute):
    """VI_ATTR_TERMCHAR is the termination character.

    When the termination character is read and VI_ATTR_TERMCHAR_EN is enabled
    during a read operation, the read operation terminates.

    For message based resource this automatically handled by the `read_termination`
    property.

    """

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_TERMCHAR"

    visa_type = "ViUInt8"

    default = 0x0A  # (linefeed)

    read, write, local = True, True, True


class AttrVI_ATTR_IO_PROT(EnumAttribute):
    """IO protocol to use.

    In VXI, you can choose normal word serial or fast data channel (FDC).
    In GPIB, you can choose normal or high-speed (HS-488) transfers.
    In serial, TCPIP, or USB RAW, you can choose normal transfers or
    488.2-defined strings.
    In USB INSTR, you can choose normal or vendor-specific transfers.

    """

    # Crossing IVI and NI this is correct
    resources = [
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = "io_protocol"

    visa_name = "VI_ATTR_IO_PROT"

    visa_type = "ViUInt16"

    default = constants.IOProtocol.normal

    read, write, local = True, True, True

    enum_type = constants.IOProtocol


class AttrVI_ATTR_FILE_APPEND_EN(BooleanAttribute):
    """Should viReadToFile() overwrite (truncate) or append when opening a file."""

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_FILE_APPEND_EN"

    visa_type = "ViBoolean"

    default = False

    read, write, local = True, True, True


class AttrVI_ATTR_RD_BUF_OPER_MODE(RangeAttribute):
    """Operational mode of the formatted I/O read buffer.

    When the operational mode is set to VI_FLUSH_DISABLE (default), the buffer
    is flushed only on explicit calls to viFlush(). If the operational mode is
    set to VI_FLUSH_ON_ACCESS, the read buffer is flushed every time a
    viScanf() (or related) operation completes.

    """

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_RD_BUF_OPER_MODE"

    visa_type = "ViUInt16"

    default = constants.VI_FLUSH_DISABLE

    read, write, local = True, True, True

    min_value, max_value, values = 0, 65535, None


class AttrVI_ATTR_RD_BUF_SIZE(RangeAttribute):
    """Current size of the formatted I/O input buffer for this session.

    The user can modify this value by calling viSetBuf().

    """

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_RD_BUF_SIZE"

    visa_type = "ViUInt32"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = 0, 4294967295, None


class AttrVI_ATTR_WR_BUF_OPER_MODE(RangeAttribute):
    """Operational mode of the formatted I/O write buffer.

    When the operational mode is set to VI_FLUSH_WHEN_FULL (default), the buffer
    is flushed when an END indicator is written to the buffer, or when the buffer
     fills up.
    If the operational mode is set to VI_FLUSH_ON_ACCESS, the write
    buffer is flushed under the same conditions, and also every time a
    viPrintf() (or related) operation completes.

    """

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_WR_BUF_OPER_MODE"

    visa_type = "ViUInt16"

    default = constants.VI_FLUSH_WHEN_FULL

    read, write, local = True, True, True

    min_value, max_value, values = 0, 65535, None


class AttrVI_ATTR_WR_BUF_SIZE(RangeAttribute):
    """Current size of the formatted I/O output buffer for this session.

    The user can modify this value by calling viSetBuf().

    """

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_WR_BUF_SIZE"

    visa_type = "ViUInt32"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = 0, 4294967295, None


class AttrVI_ATTR_DMA_ALLOW_EN(BooleanAttribute):
    """Should I/O accesses use DMA (True) or Programmed I/O (False).

    In some implementations, this attribute may have global effects even though
    it is documented to be a local attribute. Since this affects performance and not
    functionality, that behavior is acceptable.

    """

    # TODO find a reliable source to check if USB RAW should be listed
    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = "allow_dma"

    visa_name = "VI_ATTR_DMA_ALLOW_EN"

    visa_type = "ViBoolean"

    default = NotAvailable

    read, write, local = True, True, True


class AttrVI_ATTR_TCPIP_ADDR(Attribute):
    """TCPIP address of the device to which the session is connected.

    This string is formatted in dot notation.

    """

    resources = [
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_TCPIP_ADDR"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_TCPIP_HOSTNAME(Attribute):
    """Host name of the device.

    If no host name is available, this attribute returns an empty string.

    """

    resources = [
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_TCPIP_HOSTNAME"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_TCPIP_PORT(RangeAttribute):
    """Port number for a given TCPIP address.

    For a TCPIP SOCKET Resource, this is a required part of the address string.

    """

    resources = [
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_TCPIP_PORT"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 0xFFFF, None


class AttrVI_ATTR_TCPIP_DEVICE_NAME(Attribute):
    """LAN device name used by the VXI-11 or LXI protocol during connection."""

    resources = [(constants.InterfaceType.tcpip, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_TCPIP_DEVICE_NAME"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_TCPIP_NODELAY(BooleanAttribute):
    """The Nagle algorithm is disabled when this attribute is enabled (and vice versa).

    The Nagle algorithm improves network performance by buffering "send" data until a
    full-size packet can be sent. This attribute is enabled by default in VISA to
    verify that synchronous writes get flushed immediately.

    """

    resources = [
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_TCPIP_NODELAY"

    visa_type = "ViBoolean"

    default = True

    read, write, local = True, True, True


class AttrVI_ATTR_TCPIP_KEEPALIVE(BooleanAttribute):
    """Requests that a TCP/IP provider enable the use of keep-alive packets.

    After the system detects that a connection was dropped, VISA returns a lost
    connection error code on subsequent I/O calls on the session. The time required
    for the system to detect that the connection was dropped is dependent on the
    system and is not settable.

    """

    resources = [
        (constants.InterfaceType.prlgx_tcpip, "INTFC"),
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.vicp, "INSTR"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_TCPIP_KEEPALIVE"

    visa_type = "ViBoolean"

    default = False

    read, write, local = True, True, True


class AttrVI_ATTR_TCPIP_IS_HISLIP(BooleanAttribute):
    """Does this resource use the HiSLIP protocol."""

    resources = [(constants.InterfaceType.tcpip, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_TCPIP_IS_HISLIP"

    visa_type = "ViBoolean"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_TCPIP_HISLIP_VERSION(RangeAttribute):
    """HiSLIP protocol version used for a particular HiSLIP connetion.

    Currently, HiSLIP version 1.0 would return a ViVersion value of 0x00100000.

    """

    resources = [(constants.InterfaceType.tcpip, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_TCPIP_HISLIP_VERSION"

    visa_type = "ViVersion"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = 0, 0xFFFFFFFF, None


class AttrVI_ATTR_TCPIP_HISLIP_OVERLAP_EN(BooleanAttribute):
    """Enables HiSLIP 'Overlap' mode.

    The value defaults to the mode suggested by the instrument on HiSLIP connection.
    If disabled, the connection uses 'Synchronous' mode to detect and recover
    from interrupted errors. If enabled, the connection uses 'Overlapped' mode
    to allow overlapped responses. If changed, VISA will do a Device Clear
    operation to change the mode.

    """

    resources = [(constants.InterfaceType.tcpip, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_TCPIP_HISLIP_OVERLAP_EN"

    visa_type = "ViBoolean"

    default = NotAvailable

    read, write, local = True, True, True


class AttrVI_ATTR_TCPIP_HISLIP_MAX_MESSAGE_KB(RangeAttribute):
    """Maximum HiSLIP message size in kilobytes VISA will accept from a HiSLIP system.

    Defaults to 1024 (a 1 MB maximum message size).

    """

    resources = [(constants.InterfaceType.tcpip, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_TCPIP_HISLIP_MAX_MESSAGE_KB"

    visa_type = "ViUInt32"

    default = 1024

    read, write, local = True, True, True

    min_value, max_value, values = 0, 0xFFFFFFFF, None


class AttrVI_ATTR_GPIB_PRIMARY_ADDR(RangeAttribute):
    """Primary address of the GPIB device used by the given session.

    For the GPIB INTFC Resource, this attribute is Read-Write.

    """

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
    ]

    py_name = "primary_address"

    visa_name = "VI_ATTR_GPIB_PRIMARY_ADDR"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, True, False

    min_value, max_value, values = 0, 30, None


class AttrVI_ATTR_GPIB_SECONDARY_ADDR(RangeAttribute):
    """Secondary address of the GPIB device used by the given session.

    For the GPIB INTFC Resource, this attribute is Read-Write.

    """

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
    ]

    py_name = "secondary_address"

    visa_name = "VI_ATTR_GPIB_SECONDARY_ADDR"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, True, False

    min_value, max_value, values = 0, 30, [constants.VI_NO_SEC_ADDR]


class AttrVI_ATTR_GPIB_SYS_CNTRL_STATE(BooleanAttribute):
    """Is the specified GPIB interface currently the system controller.

    In some implementations, this attribute may be modified only through a
    configuration utility. On these systems this attribute is read-only (RO).

    """

    resources = [(constants.InterfaceType.gpib, "INTFC")]

    py_name = "is_system_controller"

    visa_name = "VI_ATTR_GPIB_SYS_CNTRL_STATE"

    visa_type = "ViBoolean"

    default = NotAvailable

    read, write, local = True, True, False


class AttrVI_ATTR_GPIB_CIC_STATE(BooleanAttribute):
    """Is the specified GPIB interface currently CIC (Controller In Charge)."""

    resources = [(constants.InterfaceType.gpib, "INTFC")]

    py_name = "is_controller_in_charge"

    visa_name = "VI_ATTR_GPIB_CIC_STATE"

    visa_type = "ViBoolean"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_GPIB_REN_STATE(EnumAttribute):
    """Current state of the GPIB REN (Remote ENable) interface line."""

    resources = [
        (constants.InterfaceType.gpib, "INSTR"),
        (constants.InterfaceType.gpib, "INTFC"),
    ]

    py_name = "remote_enabled"

    visa_name = "VI_ATTR_GPIB_REN_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.LineState


class AttrVI_ATTR_GPIB_ATN_STATE(EnumAttribute):
    """Current state of the GPIB ATN (ATtentioN) interface line."""

    resources = [(constants.InterfaceType.gpib, "INTFC")]

    py_name = "atn_state"

    visa_name = "VI_ATTR_GPIB_ATN_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.LineState


class AttrVI_ATTR_GPIB_NDAC_STATE(EnumAttribute):
    """Current state of the GPIB NDAC (Not Data ACcepted) interface line."""

    resources = [(constants.InterfaceType.gpib, "INTFC")]

    py_name = "ndac_state"

    visa_name = "VI_ATTR_GPIB_NDAC_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.LineState


class AttrVI_ATTR_GPIB_SRQ_STATE(EnumAttribute):
    """Current state of the GPIB SRQ (Service ReQuest) interface line."""

    resources = [(constants.InterfaceType.gpib, "INTFC")]

    py_name = ""

    visa_name = "VI_ATTR_GPIB_SRQ_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.LineState


class AttrVI_ATTR_GPIB_ADDR_STATE(EnumAttribute):
    """Current state of the GPIB interface.

    The interface can be addressed to talk or listen, or not addressed.

    """

    resources = [(constants.InterfaceType.gpib, "INTFC")]

    py_name = "address_state"

    visa_name = "VI_ATTR_GPIB_ADDR_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.AddressState


class AttrVI_ATTR_GPIB_UNADDR_EN(BooleanAttribute):
    """Whether to unaddress the device (UNT and UNL) after each read/write operation."""

    resources = [(constants.InterfaceType.gpib, "INSTR")]

    py_name = "enable_unaddressing"

    visa_name = "VI_ATTR_GPIB_UNADDR_EN"

    visa_type = "ViBoolean"

    default = False

    read, write, local = True, True, True


class AttrVI_ATTR_GPIB_READDR_EN(BooleanAttribute):
    """Whether to use repeat addressing before each read/write operation."""

    resources = [(constants.InterfaceType.gpib, "INSTR")]

    py_name = "enable_repeat_addressing"

    visa_name = "VI_ATTR_GPIB_READDR_EN"

    visa_type = "ViBoolean"

    default = True

    read, write, local = True, True, True


class AttrVI_ATTR_GPIB_HS488_CBL_LEN(RangeAttribute):
    """Total number of meters of GPIB cable used in the specified GPIB interface."""

    resources = [(constants.InterfaceType.gpib, "INTFC")]

    py_name = ""

    visa_name = "VI_ATTR_GPIB_HS488_CBL_LEN"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, False

    min_value, max_value, values = (
        1,
        15,
        [constants.VI_GPIB_HS488_DISABLED, constants.VI_GPIB_HS488_NIMPL],
    )


class AttrVI_ATTR_ASRL_AVAIL_NUM(RangeAttribute):
    """Number of bytes available in the low-level I/O receive buffer."""

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "bytes_in_buffer"

    visa_name = "VI_ATTR_ASRL_AVAIL_NUM"

    visa_type = "ViUInt32"

    default = 0

    read, write, local = True, False, False

    min_value, max_value, values = 0, 0xFFFFFFFF, None


class AttrVI_ATTR_ASRL_BAUD(RangeAttribute):
    """Baud rate of the interface.

    It is represented as an unsigned 32-bit integer so that any baud rate can
    be used, but it usually requires a commonly used rate such as 300, 1200,
    2400, or 9600 baud.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "baud_rate"

    visa_name = "VI_ATTR_ASRL_BAUD"

    visa_type = "ViUInt32"

    default = 9600

    read, write, local = True, True, False

    min_value, max_value, values = 0, 0xFFFFFFFF, None


class AttrVI_ATTR_ASRL_DATA_BITS(RangeAttribute):
    """Number of data bits contained in each frame (from 5 to 8).

    The data bits for each frame are located in the low-order bits of every
    byte stored in memory.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "data_bits"

    visa_name = "VI_ATTR_ASRL_DATA_BITS"

    visa_type = "ViUInt16"

    default = 8

    read, write, local = True, True, False

    min_value, max_value, values = 5, 8, None


class AttrVI_ATTR_ASRL_PARITY(EnumAttribute):
    """Parity used with every frame transmitted and received."""

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "parity"

    visa_name = "VI_ATTR_ASRL_PARITY"

    visa_type = "ViUInt16"

    default = constants.Parity.none

    read, write, local = True, True, False

    enum_type = constants.Parity


class AttrVI_ATTR_ASRL_STOP_BITS(EnumAttribute):
    """Number of stop bits used to indicate the end of a frame.

    The value VI_ASRL_STOP_ONE5 indicates one-and-one-half (1.5) stop bits.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "stop_bits"

    visa_name = "VI_ATTR_ASRL_STOP_BITS"

    visa_type = "ViUInt16"

    default = constants.StopBits.one

    read, write, local = True, True, False

    enum_type = constants.StopBits


class AttrVI_ATTR_ASRL_FLOW_CNTRL(FlagAttribute):
    """Indicate the type of flow control used by the transfer mechanism."""

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "flow_control"

    visa_name = "VI_ATTR_ASRL_FLOW_CNTRL"

    visa_type = "ViUInt16"

    default = constants.ControlFlow.none

    read, write, local = True, True, False

    enum_type = constants.ControlFlow


class AttrVI_ATTR_ASRL_DISCARD_NULL(BooleanAttribute):
    """If set to True, NUL characters are discarded.

    Otherwise, they are treated as normal data characters. For binary transfers,
    set this attribute to VI_FALSE.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "discard_null"

    visa_name = "VI_ATTR_ASRL_DISCARD_NULL"

    visa_type = "ViBoolean"

    default = False

    read, write, local = True, True, False


class AttrVI_ATTR_ASRL_CONNECTED(BooleanAttribute):
    """Whether the port is properly connected to another port or device.

    This attribute is valid only with serial drivers developed by National
    Instruments and documented to support this feature with the corresponding
    National Instruments hardware.

    """

    resources = [(constants.InterfaceType.asrl, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_ASRL_CONNECTED"

    visa_type = "ViBoolean"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_ASRL_ALLOW_TRANSMIT(BooleanAttribute):
    """Manually control transmission.

    If set to False, suspend transmission as if an XOFF character has been received.

    If set to True, it resumes transmission as if an XON character has been received.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "allow_transmit"

    visa_name = "VI_ATTR_ASRL_ALLOW_TRANSMIT"

    visa_type = "ViBoolean"

    default = True

    read, write, local = True, True, False


class AttrVI_ATTR_ASRL_END_IN(EnumAttribute):
    """Method used to terminate read operations."""

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "end_input"

    visa_name = "VI_ATTR_ASRL_END_IN"

    visa_type = "ViUInt16"

    default = constants.SerialTermination.termination_char

    read, write, local = True, True, True

    enum_type = constants.SerialTermination


class AttrVI_ATTR_ASRL_END_OUT(EnumAttribute):
    """Method used to terminate write operations."""

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "end_output"

    visa_name = "VI_ATTR_ASRL_END_OUT"

    visa_type = "ViUInt16"

    default = constants.SerialTermination.none

    read, write, local = True, True, True

    enum_type = constants.SerialTermination


class AttrVI_ATTR_ASRL_BREAK_LEN(RangeAttribute):
    """Duration (in milliseconds) of the break signal.

    The break signal is asserted when VI_ATTR_ASRL_END_OUT is set to
    constants.SerialTermination.termination_break. If you want to control the
    assertion state and length of a break signal manually, use the
    VI_ATTR_ASRL_BREAK_STATE attribute instead.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "break_length"

    visa_name = "VI_ATTR_ASRL_BREAK_LEN"

    visa_type = "ViInt16"

    default = 250

    read, write, local = True, True, True

    min_value, max_value, values = -32768, 32767, None


class AttrVI_ATTR_ASRL_BREAK_STATE(EnumAttribute):
    """Manually control the assertion state of the break signal.

    If set to constants.LineState.asserted, it suspends character transmission
    and places the transmission line in a break state until this attribute
    is reset to constants.LineState.unasserted.

    If you want VISA to send a break signal after each write operation
    automatically, use the VI_ATTR_ASRL_BREAK_LEN and VI_ATTR_ASRL_END_OUT
    attributes instead.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "break_state"

    visa_name = "VI_ATTR_ASRL_BREAK_STATE"

    visa_type = "ViInt16"

    default = constants.LineState.unasserted

    read, write, local = True, True, False

    enum_type = constants.LineState


class AttrVI_ATTR_ASRL_REPLACE_CHAR(CharAttribute):
    """Character to be used to replace incoming characters that arrive with errors.

    This refers for example to character that arrives with parity error.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "replace_char"

    visa_name = "VI_ATTR_ASRL_REPLACE_CHAR"

    visa_type = "ViUInt8"

    default = 0

    read, write, local = True, True, True


class AttrVI_ATTR_ASRL_XOFF_CHAR(CharAttribute):
    """XOFF character used for XON/XOFF flow control (both directions).

    If XON/XOFF flow control (software handshaking) is not being used, the value of
    this attribute is ignored.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "xoff_char"

    visa_name = "VI_ATTR_ASRL_XOFF_CHAR"

    visa_type = "ViUInt8"

    default = 0x13

    read, write, local = True, True, True


class AttrVI_ATTR_ASRL_XON_CHAR(CharAttribute):
    """XON character used for XON/XOFF flow control (both directions).

    If XON/XOFF flow control (software handshaking) is not being used, the value of
    this attribute is ignored.

    """

    resources = [
        (constants.InterfaceType.asrl, "INSTR"),
        (constants.InterfaceType.prlgx_asrl, "INTFC"),
    ]

    py_name = "xon_char"

    visa_name = "VI_ATTR_ASRL_XON_CHAR"

    visa_type = "ViUInt8"

    default = 0x11

    read, write, local = True, True, True


class AttrVI_ATTR_ASRL_CTS_STATE(EnumAttribute):
    """Current state of the Clear To Send (CTS) input signal."""

    resources = [(constants.InterfaceType.asrl, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_ASRL_CTS_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.LineState


class AttrVI_ATTR_ASRL_DSR_STATE(EnumAttribute):
    """Current state of the Data Set Ready (DSR) input signal."""

    resources = [(constants.InterfaceType.asrl, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_ASRL_DSR_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.LineState


class AttrVI_ATTR_ASRL_DTR_STATE(EnumAttribute):
    """Current state of the Data Terminal Ready (DTR) input signal."""

    resources = [(constants.InterfaceType.asrl, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_ASRL_DTR_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, False

    enum_type = constants.LineState


class AttrVI_ATTR_ASRL_RTS_STATE(EnumAttribute):
    """Manually assert or unassert the Request To Send (RTS) output signal."""

    resources = [(constants.InterfaceType.asrl, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_ASRL_RTS_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, False

    enum_type = constants.LineState


class AttrVI_ATTR_ASRL_WIRE_MODE(EnumAttribute):
    """Current wire/transceiver mode.

    For RS-485 hardware, this attribute is valid only with the RS-485 serial
    driver developed by National Instruments.

    For RS-232 hardware, the values RS232/DCE and RS232/AUTO are valid only
    with RS-232 serial drivers developed by National Instruments and documented
    to support this feature with the corresponding National Instruments hardware.
    When this feature is not supported, RS232/DTE is the only valid value.

    """

    resources = [(constants.InterfaceType.asrl, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_ASRL_WIRE_MODE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, False

    enum_type = constants.WireMode


class AttrVI_ATTR_ASRL_DCD_STATE(EnumAttribute):
    """Current state of the Data Carrier Detect (DCD) input signal.

    The DCD signal is often used by modems to indicate the detection of a
    carrier (remote modem) on the telephone line. The DCD signal is also known
    as Receive Line Signal Detect (RLSD).

    This attribute is Read Only except when the VI_ATTR_ASRL_WIRE_MODE attribute
    is set to VI_ASRL_WIRE_232_DCE, or VI_ASRL_WIRE_232_AUTO with the hardware
    currently in the DCE state.

    """

    resources = [(constants.InterfaceType.asrl, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_ASRL_DCD_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, False

    enum_type = constants.LineState


class AttrVI_ATTR_ASRL_RI_STATE(EnumAttribute):
    """Current state of the Ring Indicator (RI) input signal.

    The RI signal is often used by modems to indicate that the telephone line
    is ringing. This attribute is Read Only except when the VI_ATTR_ASRL_WIRE_MODE
    attribute is set to VI_ASRL_WIRE_232_DCE, or VI_ASRL_WIRE_232_AUTO with the
    hardware currently in the DCE state.

    """

    resources = [(constants.InterfaceType.asrl, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_ASRL_RI_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, False

    enum_type = constants.LineState


class AttrVI_ATTR_USB_INTFC_NUM(RangeAttribute):
    """USB interface number used by the given session."""

    resources = [
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
    ]

    py_name = "interface_number"

    visa_name = "VI_ATTR_USB_INTFC_NUM"

    visa_type = "ViInt16"

    default = 0

    read, write, local = True, False, False

    min_value, max_value, values = 0, 0xFE, None


class AttrVI_ATTR_USB_SERIAL_NUM(Attribute):
    """USB serial number of this device."""

    resources = [
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
    ]

    py_name = "serial_number"

    visa_name = "VI_ATTR_USB_SERIAL_NUM"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_USB_PROTOCOL(RangeAttribute):
    """USB protocol used by this USB interface."""

    resources = [
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
    ]

    py_name = "usb_protocol"

    visa_name = "VI_ATTR_USB_PROTOCOL"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 0xFF, None


class AttrVI_ATTR_USB_MAX_INTR_SIZE(RangeAttribute):
    """Maximum size of data that will be stored by any given USB interrupt.

    If a USB interrupt contains more data than this size, the data in excess of
    this size will be lost.

    """

    resources = [
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
    ]

    py_name = "maximum_interrupt_size"

    visa_name = "VI_ATTR_USB_MAX_INTR_SIZE"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, True, True

    min_value, max_value, values = 0, 0xFFFF, None


class AttrVI_ATTR_USB_CLASS(RangeAttribute):
    """USB class used by this USB interface."""

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_CLASS"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 0xFF, None


class AttrVI_ATTR_USB_SUBCLASS(RangeAttribute):
    """USB subclass used by this USB interface."""

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_SUBCLASS"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 0xFF, None


class AttrVI_ATTR_USB_BULK_IN_STATUS(RangeAttribute):
    """Status of the USB bulk-in pipe used by the given session is stalled or ready.

    This attribute can be set to only VI_USB_PIPE_READY.

    """

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_BULK_IN_STATUS"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, True

    min_value, max_value, values = -32768, 32767, None


class AttrVI_ATTR_USB_BULK_IN_PIPE(RangeAttribute):
    """Endpoint address of the USB bulk-in pipe used by the given session.

    An initial value of -1 signifies that this resource does not have any
    bulk-in pipes. This endpoint is used in viRead and related operations.

    """

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_BULK_IN_PIPE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, True

    min_value, max_value, values = 0x81, 0x8F, [-1]


class AttrVI_ATTR_USB_BULK_OUT_STATUS(RangeAttribute):
    """Status of the USB bulk-out or interrupt-out pipe used by the given session.

    The status can be stalled or ready.

    This attribute can be set to only VI_USB_PIPE_READY.

    """

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_BULK_OUT_STATUS"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, True

    min_value, max_value, values = -32768, 32767, None


class AttrVI_ATTR_USB_BULK_OUT_PIPE(RangeAttribute):
    """Endpoint address of the USB bulk-out or interrupt-out pipe.

    An initial value of -1 signifies that this resource does not have any
    bulk-out or interrupt-out pipes. This endpoint is used in viWrite
    and related operations.

    """

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_BULK_OUT_PIPE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, True

    min_value, max_value, values = 0x01, 0x0F, [-1]


class AttrVI_ATTR_USB_INTR_IN_PIPE(RangeAttribute):
    """Endpoint address of the USB interrupt-in pipe used by the given session.

    An initial value of -1 signifies that this resource does not have any
    interrupt-in pipes. This endpoint is used in viEnableEvent for VI_EVENT_USB_INTR.

    """

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_INTR_IN_PIPE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, True

    min_value, max_value, values = 0x81, 0x8F, [-1]


class AttrVI_ATTR_USB_ALT_SETTING(RangeAttribute):
    """USB alternate setting used by this USB interface."""

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_ALT_SETTING"

    visa_type = "ViInt16"

    default = 0

    read, write, local = True, True, False

    min_value, max_value, values = 0, 0xFF, None


class AttrVI_ATTR_USB_END_IN(EnumAttribute):
    """Method used to terminate read operations."""

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_END_IN"

    visa_type = "ViUInt16"

    default = constants.VI_USB_END_SHORT_OR_COUNT

    read, write, local = True, True, True

    enum_type = constants.USBEndInput


class AttrVI_ATTR_USB_NUM_INTFCS(RangeAttribute):
    """Number of interfaces supported by this USB device."""

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_NUM_INTFCS"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 1, 0xFF, None


class AttrVI_ATTR_USB_NUM_PIPES(RangeAttribute):
    """Number of pipes supported by this USB interface.

    This does not include the default control pipe.

    """

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_NUM_PIPES"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 30, None


class AttrVI_ATTR_USB_INTR_IN_STATUS(RangeAttribute):
    """Whether the USB interrupt-in pipe used by the given session is stalled or ready.

    This attribute can be set to only VI_USB_PIPE_READY.

    """

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_INTR_IN_STATUS"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, True

    min_value, max_value, values = -32768, 32767, None


class AttrVI_ATTR_USB_CTRL_PIPE(RangeAttribute):
    """Endpoint address of the USB control pipe used by the given session.

    A value of 0 signifies that the default control pipe will be used.
    This endpoint is used in viUsbControlIn and viUsbControlOut operations.
    Nonzero values may not be supported on all platforms.

    """

    resources = [(constants.InterfaceType.usb, "RAW")]

    py_name = ""

    visa_name = "VI_ATTR_USB_CTRL_PIPE"

    visa_type = "ViInt16"

    default = 0x00

    read, write, local = True, True, True

    min_value, max_value, values = 0x00, 0x0F, None


class AttrVI_ATTR_MANF_NAME(Attribute):
    """Manufacturer name."""

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.pxi, "BACKPLANE"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
    ]

    py_name = "manufacturer_name"

    visa_name = "VI_ATTR_MANF_NAME"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_MANF_ID(RangeAttribute):
    """Manufacturer identification number of the device."""

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
    ]

    py_name = "manufacturer_id"

    visa_name = "VI_ATTR_MANF_ID"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0x0, 0xFFFF, None


class AttrVI_ATTR_MODEL_NAME(Attribute):
    """Model name of the device."""

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.pxi, "BACKPLANE"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
    ]

    py_name = "model_name"

    visa_name = "VI_ATTR_MODEL_NAME"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_MODEL_CODE(RangeAttribute):
    """Model code for the device."""

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.usb, "RAW"),
        (constants.InterfaceType.vxi, "INSTR"),
    ]

    py_name = "model_code"

    visa_name = "VI_ATTR_MODEL_CODE"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0x0, 0xFFFF, None


class AttrVI_ATTR_DEV_STATUS_BYTE(CharAttribute):
    """488-style status byte of the local controller or device for this session."""

    resources = [
        (constants.InterfaceType.gpib, "INTFC"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_DEV_STATUS_BYTE"

    visa_type = "ViUInt8"

    default = NotAvailable

    read, write, local = True, True, False


class AttrVI_ATTR_4882_COMPLIANT(BooleanAttribute):
    """Whether the device is 488.2 compliant."""

    resources = [
        (constants.InterfaceType.usb, "INSTR"),
        (constants.InterfaceType.vxi, "INSTR"),
    ]

    py_name = "is_4882_compliant"

    visa_name = "VI_ATTR_4882_COMPLIANT"

    visa_type = "ViBoolean"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_SLOT(RangeAttribute):
    """Physical slot location of the device.

    If the slot number is not known, VI_UNKNOWN_SLOT is returned.

    For VXI resources the maximum value is 12, 18 is the maximum for PXI resources.

    """

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.vxi, "INSTR"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_SLOT"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 1, 18, [constants.VI_UNKNOWN_SLOT]


class AttrVI_ATTR_WIN_ACCESS(RangeAttribute):
    """Modes in which the current window may be accessed."""

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.pxi, "MEMACC"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_WIN_ACCESS"

    visa_type = "ViUInt16"

    default = constants.VI_NMAPPED

    read, write, local = True, False, True

    min_value, max_value, values = 0, 65535, None


class AttrVI_ATTR_WIN_BASE_ADDR(RangeAttribute):
    """Base address of the interface bus to which this window is mapped."""

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.pxi, "MEMACC"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_WIN_BASE_ADDR"

    visa_type = "ViBusAddress64" if constants.is_64bits else "ViBusAddress"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = (
        0,
        0xFFFFFFFFFFFFFFFF if constants.is_64bits else 0xFFFFFFFF,
        None,
    )


class AttrVI_ATTR_WIN_SIZE(RangeAttribute):
    """Base address of the interface bus to which this window is mapped."""

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.pxi, "MEMACC"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_WIN_SIZE"

    visa_type = "ViBusSize64" if constants.is_64bits else "ViBusSize"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = (
        0,
        0xFFFFFFFFFFFFFFFF if constants.is_64bits else 0xFFFFFFFF,
        None,
    )


class AttrVI_ATTR_SRC_INCREMENT(RangeAttribute):
    """Number of elements by which to increment the source offset after a transfer.

    The default value of this attribute is 1 (that is, the source address will be
    incremented by 1 after each transfer), and the viMoveInXX() operations move from
    consecutive elements. If this attribute is set to 0, the viMoveInXX() operations
    will always read from the same element, essentially treating the source as a
    FIFO register.

    """

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.pxi, "MEMACC"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = "source_increment"

    visa_name = "VI_ATTR_SRC_INCREMENT"

    visa_type = "ViInt32"

    default = 1

    read, write, local = True, True, True

    min_value, max_value, values = 0, 1, None


class AttrVI_ATTR_DEST_INCREMENT(RangeAttribute):
    """Number of elements by which to increment the destination offset after a transfer.

    The default value of this attribute is 1 (that is, the destination address will be
    incremented by 1 after each transfer), and the viMoveOutXX() operations move
    into consecutive elements. If this attribute is set to 0, the viMoveOutXX()
    operations will always write to the same element, essentially treating the
    destination as a FIFO register.

    """

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.pxi, "MEMACC"),
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = "destination_increment"

    visa_name = "VI_ATTR_DEST_INCREMENT"

    visa_type = "ViInt32"

    default = 1

    read, write, local = True, True, True

    min_value, max_value, values = 0, 1, None


class AttrVI_ATTR_FDC_CHNL(RangeAttribute):
    """Which Fast Data Channel (FDC) to use to transfer the buffer."""

    resources = [(constants.InterfaceType.vxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_FDC_CHNL"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, True, True

    min_value, max_value, values = 0, 7, None


class AttrVI_ATTR_FDC_MODE(RangeAttribute):
    """Which Fast Data Channel (FDC) mode to use (either normal or stream mode)."""

    resources = [(constants.InterfaceType.vxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_FDC_MODE"

    visa_type = "ViUInt16"

    default = constants.VI_FDC_NORMAL

    read, write, local = True, True, True

    min_value, max_value, values = 0, 65535, None


class AttrVI_ATTR_FDC_GEN_SIGNAL_EN(BooleanAttribute):
    """Fast Data Channel (FDC) signal enable."""

    resources = [(constants.InterfaceType.vxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_FDC_GEN_SIGNAL_EN"

    visa_type = "ViBoolean"

    default = NotAvailable

    read, write, local = True, True, True


class AttrVI_ATTR_FDC_USE_PAIR(BooleanAttribute):
    """Use a channel pair for transferring data.

    If set to False, only one channel will be used.

    """

    resources = [(constants.InterfaceType.vxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_FDC_USE_PAIR"

    visa_type = "ViBoolean"

    default = False

    read, write, local = True, True, True


class AttrVI_ATTR_MAINFRAME_LA(RangeAttribute):
    """Lowest logical address in the mainframe.

    If the logical address is not known, VI_UNKNOWN_LA is returned.

    """

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "BACKPLANE"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_MAINFRAME_LA"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 255, [constants.VI_UNKNOWN_LA]


class AttrVI_ATTR_VXI_LA(RangeAttribute):
    """Logical address of the VXI or VME device.

    For a MEMACC or SERVANT session, this attribute specifies the logical
    address of the local controller.

    """

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_VXI_LA"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 511, None


class AttrVI_ATTR_CMDR_LA(RangeAttribute):
    """Unique logical address of the commander of the VXI device."""

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_CMDR_LA"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 255, [constants.VI_UNKNOWN_LA]


class AttrVI_ATTR_MEM_SPACE(EnumAttribute):
    """VI_ATTR_MEM_SPACE specifies the VXIbus address space used by the device.

    The four types are A16, A24, A32 or A64 memory address space.

    """

    resources = [(constants.InterfaceType.vxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_MEM_SPACE"

    visa_type = "ViUInt16"

    default = constants.VI_A16_SPACE

    read, write, local = True, False, False

    enum_type = constants.AddressSpace


class AttrVI_ATTR_MEM_SIZE(RangeAttribute):
    """Unique logical address of the commander of the VXI device."""

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_MEM_SIZE"

    visa_type = "ViBusSize64" if constants.is_64bits else "ViBusSize"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = (
        0,
        0xFFFFFFFFFFFFFFFF if constants.is_64bits else 0xFFFFFFFF,
        None,
    )


class AttrVI_ATTR_MEM_BASE(RangeAttribute):
    """Unique logical address of the commander of the VXI device."""

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "SERVANT"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_MEM_BASE"

    visa_type = "ViBusAddress64" if constants.is_64bits else "ViUInt32"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = (
        0,
        0xFFFFFFFFFFFFFFFF if constants.is_64bits else 0xFFFFFFFF,
        None,
    )


class AttrVI_ATTR_IMMEDIATE_SERV(BooleanAttribute):
    """Is the device an immediate servant of the controller running VISA."""

    resources = [(constants.InterfaceType.vxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_IMMEDIATE_SERV"

    visa_type = "ViBoolean"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_DEST_ACCESS_PRIV(RangeAttribute):
    """Address modifier to be used in high-level write operations.

    High level operations are viOutXX() and viMoveOutXX().

    """

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_DEST_ACCESS_PRIV"

    visa_type = "ViUInt16"

    default = constants.VI_DATA_PRIV

    read, write, local = True, True, True

    min_value, max_value, values = 0, 65535, None


class AttrVI_ATTR_DEST_BYTE_ORDER(EnumAttribute):
    """Byte order to be used in high-level write operations.

    High level operations are viOutXX() and viMoveOutXX().

    """

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_DEST_BYTE_ORDER"

    visa_type = "ViUInt16"

    default = constants.VI_BIG_ENDIAN

    read, write, local = True, True, True

    enum_type = constants.ByteOrder


class AttrVI_ATTR_SRC_ACCESS_PRIV(EnumAttribute):
    """Address modifier to be used in high-level read operations.

    High level operations are viInXX() and viMoveinXX().

    """

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_SRC_ACCESS_PRIV"

    visa_type = "ViUInt16"

    default = constants.VI_DATA_PRIV

    read, write, local = True, True, True

    enum_type = constants.AddressModifiers


class AttrVI_ATTR_SRC_BYTE_ORDER(EnumAttribute):
    """Byte order to be used in high-level read operations.

    High level operations are viOutXX() and viMoveOutXX().

    """

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_SRC_BYTE_ORDER"

    visa_type = "ViUInt16"

    default = constants.VI_BIG_ENDIAN

    read, write, local = True, True, True

    enum_type = constants.ByteOrder


class AttrVI_ATTR_WIN_ACCESS_PRIV(EnumAttribute):
    """Address modifier to be used in low-level access operations.

    Low-level operation are viMapAddress(), viPeekXX(), and viPokeXX(),
    when accessing the mapped window.

    """

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_WIN_ACCESS_PRIV"

    visa_type = "ViUInt16"

    default = constants.VI_DATA_PRIV

    read, write, local = True, True, True

    enum_type = constants.AddressModifiers


class AttrVI_ATTR_WIN_BYTE_ORDER(EnumAttribute):
    """Byte order to be used in low- level access operations.

    Low-level operation are viMapAddress(), viPeekXX(), and viPokeXX(),
    when accessing the mapped window.

    """

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "MEMACC"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_WIN_BYTE_ORDER"

    visa_type = "ViUInt16"

    default = constants.VI_BIG_ENDIAN

    read, write, local = True, True, True

    enum_type = constants.ByteOrder


class AttrVI_ATTR_VXI_TRIG_SUPPORT(RangeAttribute):
    """VXI trigger lines this implementation supports.

    This is a bit vector. Bits 0-7 correspond to VI_TRIG_TTL0 to VI_TRIG_TTL7.
    Bits 8-13 correspond to VI_TRIG_ECL0 to VI_TRIG_ECL5. Bits 14-25 correspond
    to VI_TRIG_STAR_SLOT1 to VI_TRIG_STAR_SLOT12. Bit 27 corresponds to
    VI_TRIG_PANEL_IN and bit 28 corresponds to VI_TRIG_PANEL_OUT. Bits 29-31
    correspond to VI_TRIG_STAR_VXI0 to VI_TRIG_STAR_VXI2. VXI does not use
    VI_TRIG_TTL8 to VI_TRIG_TTL11.

    """

    resources = [
        (constants.InterfaceType.vxi, "INSTR"),
        (constants.InterfaceType.vxi, "BACKPLANE"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_VXI_TRIG_SUPPORT"

    visa_type = "ViUInt32"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 4294967295, None


class AttrVI_ATTR_INTF_PARENT_NUM(RangeAttribute):
    """This attribute shows the current state of the VXI/VME interrupt lines.
    This is a bit vector with bits 0-6 corresponding to interrupt
    lines 1-7.
    """

    resources = [(constants.InterfaceType.vxi, "BACKPLANE")]

    py_name = ""

    visa_name = "VI_ATTR_INTF_PARENT_NUM"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 65535, None


class AttrVI_ATTR_VXI_DEV_CLASS(EnumAttribute):
    """VXI-defined device class to which the resource belongs.

    This can be either:

    - message based (VI_VXI_CLASS_MESSAGE)
    - register based (VI_VXI_CLASS_REGISTER)
    - extended (VI_VXI_CLASS_EXTENDED)
    - memory (VI_VXI_CLASS_MEMORY)
    - other (VI_VXI_CLASS_OTHER)

    VME devices are usually either register based or belong to a miscellaneous
    class (VI_VXI_CLASS_OTHER).

    """

    resources = [(constants.InterfaceType.vxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_VXI_DEV_CLASS"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.VXIClass


class AttrVI_ATTR_VXI_TRIG_DIR(RangeAttribute):
    """Bit map of the directions of the mapped TTL trigger lines.

    Bits 0-7 represent TTL triggers 0-7 respectively. A bit's value of 0 means
    the line is routed out of the frame, and a value of 1 means into the frame.
    In order for a direction to be set, the line must also be enabled using
    VI_ATTR_VXI_TRIG_LINES_EN.

    """

    resources = [(constants.InterfaceType.vxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_VXI_TRIG_DIR"

    visa_type = "ViUInt16"

    default = 0

    read, write, local = True, True, False

    min_value, max_value, values = 0, 65535, None


class AttrVI_ATTR_VXI_TRIG_LINES_EN(RangeAttribute):
    """Bit map of what VXI TLL triggers have mappings.

    Bits 0-7 represent TTL triggers 0-7 respectively. A bit's value of 0 means
    the trigger line is unmapped, and 1 means a mapping exists. Use
    VI_ATTR_VXI_TRIG_DIR to set an enabled line's direction.

    """

    resources = [(constants.InterfaceType.vxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_VXI_TRIG_LINES_EN"

    visa_type = "ViUInt16"

    default = 0

    read, write, local = True, True, False

    min_value, max_value, values = 0, 65535, None


class AttrVI_ATTR_VXI_VME_INTR_STATUS(RangeAttribute):
    """Current state of the VXI/VME interrupt lines.

    This is a bit vector with bits 0-6 corresponding to interrupt lines 1-7.

    """

    resources = [(constants.InterfaceType.vxi, "BACKPLANE")]

    py_name = ""

    visa_name = "VI_ATTR_VXI_VME_INTR_STATUS"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 65535, None


class AttrVI_ATTR_VXI_TRIG_STATUS(RangeAttribute):
    """Current state of the VXI trigger lines.

    This is a bit vector with bits 0-9 corresponding to VI_TRIG_TTL0
    through VI_TRIG_ECL1.

    """

    resources = [(constants.InterfaceType.vxi, "BACKPLANE")]

    py_name = ""

    visa_name = "VI_ATTR_VXI_TRIG_STATUS"

    visa_type = "ViUInt32"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 4294967295, None


class AttrVI_ATTR_VXI_VME_SYSFAIL_STATE(EnumAttribute):
    """Current state of the VXI/VME SYSFAIL (SYStem FAILure) backplane line."""

    resources = [(constants.InterfaceType.vxi, "BACKPLANE")]

    py_name = ""

    visa_name = "VI_ATTR_VXI_VME_SYSFAIL_STATE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.LineState


class AttrVI_ATTR_PXI_DEV_NUM(RangeAttribute):
    """PXI device number."""

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_DEV_NUM"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 31, None


class AttrVI_ATTR_PXI_FUNC_NUM(RangeAttribute):
    """PCI function number of the PXI/PCI resource.

    For most devices, the function number is 0, but a multifunction device may
    have a function number up to 7. The meaning of a function number other than
    0 is device specific.

    """

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_FUNC_NUM"

    visa_type = "ViUInt16"

    default = 0

    read, write, local = True, False, False

    min_value, max_value, values = 0, 7, None


class AttrVI_ATTR_PXI_BUS_NUM(RangeAttribute):
    """PCI bus number of this device."""

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_BUS_NUM"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 255, None


class AttrVI_ATTR_PXI_CHASSIS(RangeAttribute):
    """PXI chassis number of this device.

    A value of -1 means the chassis number is unknown.

    """

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.pxi, "BACKPLANE"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_PXI_CHASSIS"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 255, [-1]


class AttrVI_ATTR_PXI_SLOTPATH(Attribute):
    """Slot path of this device."""

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_SLOTPATH"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_PXI_SLOT_LBUS_LEFT(RangeAttribute):
    """Slot number or special feature connected to the local bus left lines."""

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_SLOT_LBUS_LEFT"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = (
        1,
        18,
        [
            constants.VI_PXI_LBUS_UNKNOWN,
            constants.VI_PXI_LBUS_NONE,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_0,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_1,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_2,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_3,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_4,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_5,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_6,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_7,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_8,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_9,
            constants.VI_PXI_STAR_TRIG_CONTROLLER,
            constants.VI_PXI_LBUS_SCXI,
        ],
    )


class AttrVI_ATTR_PXI_SLOT_LBUS_RIGHT(RangeAttribute):
    """Slot number or special feature connected to the local bus right lines."""

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_SLOT_LBUS_RIGHT"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = (
        1,
        18,
        [
            constants.VI_PXI_LBUS_UNKNOWN,
            constants.VI_PXI_LBUS_NONE,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_0,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_1,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_2,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_3,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_4,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_5,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_6,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_7,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_8,
            constants.VI_PXI_LBUS_STAR_TRIG_BUS_9,
            constants.VI_PXI_STAR_TRIG_CONTROLLER,
            constants.VI_PXI_LBUS_SCXI,
        ],
    )


class AttrVI_ATTR_PXI_IS_EXPRESS(BooleanAttribute):
    """Whether the device is PXI/PCI or PXI/PCI Express."""

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_IS_EXPRESS"

    visa_type = "ViBoolean"

    default = NotAvailable

    read, write, local = True, False, False


class AttrVI_ATTR_PXI_SLOT_LWIDTH(ValuesAttribute):
    """PCI Express link width of the PXI Express peripheral slot of the device.

    A value of -1 indicates that the device is not a PXI Express device.

    """

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_SLOT_LWIDTH"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    values = [-1, 1, 4, 8]


class AttrVI_ATTR_PXI_MAX_LWIDTH(ValuesAttribute):
    """Maximum PCI Express link width of the device.

    A value of -1 indicates that the device is not a PXI/PCI Express device.

    """

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_MAX_LWIDTH"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    values = [-1, 1, 2, 4, 8, 16]


class AttrVI_ATTR_PXI_ACTUAL_LWIDTH(ValuesAttribute):
    """PCI Express link width negotiated between the host controller and the device.

    A value of -1 indicates that the device is not a PXI/PCI Express device.

    """

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_ACTUAL_LWIDTH"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    values = [-1, 1, 2, 4, 8, 16]


class AttrVI_ATTR_PXI_DSTAR_BUS(RangeAttribute):
    """Differential star bus number of this device.

    A value of -1 means the chassis is unidentified or does not have a timing slot.

    """

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_DSTAR_BUS"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = -32768, 32767, None


class AttrVI_ATTR_PXI_DSTAR_SET(RangeAttribute):
    """Set of PXI_DSTAR lines connected to this device."""

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_DSTAR_SET"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 16, [-1]


class AttrVI_ATTR_PXI_TRIG_BUS(RangeAttribute):
    """The trigger bus number of this device."""

    resources = [
        (constants.InterfaceType.pxi, "INSTR"),
        (constants.InterfaceType.pxi, "BACKPLANE"),
    ]

    py_name = ""

    visa_name = "VI_ATTR_PXI_TRIG_BUS"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, True, True

    min_value, max_value, values = 1, 3, [-1]


class AttrVI_ATTR_PXI_STAR_TRIG_BUS(RangeAttribute):
    """The star trigger bus number of this device."""

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_STAR_TRIG_BUS"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 1, 3, [-1]


class AttrVI_ATTR_PXI_STAR_TRIG_LINE(RangeAttribute):
    """The PXI_STAR line connected to this device."""

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_STAR_TRIG_LINE"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 32767, None


class AttrVI_ATTR_PXI_SRC_TRIG_BUS(RangeAttribute):
    """VThe segment to use to qualify trigSrc in viMapTrigger."""

    resources = [(constants.InterfaceType.pxi, "BACKPLANE")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_SRC_TRIG_BUS"

    visa_type = "ViInt16"

    default = -1

    read, write, local = True, True, True

    min_value, max_value, values = 1, 3, [-1]


class AttrVI_ATTR_PXI_DEST_TRIG_BUS(RangeAttribute):
    """The segment to use to qualify trigDest in viMapTrigger."""

    resources = [(constants.InterfaceType.pxi, "BACKPLANE")]

    py_name = ""

    visa_name = "VI_ATTR_PXI_DEST_TRIG_BUS"

    visa_type = "ViInt16"

    default = -1

    read, write, local = True, True, True

    min_value, max_value, values = 1, 3, [-1]


class _AttrVI_ATTR_PXI_MEM_TYPE_BARX(EnumAttribute):
    """Memory type used by the device in the specified BAR (if applicable)."""

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, False

    enum_type = constants.PXIMemory


class _AttrVI_ATTR_PXI_MEM_BASE_BARX(RangeAttribute):
    """PXI memory base address assigned to the specified BAR.

    If the value of the corresponding VI_ATTR_PXI_MEM_TYPE_BARx is
    constants.PXIMemory.none, the value of this attribute is meaningless for
    the given PXI device.

    """

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_type = "ViUInt32"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 0xFFFFFFFF, None


class _AttrVI_ATTR_PXI_MEM_SIZE_BARX(RangeAttribute):
    """Memory size used by the device in the specified BAR.

    If the value of the corresponding VI_ATTR_PXI_MEM_TYPE_BARx is
    constants.PXIMemory.none, the value of this attribute is meaningless for
    the given PXI device.

    """

    resources = [(constants.InterfaceType.pxi, "INSTR")]

    py_name = ""

    visa_type = "ViUInt32"

    default = NotAvailable

    read, write, local = True, False, False

    min_value, max_value, values = 0, 0xFFFFFFFF, None


mod = sys.modules[__name__]
for i in range(0, 6):
    setattr(
        mod,
        f"AttrVI_ATTR_PXI_MEM_TYPE_BAR{i}",
        type(
            f"AttrVI_ATTR_PXI_MEM_TYPE_BAR{i}",
            (_AttrVI_ATTR_PXI_MEM_TYPE_BARX,),
            {"visa_name": f"VI_ATTR_PXI_MEM_TYPE_BAR{i}"},
        ),
    )

    setattr(
        mod,
        f"AttrVI_ATTR_PXI_MEM_BASE_BAR{i}",
        type(
            f"AttrVI_ATTR_PXI_MEM_BASE_BAR{i}",
            (_AttrVI_ATTR_PXI_MEM_BASE_BARX,),
            {"visa_name": f"VI_ATTR_PXI_MEM_BASE_BAR{i}"},
        ),
    )

    setattr(
        mod,
        f"AttrVI_ATTR_PXI_MEM_SIZE_BAR{i}",
        type(
            f"AttrVI_ATTR_PXI_MEM_SIZE_BAR{i}",
            (_AttrVI_ATTR_PXI_MEM_SIZE_BARX,),
            {"visa_name": f"VI_ATTR_PXI_MEM_SIZE_BAR{i}"},
        ),
    )


# --- Event type attributes ------------------------------------------------------------


class AttrVI_ATTR_STATUS(EnumAttribute):
    """Status code of the operation generating this event."""

    resources = [constants.EventType.exception, constants.EventType.io_completion]

    py_name = "status"

    visa_name = "VI_ATTR_STATUS"

    visa_type = "ViStatus"

    default = NotAvailable

    read, write, local = True, False, True

    enum_type = constants.StatusCode


class AttrVI_ATTR_OPER_NAME(Attribute):
    """Name of the operation generating this event."""

    resources = [constants.EventType.io_completion, constants.EventType.exception]

    py_name = "operation_name"

    visa_name = "VI_ATTR_OPER_NAME"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, True


class AttrVI_ATTR_JOB_ID(Attribute):
    """Job ID of the asynchronous operation that has completed."""

    resources = [constants.EventType.io_completion]

    py_name = "job_id"

    visa_name = "VI_ATTR_JOB_ID"

    visa_type = "ViJobId"

    default = NotAvailable

    read, write, local = True, False, True


class AttrVI_ATTR_RET_COUNT(RangeAttribute):
    """Actual number of elements that were asynchronously transferred."""

    resources = [constants.EventType.io_completion]

    py_name = "return_count"

    visa_name = "VI_ATTR_RET_COUNT"

    visa_type = "ViUInt32"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = 0, 0xFFFFFFFF, None


class AttrVI_ATTR_BUFFER(Attribute):
    """Buffer that was used in an asynchronous operation."""

    resources = [constants.EventType.io_completion]

    py_name = "buffer"

    visa_name = "VI_ATTR_BUFFER"

    visa_type = "ViBuf"

    default = NotAvailable

    read, write, local = True, False, True

    def __get__(  # type: ignore
        self, instance: Optional["IOCompletionEvent"], owner
    ) -> Optional[Union[SupportsBytes, "AttrVI_ATTR_BUFFER"]]:
        """Retrieve the buffer stored on the library using the jod Id."""
        if instance is None:
            return self
        # The buffer we need to access has been created in an earlier call
        # starting an asynchronous read. When starting that call we stored
        # the buffer on the resource and we can now retrieve it
        return instance.visalib.get_buffer_from_id(instance.job_id)


class AttrVI_ATTR_RECV_TRIG_ID(EnumAttribute):
    """Id of the trigger that generated the event."""

    resources = [constants.EventType.trig]

    py_name = "received_trigger_id"

    visa_name = "VI_ATTR_RECV_TRIG_ID"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, True

    enum_type = constants.TriggerEventID


class AttrVI_ATTR_GPIB_RECV_CIC_STATE(BooleanAttribute):
    """Whether the event by the gain or the loss of the CIC state."""

    resources = [constants.EventType.gpib_controller_in_charge]

    py_name = "cic_state"

    visa_name = "VI_ATTR_GPIB_RECV_CIC_STATE"

    visa_type = "ViBool"

    default = NotAvailable

    read, write, local = True, False, True


class AttrVI_ATTR_RECV_TCPIP_ADDR(Attribute):
    """Address of the device from which the session received a connection."""

    resources = [constants.EventType.tcpip_connect]

    py_name = "tcpip_connect"

    visa_name = "VI_ATTR_RECV_TCPIP_ADDR"

    visa_type = "ViString"

    default = NotAvailable

    read, write, local = True, False, True


class AttrVI_ATTR_USB_RECV_INTR_SIZE(RangeAttribute):
    """Size of the data that was received from the USB interrupt-IN pipe."""

    resources = [constants.EventType.usb_interrupt]

    py_name = "size"

    visa_name = "VI_ATTR_USB_RECV_INTR_SIZE"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = 0, 0xFFFF, None


class AttrVI_ATTR_USB_RECV_INTR_DATA(Attribute):
    """Actual data that was received from the USB interrupt-IN pipe."""

    resources = [constants.EventType.pxi_interrupt]

    py_name = "data"

    visa_name = "VI_ATTR_USB_RECV_INTR_DATA"

    visa_type = "ViBuf"

    default = NotAvailable

    read, write, local = True, False, True


class AttrVI_ATTR_SIGP_STATUS_ID(RangeAttribute):
    """16-bit Status/ID retrieved during the IACK cycle or from the Signal register."""

    resources = [constants.EventType.vxi_signal_interrupt]

    py_name = "signal_register_status_id"

    visa_name = "VI_ATTR_SIGP_STATUS_ID"

    visa_type = "ViUInt16"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = 0, 0xFFFF, None


class AttrVI_ATTR_INTR_STATUS_ID(RangeAttribute):
    """32-bit status/ID retrieved during the IACK cycle."""

    resources = [constants.EventType.vxi_vme_interrupt]

    py_name = "status_id"

    visa_name = "VI_ATTR_INTR_STATUS_ID"

    visa_type = "ViUInt32"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = 0, 0xFFFFFFFF, None


class AttrVI_ATTR_RECV_INTR_LEVEL(RangeAttribute):
    """VXI interrupt level on which the interrupt was received."""

    resources = [constants.EventType.vxi_vme_interrupt]

    py_name = "level"

    visa_name = "VI_ATTR_RECV_INTR_LEVEL"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, True

    min_value, max_value, values = 1, 7, [constants.VI_UNKNOWN_LEVEL]


class AttrVI_ATTR_PXI_RECV_INTR_SEQ(Attribute):
    """Index of the interrupt sequence that detected the interrupt condition."""

    resources = [constants.EventType.pxi_interrupt]

    py_name = "sequence"

    visa_name = "VI_ATTR_PXI_RECV_INTR_SEQ"

    visa_type = "ViInt16"

    default = NotAvailable

    read, write, local = True, False, True


class AttrVI_ATTR_PXI_RECV_INTR_DATA(Attribute):
    """First PXI/PCI register read in the successful interrupt detection sequence."""

    resources = [constants.EventType.pxi_interrupt]

    py_name = "data"

    visa_name = "VI_ATTR_PXI_RECV_INTR_DATA"

    visa_type = "ViUInt32"

    default = NotAvailable

    read, write, local = True, False, True
