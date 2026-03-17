# -*- coding: utf-8 -*-
"""Functions and classes to parse and assemble resource name.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import contextlib
import re
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, fields
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)

from typing_extensions import ClassVar, Self

from . import constants, errors, logger

if TYPE_CHECKING:
    from .resources import Resource  # pragma: no cover

#: Interface types for which a subclass of ResourceName exists
_INTERFACE_TYPES: Set[str] = set()

#: Resource Class for Interface type
_RESOURCE_CLASSES: Dict[str, Set[str]] = defaultdict(set)

#: Subclasses of ResourceName matching an interface type, resource class pair
_SUBCLASSES: Dict[Tuple[str, str], Type["ResourceName"]] = {}

# DEFAULT Resource Class for a given interface type.
_DEFAULT_RC: Dict[str, str] = {}


class InvalidResourceName(ValueError):
    """Exception raised when the resource name cannot be parsed."""

    def __init__(self, msg: str) -> None:
        self.msg = msg

    @classmethod
    def bad_syntax(
        cls, syntax: str, resource_name: str, ex: Optional[Exception] = None
    ) -> Self:
        """Build an exception when the resource name cannot be parsed."""
        if ex:
            msg = "The syntax is '%s' (%s)." % (syntax, ex)
        else:
            msg = "The syntax is '%s'." % syntax

        msg = "Could not parse '%s'. %s" % (resource_name, msg)

        return cls(msg)

    @classmethod
    def subclass_notfound(
        cls,
        interface_type_resource_class: Tuple[str, str],
        resource_name: Optional[str] = None,
    ) -> Self:
        """Build an exception when no parser has been registered for a pair."""

        msg = "Parser not found for: %s." % (interface_type_resource_class,)

        if resource_name:
            msg = "Could not parse '%s'. %s" % (resource_name, msg)

        return cls(msg)

    @classmethod
    def rc_notfound(
        cls, interface_type: str, resource_name: Optional[str] = None
    ) -> Self:
        """Build an exception when no resource class is provided and no default is found."""

        msg = (
            "Resource class for %s not provided and default not found." % interface_type
        )

        if resource_name:
            msg = "Could not parse '%s'. %s" % (resource_name, msg)

        return cls(msg)

    def __str__(self) -> str:
        return self.msg


T = TypeVar("T", bound=Type["ResourceName"])


def register_subclass(cls: T) -> T:
    """Register a subclass for a given interface type and resource class.

    Fields with a default value of None will be fully omitted from the resource
    string when formatted.

    """

    # Assemble the format string based on the resource parts
    fmt: OrderedDict[str, str] = OrderedDict([("interface_type", cls.interface_type)])
    syntax = cls.interface_type
    for ndx, f in enumerate(fields(cls)):
        sep = "::" if ndx else ""

        fmt[f.name] = sep + "{0}"

        if f.default == "":
            syntax += sep + f.name.replace("_", " ")
        else:
            syntax += "[" + sep + f.name.replace("_", " ") + "]"

    fmt["resource_class"] = "::" + cls.resource_class

    if not cls.is_rc_optional:
        syntax += "::" + cls.resource_class
    else:
        syntax += "[" + "::" + cls.resource_class + "]"

    cls._visa_syntax = syntax
    cls._canonical_fmt = fmt

    key = cls.interface_type, cls.resource_class

    if key in _SUBCLASSES:
        raise ValueError("Class already registered for %s and %s" % key)

    _SUBCLASSES[(cls.interface_type, cls.resource_class)] = cls

    _INTERFACE_TYPES.add(cls.interface_type)
    _RESOURCE_CLASSES[cls.interface_type].add(cls.resource_class)

    if cls.is_rc_optional:
        if cls.interface_type in _DEFAULT_RC:
            raise ValueError("Default already specified for %s" % cls.interface_type)
        _DEFAULT_RC[cls.interface_type] = cls.resource_class

    return cls


class _ResourceNameBase:
    """Base class for ResourceNames to be used as a mixin."""

    #: Specifices if the resource class part of the string is optional.
    is_rc_optional: ClassVar[bool] = False

    #: Formatting string for canonical
    _canonical_fmt: Dict[str, str]

    #: VISA syntax for resource
    _visa_syntax: str

    #: VISA syntax for resource
    _fields: Tuple[str, ...]

    #: Resource name provided by the user (not empty only when parsing)
    user: Optional[str]

    @classmethod
    def from_string(cls, resource_name: str) -> Self:
        """Parse a resource name and return a ResourceName

        Parameters
        ----------
        resource_name : str
            Name of the resource

        Raises
        ------
        InvalidResourceName
            Raised if the resource name is invalid.

        """
        # TODO Remote VISA

        uname = resource_name.upper()

        for interface_type in _INTERFACE_TYPES:
            # Loop through all known interface types until we found one
            # that matches the beginning of the resource name
            if not uname.startswith(interface_type):
                continue

            parts: List[str]
            if len(resource_name) == len(interface_type):
                parts = []
            else:
                parts = resource_name[len(interface_type) :].split("::")

            # Try to match the last part of the resource name to
            # one of the known resource classes for the given interface type.
            # If not possible, use the default resource class
            # for the given interface type.
            if parts and parts[-1] in _RESOURCE_CLASSES[interface_type]:
                parts, resource_class = parts[:-1], parts[-1]
            else:
                try:
                    resource_class = _DEFAULT_RC[interface_type]
                except KeyError:
                    raise InvalidResourceName.rc_notfound(interface_type, resource_name)

            # Look for the subclass
            try:
                subclass = _SUBCLASSES[(interface_type, resource_class)]
            except KeyError:
                raise InvalidResourceName.subclass_notfound(
                    (interface_type, resource_class), resource_name
                )

            # And create the object
            try:
                rn = subclass.from_parts(*parts)
                rn.user = resource_name
                return rn
            except (ValueError, TypeError) as ex:
                raise InvalidResourceName.bad_syntax(
                    subclass._visa_syntax, resource_name, ex
                )

        raise InvalidResourceName(
            "Could not parse %s: unknown interface type" % resource_name
        )

    @classmethod
    def from_kwargs(cls, **kwargs) -> Self:
        """Build a resource from keyword arguments."""
        interface_type = kwargs.pop("interface_type")

        if interface_type not in _INTERFACE_TYPES:
            raise InvalidResourceName("Unknown interface type: %s" % interface_type)

        try:
            if interface_type not in _DEFAULT_RC:
                resource_class = kwargs.pop("resource_class")
            else:
                resource_class = kwargs.pop(
                    "resource_class", _DEFAULT_RC[interface_type]
                )
        except KeyError:
            raise InvalidResourceName.rc_notfound(interface_type)

        # Look for the subclass
        try:
            subclass = _SUBCLASSES[(interface_type, resource_class)]
        except KeyError:
            raise InvalidResourceName.subclass_notfound(
                (interface_type, resource_class)
            )

        # And create the object
        try:
            # Always use for subclasses that do take arguments
            return subclass(**kwargs)  # type: ignore
        except (ValueError, TypeError) as ex:
            raise InvalidResourceName(str(ex))

    def __str__(self):
        s = ""
        for part, form in self._canonical_fmt.items():
            value = getattr(self, part, None)
            if value is not None:
                s += form.format(value)
        return s


@dataclass
class ResourceName(_ResourceNameBase):
    #: Interface type string
    interface_type: ClassVar[str]

    #: Resource class string
    resource_class: ClassVar[str]

    def __post_init__(self):
        # Ensure that all mandatory arguments have been passed
        for f in fields(self):
            if getattr(self, f.name) == "":
                raise TypeError(f.name + " is a required parameter")
        self._fields = tuple(f.name for f in fields(self))

    @property
    def interface_type_const(self) -> constants.InterfaceType:
        try:
            interface_type = self.interface_type.lower().replace("-", "_")
            return getattr(constants.InterfaceType, interface_type)
        except Exception:
            return constants.InterfaceType.unknown

        # Implemented when building concrete subclass in build_rn_class

    @classmethod
    def from_parts(cls, *parts):
        """Construct a resource name from a list of parts."""

        resource_parts = fields(cls)
        if len(parts) < sum(1 for f in resource_parts if f.default):
            raise ValueError("not enough parts")
        elif len(parts) > len(resource_parts):
            raise ValueError("too many parts")

        k, rp = resource_parts[0], resource_parts[1:]

        # The first part (just after the interface_type) is the only
        # optional part which can be empty and therefore the
        # default value should be used.
        p, pending = parts[0], parts[1:]
        kwargs = {k.name: k.default if p == "" else p}

        # The rest of the parts are consumed when mandatory elements are required.
        while len(pending) < len(rp):
            k, rp = rp[0], rp[1:]
            if k.default == "":
                # This is impossible as far as I can tell for currently implemented
                # resource names
                if not pending:
                    raise ValueError(k.name + " part is mandatory")  # pragma: no cover
                p, pending = pending[0], pending[1:]
                if not p:
                    raise ValueError(k.name + " part is mandatory")
                kwargs[k.name] = p
            else:
                kwargs[k.name] = k.default

        # When the length of the pending provided and resource parts
        # are equal, we just consume everything.
        kwargs.update((k.name, p) for k, p in zip(rp, pending))

        return cls(**kwargs)


# Build subclasses for each resource


@register_subclass
@dataclass
class GPIBInstr(ResourceName):
    """GPIB INSTR

    The syntax is:
    GPIB[board]::primary_address[::secondary_address][::INSTR]

    """

    #: GPIB board to use.
    board: str = "0"

    #: Primary address of the device to connect to
    primary_address: str = ""

    #: Secondary address of the device to connect to
    # Reference for the GPIB secondary address
    # https://www.mathworks.com/help/instrument/secondaryaddress.html
    # NOTE: a secondary address of 0 is not the same as no secondary address.
    secondary_address: Optional[str] = None

    interface_type: ClassVar[str] = "GPIB"
    resource_class: ClassVar[str] = "INSTR"
    is_rc_optional: ClassVar[bool] = True


@register_subclass
@dataclass
class GPIBIntfc(ResourceName):
    """GPIB INTFC

    The syntax is:
    GPIB[board]::INTFC

    """

    #: GPIB board to use.
    board: str = "0"

    interface_type: ClassVar[str] = "GPIB"
    resource_class: ClassVar[str] = "INTFC"


@register_subclass
@dataclass
class PrlgxASRLIntfc(ResourceName):
    """PRLGX-ASRL INTFC

    The syntax is:
    PRLGX-ASRL[board]::serial device::INTFC
    """

    #: GPIB "board" to use.
    board: str = "0"

    #: Serial device to use.
    serial_device: str = ""

    interface_type: ClassVar[str] = "PRLGX-ASRL"
    resource_class: ClassVar[str] = "INTFC"


@register_subclass
@dataclass
class PrlgxTCPIPIntfc(ResourceName):
    """PRLGX-TCPIP INTFC

    The syntax is:
    PRLGX-TCPIP[board]::host address[::port]::INTFC
    """

    #: GPIB "board" to use.
    board: str = "0"

    #: Host address of the device (IPv4 or host name)
    host_address: str = ""

    #: Port on which to establish the connection
    port: str = "1234"

    interface_type: ClassVar[str] = "PRLGX-TCPIP"
    resource_class: ClassVar[str] = "INTFC"


@register_subclass
@dataclass
class ASRLInstr(ResourceName):
    """ASRL INSTR

    The syntax is:
    ASRL[board]::INSTR

    """

    #: Serial connection to use.
    board: str = "0"

    interface_type: ClassVar[str] = "ASRL"
    resource_class: ClassVar[str] = "INSTR"
    is_rc_optional: ClassVar[bool] = True


@register_subclass
@dataclass
class TCPIPInstr(ResourceName):
    """TCPIP INSTR

    The syntax is:
    TCPIP[board]::host address[::LAN device name][::INSTR]

    """

    #: Board to use.
    board: str = "0"

    #: Host address of the device (IPv4 or host name)
    host_address: str = ""

    #: LAN device name of the device
    lan_device_name: str = "inst0"

    interface_type: ClassVar[str] = "TCPIP"
    resource_class: ClassVar[str] = "INSTR"
    is_rc_optional: ClassVar[bool] = True


@register_subclass
@dataclass
class VICPInstr(ResourceName):
    """VICP INSTR

    The syntax is:
    VICP::host address[::INSTR]

    """

    #: VICP resource do not support a board index. But it is the only resource
    #: in this case so we allow parsing one but set a default of ""
    _unused: None = None

    #: Host address of the device (IPv4 or host name)
    host_address: str = ""

    interface_type: ClassVar[str] = "VICP"
    resource_class: ClassVar[str] = "INSTR"
    is_rc_optional: ClassVar[bool] = True


@register_subclass
@dataclass
class TCPIPSocket(ResourceName):
    """TCPIP SOCKET

    The syntax is:
    TCPIP[board]::host address[::port]::SOCKET

    """

    #: Board to use
    board: str = "0"

    #: Host address of the device (IPv4 or host name)
    host_address: str = ""

    #: Port on which to establish the connection
    port: str = ""

    interface_type: ClassVar[str] = "TCPIP"
    resource_class: ClassVar[str] = "SOCKET"


@register_subclass
@dataclass
class USBInstr(ResourceName):
    """USB INSTR

    The syntax is:
    USB[board]::manufacturer ID::model code::serial number[::USB interface number][::INSTR]

    """

    #: USB board to use.
    board: str = "0"

    #: ID of the instrument manufacturer.
    manufacturer_id: str = ""

    #: Code identifying the model of the instrument.
    model_code: str = ""

    #: Serial number of the instrument.
    serial_number: str = ""

    #: USB interface number.
    usb_interface_number: str = "0"

    interface_type: ClassVar[str] = "USB"
    resource_class: ClassVar[str] = "INSTR"
    is_rc_optional: ClassVar[bool] = True


@register_subclass
@dataclass
class USBRaw(ResourceName):
    """USB RAW

    The syntax is:
    USB[board]::manufacturer ID::model code::serial number[::USB interface number]::RAW

    """

    #: USB board to use.
    board: str = "0"

    #: ID of the instrument manufacturer.
    manufacturer_id: str = ""

    #: Code identifying the model of the instrument.
    model_code: str = ""

    #: Serial number of the instrument.
    serial_number: str = ""

    #: USB interface number.
    usb_interface_number: str = "0"

    interface_type: ClassVar[str] = "USB"
    resource_class: ClassVar[str] = "RAW"


@register_subclass
@dataclass
class PXIBackplane(ResourceName):
    """PXI BACKPLANE

    The syntax is:
    PXI[interface]::chassis number::BACKPLANE

    """

    #: PXI interface number.
    interface: str = "0"

    #: PXI chassis number
    chassis_number: str = ""

    interface_type: ClassVar[str] = "PXI"
    resource_class: ClassVar[str] = "BACKPLANE"


@register_subclass
@dataclass
class PXIMemacc(ResourceName):
    """PXI MEMACC

    The syntax is:
    PXI[interface]::MEMACC

    """

    #: PXI interface number
    interface: str = "0"

    interface_type: ClassVar[str] = "PXI"
    resource_class: ClassVar[str] = "MEMACC"


@register_subclass
@dataclass
class VXIBackplane(ResourceName):
    """VXI BACKPLANE

    The syntax is:
    VXI[board]::VXI logical address::BACKPLANE

    """

    #: VXI board
    board: str = "0"

    #: VXI logical address
    vxi_logical_address: str = ""

    interface_type: ClassVar[str] = "VXI"
    resource_class: ClassVar[str] = "BACKPLANE"


@register_subclass
@dataclass
class VXIInstr(ResourceName):
    """VXI INSTR

    The syntax is:
    VXI[board]::VXI logical address[::INSTR]

    """

    #: VXI board
    board: str = "0"

    #: VXI logical address
    vxi_logical_address: str = ""

    interface_type: ClassVar[str] = "VXI"
    resource_class: ClassVar[str] = "INSTR"
    is_rc_optional: ClassVar[bool] = True


@register_subclass
@dataclass
class VXIMemacc(ResourceName):
    """VXI MEMACC

    The syntax is:
    VXI[board]::MEMACC

    """

    #: VXI board
    board: str = "0"

    interface_type: ClassVar[str] = "VXI"
    resource_class: ClassVar[str] = "MEMACC"


@register_subclass
@dataclass
class VXIServant(ResourceName):
    """VXI SERVANT

    The syntax is:
    VXI[board]::SERVANT

    """

    #: VXI board
    board: str = "0"

    interface_type: ClassVar[str] = "VXI"
    resource_class: ClassVar[str] = "SERVANT"


# TODO 3 types of PXI INSTR
# TODO ENET-Serial INSTR
# TODO Remote NI-VISA


def assemble_canonical_name(**kwargs) -> str:
    """Build the canonical resource name from a set of keyword arguments."""
    return str(ResourceName.from_kwargs(**kwargs))


def to_canonical_name(resource_name: str) -> str:
    """Parse a resource name and return the canonical version."""
    return str(ResourceName.from_string(resource_name))


parse_resource_name = ResourceName.from_string


def filter(resources: Iterable[str], query: str) -> Tuple[str, ...]:
    r"""Filter a list of resources according to a query expression.

    The search criteria specified in the query parameter has two parts:
      1. a VISA regular expression over a resource string.
      2. optional logical expression over attribute values
         (not implemented in this function, see below).

    .. note: The VISA regular expression syntax is not the same as the
             Python regular expression syntax. (see below)

    The regular expression is matched against the resource strings of resources
    known to the VISA Resource Manager. If the resource string matches the
    regular expression, the attribute values of the resource are then matched
    against the expression over attribute values. If the match is successful,
    the resource has met the search criteria and gets added to the list of
    resources found.

    By using the optional attribute expression, you can construct flexible
    and powerful expressions with the use of logical ANDs (&&), ORs(||),
    and NOTs (!). You can use equal (==) and unequal (!=) comparators to
    compare attributes of any type, and other inequality comparators
    (>, <, >=, <=) to compare attributes of numeric type. Use only global
    attributes in the attribute expression. Local attributes are not allowed
    in the logical expression part of the expr parameter.


    Symbol      Meaning
    ----------  ----------

    ?           Matches any one character.

    \           Makes the character that follows it an ordinary character
                instead of special character. For example, when a question
                mark follows a backslash (\?), it matches the ? character
                instead of any one character.

    [list]      Matches any one character from the enclosed list. You can
                use a hyphen to match a range of characters.

    [^list]     Matches any character not in the enclosed list. You can use
                a hyphen to match a range of characters.

    *           Matches 0 or more occurrences of the preceding character or
                expression.

    +           Matches 1 or more occurrences of the preceding character or
                expression.

    Exp|exp     Matches either the preceding or following expression. The or
                operator | matches the entire expression that precedes or
                follows it and not just the character that precedes or follows
                it. For example, VXI|GPIB means (VXI)|(GPIB), not VX(I|G)PIB.

    (exp)       Grouping characters or expressions.


    """

    if "{" in query:
        query, _ = query.split("{")
        logger.warning(
            "optional part of the query expression not supported. See filter2"
        )

    try:
        query = query.replace("?", ".")
        matcher = re.compile(query, re.IGNORECASE)
    except re.error:
        raise errors.VisaIOError(constants.VI_ERROR_INV_EXPR)

    return tuple(res for res in resources if matcher.match(res))


class _AttrGetter:
    """Smart attr getter infering common attribute from resource name.

    Used to implement filter2

    """

    def __init__(
        self, resource_name: str, open_resource: Callable[[str], "Resource"]
    ) -> None:
        self.resource_name = resource_name
        self.parsed = parse_resource_name(resource_name)
        self.resource = None
        self.open_resource = open_resource

    def __getattr__(self, item):  # noqa: C901
        if item == "VI_ATTR_INTF_NUM":
            try:
                return int(self.parsed.board)
            except AttributeError:
                return int(self.interface)
        elif item == "VI_ATTR_MANF_ID":
            if not isinstance(self.parsed, (USBInstr, USBRaw)):
                raise self.raise_missing_attr(item)
            else:
                return self.parsed.manufacturer_id
        elif item == "VI_ATTR_MODEL_CODE":
            if not isinstance(self.parsed, (USBInstr, USBRaw)):
                raise self.raise_missing_attr(item)
            else:
                return self.parsed.model_code
        elif item == "VI_ATTR_USB_SERIAL_NUM":
            if not isinstance(self.parsed, (USBInstr, USBRaw)):
                raise self.raise_missing_attr(item)
            else:
                return self.parsed.serial_number
        elif item == "VI_ATTR_USB_INTFC_NUM":
            if not isinstance(self.parsed, (USBInstr, USBRaw)):
                raise self.raise_missing_attr(item)
            else:
                return int(self.parsed.board)
        elif item == "VI_ATTR_TCPIP_ADDR":
            if not isinstance(self.parsed, (TCPIPInstr, TCPIPSocket)):
                raise self.raise_missing_attr(item)
            else:
                return self.parsed.host_address
        elif item == "VI_ATTR_TCPIP_DEVICE_NAME":
            if not isinstance(self.parsed, TCPIPInstr):
                raise self.raise_missing_attr(item)
            else:
                return self.parsed.lan_device_name
        elif item == "VI_ATTR_TCPIP_PORT":
            if not isinstance(self.parsed, TCPIPSocket):
                raise self.raise_missing_attr(item)
            else:
                return int(self.parsed.port)
        elif item == "VI_ATTR_GPIB_PRIMARY_ADDR":
            if not isinstance(self.parsed, GPIBInstr):
                raise self.raise_missing_attr(item)
            else:
                return int(self.parsed.primary_address)
        elif item == "VI_ATTR_GPIB_SECONDARY_ADDR":
            if not isinstance(self.parsed, GPIBInstr):
                raise self.raise_missing_attr(item)
            else:
                return (
                    int(self.parsed.secondary_address)
                    if self.parsed.secondary_address is not None
                    else constants.VI_NO_SEC_ADDR
                )
        elif item == "VI_ATTR_PXI_CHASSIS":
            if not isinstance(self.parsed, PXIBackplane):
                raise self.raise_missing_attr(item)
            else:
                return int(self.parsed.chassis_number)
        elif item == "VI_ATTR_MAINFRAME_LA":
            if not isinstance(self.parsed, (VXIInstr, VXIBackplane)):
                raise self.raise_missing_attr(item)
            else:
                return int(self.parsed.vxi_logical_address)

        if self.resource is None:
            self.resource = self.open_resource(self.resource_name)

        return self.resource.get_visa_attribute(item)

    def raise_missing_attr(self, item):
        raise errors.VisaIOError(constants.VI_ERROR_NSUP_ATTR)


def filter2(
    resources: Iterable[str], query: str, open_resource: Callable[[str], "Resource"]
) -> Tuple[str, ...]:
    """Filter a list of resources according to a query expression.

    It accepts the optional part of the expression.

    .. warning: This function is experimental and unsafe as it uses eval,
                It also might require to open the resource.

    Parameters
    ----------
    resources : Iterable[str]
        Iterable of resource name to filter.

    query : str
        The pattern to use for filtering

    open_resource : Callable[[str], Resource]
        Function to open a resource (typically ResourceManager().open_resource)

    """
    optional: Optional[str]
    if "{" in query:
        try:
            query, optional = query.split("{")
            optional, _ = optional.split("}")
        except ValueError:
            raise errors.VisaIOError(constants.VI_ERROR_INV_EXPR)
    else:
        optional = None

    filtered = filter(resources, query)

    if not optional:
        return tuple(filtered)

    optional = optional.replace("&&", "and").replace("||", "or").replace("!", "not ")
    optional = optional.replace("VI_", "res.VI_")

    @contextlib.contextmanager
    def open_close(resource_name):
        getter = _AttrGetter(resource_name, open_resource)
        yield getter
        if getter.resource is not None:
            getter.resource.close()

    selected = []
    for rn in filtered:
        with open_close(rn) as getter:
            try:
                if eval(optional, None, {"res": getter}):
                    selected.append(rn)
            except Exception:
                logger.exception("Failed to evaluate %s on %s", optional, rn)

    return tuple(selected)
