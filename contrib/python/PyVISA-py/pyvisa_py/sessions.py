# -*- coding: utf-8 -*-
"""Base Session class.


:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import abc
import time
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from pyvisa import attributes, constants, rname
from pyvisa.constants import ResourceAttribute, StatusCode
from pyvisa.typing import VISARMSession

from .common import LOGGER, int_to_byte

#: Type var used when typing register.
T = TypeVar("T", bound=Type["Session"])


class OpenError(Exception):
    """Custom exception signaling we failed to open a resource."""

    def __init__(self, error_code: StatusCode = StatusCode.error_resource_not_found):
        self.error_code = error_code


class UnknownAttribute(Exception):
    """Custom exception signaling a VISA attribute is not supported."""

    def __init__(self, attribute: constants.ResourceAttribute) -> None:
        self.attribute = attribute

    def __str__(self) -> str:
        attr = self.attribute
        if isinstance(attr, int):
            try:
                name = attributes.AttributesByID[attr].visa_name
            except KeyError:
                name = "Name not found"

            return "Unknown attribute %s (%s - %s)" % (attr, hex(attr), name)

        return "Unknown attribute %s" % attr

    __repr__ = __str__


class Session(metaclass=abc.ABCMeta):
    """A base class for Session objects.

    Just makes sure that common methods are defined and information is stored.

    Parameters
    ----------
    resource_manager_session : VISARMSession
        Session handle of the parent Resource Manager
    resource_name : str
        Name of the resource this session is communicating with
    parsed : rname.ResourceName, optional
        Parsed representation of the resource name. The default is False meaning
        that the provided resource name will be parsed.

    """

    @abc.abstractmethod
    def _get_attribute(
        self, attribute: constants.ResourceAttribute
    ) -> Tuple[Any, StatusCode]:
        """Get the value for a given VISA attribute for this session.

        Use to implement custom logic for attributes.

        Parameters
        ----------
        attribute : constants.ResourceAttribute
            Resource attribute for which the state query is made

        Returns
        -------
        Any
            State of the queried attribute for a specified resource
        constants.StatusCode
            Return value of the library call.

        """

    @abc.abstractmethod
    def _set_attribute(
        self, attribute: constants.ResourceAttribute, attribute_state: Any
    ) -> StatusCode:
        """Set the attribute_state value for a given VISA attribute for this session.

        Use to implement custom logic for attributes.

        Parameters
        ----------
        attribute : constants.ResourceAttribute
            Resource attribute for which the state query is made.
        attribute_state : Any
            Value to which to set the attribute.

        Returns
        -------
        StatusCode
            The return value of the library call.

        """

    @abc.abstractmethod
    def close(self) -> StatusCode:
        """Close the session.

        Use it to do final clean ups.

        """

    #: Session handle of the parent Resource Manager
    resource_manager_session: VISARMSession

    #: Name of the resource this session is communicating with
    resource_name: str

    #: Parsed representation of the resource name.
    parsed: rname.ResourceName

    #: Session type as (Interface Type, Resource Class)
    session_type: Tuple[constants.InterfaceType, str]

    #: Timeout in milliseconds to use when opening the resource.
    open_timeout: Optional[int]

    #: Value of the timeout in seconds used for general operation
    timeout: Optional[float]

    #: Used as a place holder for the object doing the lowlevel communication.
    interface: Any

    #: Used for attributes not handled by the underlying interface.
    #: Values are get or set automatically by get_attribute and set_attribute
    attrs: Dict[constants.ResourceAttribute, Any]

    #: Maps (Interface Type, Resource Class) to Python class encapsulating that
    #: resource.
    #: dict[(Interface Type, Resource Class) , Session]
    _session_classes: ClassVar[
        Dict[Tuple[constants.InterfaceType, str], Type["Session"]]
    ] = {}

    @staticmethod
    def list_resources() -> List[str]:
        """List the resources available for the resource class."""
        return []

    @classmethod
    def get_low_level_info(cls) -> str:
        """Get info about the backend used by the session."""
        return ""

    @classmethod
    def iter_valid_session_classes(
        cls,
    ) -> Iterator[Tuple[Tuple[constants.InterfaceType, str], Type["Session"]]]:
        """Iterator over valid sessions classes infos."""
        for key, val in cls._session_classes.items():
            if not issubclass(val, UnavailableSession):
                yield key, val

    @classmethod
    def iter_session_classes_issues(
        cls,
    ) -> Iterator[Tuple[Tuple[constants.InterfaceType, str], str]]:
        """Iterator over invalid sessions classes (i.e. those with import errors)."""
        for key, val in cls._session_classes.items():
            if issubclass(val, UnavailableSession):
                yield key, getattr(val, "session_issue")

    @classmethod
    def get_session_class(
        cls, interface_type: constants.InterfaceType, resource_class: str
    ) -> Type["Session"]:
        """Get the session class for a given interface type and resource class.

        Parameters
        ----------
        interface_type : constants.InterfaceType
            Type of interface.
        resource_class : str
            Class of resource.

        Returns
        -------
        Sessions
            Session subclass the most appropriate for the resource.

        """
        try:
            return cls._session_classes[(interface_type, resource_class)]
        except KeyError:
            raise ValueError(
                "No class registered for %s, %s" % (interface_type, resource_class)
            )

    @classmethod
    def register(
        cls, interface_type: constants.InterfaceType, resource_class: str
    ) -> Callable[[T], T]:
        """Register a session class for a given interface type and resource class.

        Parameters
        ----------
        interface_type : constants.InterfaceType
            Type of interface.
        resource_class : str
            Class of the resource

        Returns
        -------
        Callable[[T], T]
            Decorator function to register a session subclass.

        """

        def _internal(python_class):
            if (interface_type, resource_class) in cls._session_classes:
                LOGGER.warning(
                    "%s is already registered in the "
                    "ResourceManager. Overwriting with %s",
                    (interface_type, resource_class),
                    python_class,
                )

            python_class.session_type = (interface_type, resource_class)
            cls._session_classes[(interface_type, resource_class)] = python_class
            return python_class

        return _internal

    @classmethod
    def register_unavailable(
        cls, interface_type: constants.InterfaceType, resource_class: str, msg: str
    ) -> None:
        """Register that no session class exists.

        This creates a fake session that will raise a ValueError if called.

        Parameters
        ----------
        interface_type : constants.InterfaceType
            Type of interface.
        resource_class : str
            Class of the resource
        msg : str
            Message detailing why no session class exists for this particular
            interface type, resource class pair.

        Returns
        -------
        Type[Session]
            Fake session.

        """

        class _internal(UnavailableSession):
            #: Message detailing why no session is available.
            session_issue = msg

        if (interface_type, resource_class) in cls._session_classes:
            LOGGER.warning(
                "%s is already registered in the ResourceManager. "
                "Overwriting with unavailable %s",
                (interface_type, resource_class),
                msg,
            )

        cls._session_classes[(interface_type, resource_class)] = _internal

    def __init__(
        self,
        resource_manager_session: VISARMSession,
        resource_name: str,
        parsed: Optional[rname.ResourceName] = None,
        open_timeout: Optional[int] = None,
    ) -> None:
        if parsed is None:
            parsed = rname.parse_resource_name(resource_name)

        self.parsed = parsed
        self.open_timeout = open_timeout

        #: Used as a place holder for the object doing the lowlevel communication.
        self.interface = None

        #: Used for attributes not handled by the underlying interface.
        #: Values are get or set automatically by get_attribute and
        #: set_attribute
        #: Add your own by overriding after_parsing.
        self.attrs = {
            ResourceAttribute.resource_manager_session: resource_manager_session,
            ResourceAttribute.resource_name: str(parsed),
            ResourceAttribute.resource_class: parsed.resource_class,
            ResourceAttribute.interface_type: parsed.interface_type_const,
            ResourceAttribute.timeout_value: (self._get_timeout, self._set_timeout),
        }

        #: Timeout expressed in second or None for the absence of a timeout.
        #: The default value is set when calling self.set_attribute(attr, default_timeout)
        self.timeout = None

        #: Set the default timeout from constants
        attr = ResourceAttribute.timeout_value
        default_timeout = attributes.AttributesByID[attr].default
        self.set_attribute(attr, default_timeout)

        self.after_parsing()

    def after_parsing(self) -> None:
        """Override this method to provide custom initialization code, to be
        called after the resource name is properly parsed

        ResourceSession can register resource specific attributes handling of
        them into self.attrs.
        It is also possible to change handling of already registered common
        attributes. List of attributes is available in pyvisa package:
        * name is in constants module as: VI_ATTR_<NAME>
        * validity of attribute for resource is defined module attributes,
        AttrVI_ATTR_<NAME>.resources

        For static (read only) values, simple readonly and also readwrite
        attributes simplified construction can be used:
        `    self.attrs[constants.VI_ATTR_<NAME>] = 100`
        or
        `    self.attrs[constants.VI_ATTR_<NAME>] = <self.variable_name>`

        For more complex handling of attributes, it is possible to register
        getter and/or setter. When Null is used, NotSupported error is
        returned.
        Getter has same signature as see Session._get_attribute and setter has
        same signature as see Session._set_attribute. (It is possible to
        register also see Session._get_attribute and see Session._set_attribute
        as getter/setter). Getter and Setter are registered as tuple.
        For readwrite attribute:
        `    self.attrs[constants.VI_ATTR_<NAME>] = (<getter_name>,
                                                     <setter_name>)`
        For readonly attribute:
        `    self.attrs[constants.VI_ATTR_<NAME>] = (<getter_name>, None)`
        For reusing of see Session._get_attribute and see
        Session._set_attribute
        `    self.attrs[constants.VI_ATTR_<NAME>] = (self._get_attribute,
                                                     self._set_attribute)`

        """
        pass

    def write(self, data: bytes) -> Tuple[int, StatusCode]:
        """Writes data to device or interface synchronously.

        Corresponds to viWrite function of the VISA library.

        Parameters
        ----------
        data : bytes
            Data to be written.

        Returns
        -------
        int
            Number of bytes actually transferred
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def read(self, count: int) -> Tuple[bytes, StatusCode]:
        """Reads data from device or interface synchronously.

        Corresponds to viRead function of the VISA library.

        Parameters
        -----------
        count : int
            Number of bytes to be read.

        Returns
        -------
        bytes
            Data read from the device
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError()

    def clear(self) -> StatusCode:
        """Clears a device.

        Corresponds to viClear function of the VISA library.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        return StatusCode.error_nonsupported_operation

    def flush(self, mask: constants.BufferOperation) -> StatusCode:
        """Flush the specified buffers.

        The buffers can be associated with formatted I/O operations and/or
        serial communication.

        Corresponds to viFlush function of the VISA library.

        Parameters
        ----------
        mask : constants.BufferOperation
            Specifies the action to be taken with flushing the buffer.
            The values can be combined using the | operator. However multiple
            operations on a single buffer cannot be combined.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def read_stb(self) -> Tuple[int, StatusCode]:
        """Reads a status byte of the service request.

        Corresponds to viReadSTB function of the VISA library.

        Returns
        -------
        int
            Service request status byte
        StatusCode
            Return value of the library call.

        """
        return 0, StatusCode.error_nonsupported_operation

    def lock(
        self,
        lock_type: constants.Lock,
        timeout: int,
        requested_key: Optional[str] = None,
    ):
        """Establishes an access mode to the specified resources.

        Corresponds to viLock function of the VISA library.

        Parameters
        ----------
        lock_type : constants.Lock
            Specifies the type of lock requested.
        timeout : int
            Absolute time period (in milliseconds) that a resource waits to get
            unlocked by the locking session before returning an error.
        requested_key : Optional[str], optional
            Requested locking key in the case of a shared lock. For an exclusive
            lock it should be None.

        Returns
        -------
        Optional[str]
            Key that can then be passed to other sessions to share the lock, or
            None for an exclusive lock.
        constants.StatusCode
            Return value of the library call.

        """
        return "", StatusCode.error_nonsupported_operation

    def unlock(self) -> StatusCode:
        """Relinquishes a lock for the specified resource.

        Corresponds to viUnlock function of the VISA library.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        return StatusCode.error_nonsupported_operation

    def gpib_command(self, command_byte: bytes) -> Tuple[int, StatusCode]:
        """Write GPIB command bytes on the bus.

        Corresponds to viGpibCommand function of the VISA library.

        Parameters
        ----------
        data : bytes
            Data to write.

        Returns
        -------
        int
            Number of written bytes
        constants.StatusCode
            Return value of the library call.

        """
        return 0, StatusCode.error_nonsupported_operation

    def assert_trigger(self, protocol: constants.TriggerProtocol) -> StatusCode:
        """Assert software or hardware trigger.

        Corresponds to viAssertTrigger function of the VISA library.

        Parameters
        ----------
        protocol : constants.TriggerProtocol
            Trigger protocol to use during assertion.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def gpib_send_ifc(self) -> StatusCode:
        """Pulse the interface clear line (IFC) for at least 100 microseconds.

        Corresponds to viGpibSendIFC function of the VISA library.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        return StatusCode.error_nonsupported_operation

    def gpib_control_ren(
        self, mode: constants.RENLineOperation
    ) -> constants.StatusCode:
        """Controls the state of the GPIB Remote Enable (REN) interface line.

        Optionally the remote/local state of the device can also be set.

        Corresponds to viGpibControlREN function of the VISA library.

        Parameters
        ----------
        mode : constants.RENLineOperation
            State of the REN line and optionally the device remote/local state.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        return StatusCode.error_nonsupported_operation

    def gpib_control_atn(self, mode: constants.ATNLineOperation) -> StatusCode:
        """Specifies the state of the ATN line and the local active controller state.

        Corresponds to viGpibControlATN function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        mode : constants.ATNLineOperation
            State of the ATN line and optionally the local active controller state.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        return StatusCode.error_nonsupported_operation

    def gpib_pass_control(
        self, primary_address: int, secondary_address: int
    ) -> StatusCode:
        """Tell a GPIB device to become controller in charge (CIC).

        Corresponds to viGpibPassControl function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        primary_address : int
            Primary address of the GPIB device to which you want to pass control.
        secondary_address : int
            Secondary address of the targeted GPIB device.
            If the targeted device does not have a secondary address, this parameter
            should contain the value Constants.VI_NO_SEC_ADDR.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        return StatusCode.error_nonsupported_operation

    def get_attribute(self, attribute: ResourceAttribute) -> Tuple[Any, StatusCode]:
        """Get the value for a given VISA attribute for this session.

        Does a few checks before and calls before dispatching to
        `_get_attribute`.

        Parameters
        ----------
        attribute : ResourceAttribute
            Resource attribute for which the state query is made

        Returns
        -------
        Any
            The state of the queried attribute for a specified resource.
        StatusCode
            Return value of the library call.

        """

        # Check if the attribute value is defined.
        try:
            attr = attributes.AttributesByID[attribute]
        except KeyError:
            return 0, StatusCode.error_nonsupported_attribute

        # Check if the attribute is defined for this session type.
        if not attr.in_resource(self.session_type):
            return 0, StatusCode.error_nonsupported_attribute

        # Check if reading the attribute is allowed.
        if not attr.read:
            raise Exception("Do not now how to handle write only attributes.")

        # First try to answer those attributes that are registered in
        # self.attrs, see Session.after_parsing
        if attribute in self.attrs:
            value = self.attrs[attribute]
            status = StatusCode.success
            if isinstance(value, tuple):
                getter = value[0]
                value, status = (
                    getter(attribute)
                    if getter
                    else (0, StatusCode.error_nonsupported_attribute)
                )
            return value, status

        # Dispatch to `_get_attribute`, which must be implemented by subclasses

        try:
            return self._get_attribute(attribute)
        except UnknownAttribute as e:
            LOGGER.exception(str(e))
            return 0, StatusCode.error_nonsupported_attribute

    def set_attribute(
        self, attribute: ResourceAttribute, attribute_state: Any
    ) -> StatusCode:
        """Set the attribute_state value for a given VISA attribute for this
        session.

        Does a few checks before and calls before dispatching to
        `_set_attribute`.

        Parameters
        ----------
        attribute : ResourceAttribute
            Resource attribute for which the state query is made.
        attribute_state : Any
            Value.

        Returns
        -------
        StatusCode
            The return value of the library call.

        """
        # Check if the attribute value is defined.
        try:
            attr = attributes.AttributesByID[attribute]
        except KeyError:
            return StatusCode.error_nonsupported_attribute

        # Check if the attribute is defined for this session type.
        if not attr.in_resource(self.session_type):
            return StatusCode.error_nonsupported_attribute

        # Check if writing the attribute is allowed.
        if not attr.write:
            return StatusCode.error_attribute_read_only

        # First try to answer those attributes that are registered in
        # self.attrs, see Session.after_parsing
        if attribute in self.attrs:
            value = self.attrs[attribute]
            status = StatusCode.success
            if isinstance(value, tuple):
                setter = value[1]
                status = (
                    setter(attribute, attribute_state)
                    if setter
                    else StatusCode.error_nonsupported_attribute
                )
            else:
                self.attrs[attribute] = attribute_state
            return status

        # Dispatch to `_set_attribute`, which must be implemented by subclasses

        try:
            return self._set_attribute(attribute, attribute_state)
        except ValueError:
            return StatusCode.error_nonsupported_attribute_state
        except NotImplementedError:
            e = UnknownAttribute(attribute)
            LOGGER.exception(str(e))
            return StatusCode.error_nonsupported_attribute
        except UnknownAttribute as e:
            LOGGER.exception(str(e))
            return StatusCode.error_nonsupported_attribute

    def _read(
        self,
        reader: Callable[[], bytes],
        count: int,
        end_indicator_checker: Callable[[bytes], bool],
        suppress_end_en: bool,
        termination_char: Optional[int],
        termination_char_en: bool,
        timeout_exception: Type[Exception],
    ) -> Tuple[bytes, StatusCode]:
        """Reads data from device or interface synchronously.

        Corresponds to viRead function of the VISA library.

        Parameters
        ----------
        reader : Callable[[], bytes]
            Function to read one or more bytes.
        count : int
            Number of bytes to be read.
        end_indicator_checker : Callable[[bytes], bool]
            Function to check if the message is complete.
        suppress_end_en : bool
            Suppress end.
        termination_char : int
            Stop reading if this character is received.
        termination_char_en : bool
            Is termination char enabled.
        timeout_exception : Type[Exception]
            Exception to capture time out for the given interface.

        Returns
        -------
        bytes
            Data read from the resource.
        StatusCode
            Return value of the library call.

        """
        # NOTE: Some interfaces return not only a single byte but a complete
        # block for each read therefore we must handle the case that the
        # termination character is in the middle of the  block or that the
        # maximum number of bytes is exceeded

        # Turn the termination_char store as an int in VISA attribute in a byte
        term_char = (
            int_to_byte(termination_char) if termination_char is not None else b""
        )

        finish_time = None if self.timeout is None else (time.time() + self.timeout)
        out = bytearray()
        while True:
            try:
                current = reader()
            except timeout_exception:
                return out, StatusCode.error_timeout

            if current:
                out.extend(current)
                end_indicator_received = end_indicator_checker(current)
                if end_indicator_received and not suppress_end_en:
                    # RULE 6.1.1
                    return bytes(out), StatusCode.success
                else:
                    if termination_char_en and (term_char in current):
                        # RULE 6.1.2
                        # Return everything up to and including the termination
                        # character
                        return (
                            bytes(out[: out.index(term_char) + 1]),
                            StatusCode.success_termination_character_read,
                        )
                    elif len(out) >= count:
                        # RULE 6.1.3
                        # Return at most the number of bytes requested
                        return (bytes(out[:count]), StatusCode.success_max_count_read)

            if finish_time and time.time() > finish_time:
                return bytes(out), StatusCode.error_timeout

    def _get_timeout(self, attribute: ResourceAttribute) -> Tuple[int, StatusCode]:
        """Returns timeout calculated value from python way to VI_ way

        In VISA, the timeout is expressed in milliseconds or using the
        constants VI_TMO_INFINITE or VI_TMO_IMMEDIATE.

        In Python we store it as either None (VI_TMO_INFINITE), 0
        (VI_TMO_IMMEDIATE) or as a floating point number in seconds.

        """
        if self.timeout is None:
            ret_value = constants.VI_TMO_INFINITE
        elif self.timeout == 0:
            ret_value = constants.VI_TMO_IMMEDIATE
        else:
            ret_value = int(self.timeout * 1000.0)
        return ret_value, StatusCode.success

    def _set_timeout(self, attribute: ResourceAttribute, value: int):
        """Sets timeout calculated value from python way to VI_ way

        In VISA, the timeout is expressed in milliseconds or using the
        constants VI_TMO_INFINITE or VI_TMO_IMMEDIATE.

        In Python we store it as either None (VI_TMO_INFINITE), 0
        (VI_TMO_IMMEDIATE) or as a floating point number in seconds.

        """
        if value == constants.VI_TMO_INFINITE:
            self.timeout = None
        elif value == constants.VI_TMO_IMMEDIATE:
            self.timeout = 0
        else:
            self.timeout = value / 1000.0
        return StatusCode.success


class UnavailableSession(Session):
    session_issue: ClassVar[str]

    def __init__(self, *args, **kwargs) -> None:
        raise ValueError(self.session_issue)

    def _get_attribute(self, attr):
        raise NotImplementedError()

    def _set_attribute(self, attr, value):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()
