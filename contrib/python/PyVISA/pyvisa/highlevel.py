# -*- coding: utf-8 -*-
"""High level Visa library wrapper.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import atexit
import contextlib
import copy
import os
import pkgutil
import warnings
from collections import defaultdict
from importlib import import_module
from itertools import chain
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    SupportsBytes,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from weakref import WeakMethod, WeakSet, WeakValueDictionary

from typing_extensions import ClassVar, DefaultDict, Literal

from . import attributes, constants, errors, logger, rname
from .constants import StatusCode
from .typing import (
    VISAEventContext,
    VISAHandler,
    VISAJobID,
    VISAMemoryAddress,
    VISARMSession,
    VISASession,
)
from .util import DebugInfo, LibraryPath

if TYPE_CHECKING:
    from .resources import Resource  # pragma: no cover

#: Resource extended information
#:
#: Named tuple with information about a resource. Returned by some :class:`ResourceManager` methods.
#:
#: :interface_type: Interface type of the given resource string. :class:`pyvisa.constants.InterfaceType`
#: :interface_board_number: Board number of the interface of the given resource string. We allow None
#:                          since serial resources may not sometimes be easily described
#:                          by a single number in particular on Linux system.
#: :resource_class: Specifies the resource class (for example, "INSTR") of the given resource string.
#: :resource_name: This is the expanded version of the given resource string.
#:                 The format should be similar to the VISA-defined canonical resource name.
#: :alias: Specifies the user-defined alias for the given resource string.
ResourceInfo = NamedTuple(
    "ResourceInfo",
    (
        ("interface_type", constants.InterfaceType),
        ("interface_board_number", Optional[int]),
        ("resource_class", Optional[str]),
        ("resource_name", Optional[str]),
        ("alias", Optional[str]),
    ),
)

# Used to properly type the __new__ method
T = TypeVar("T", bound="VisaLibraryBase")


class VisaLibraryBase(object):
    """Base for VISA library classes.

    A class derived from `VisaLibraryBase` library provides the low-level communication
    to the underlying devices providing Pythonic wrappers to VISA functions. But not all
    derived class must/will implement all methods. Even if methods are expected to return
    the status code they are expected to raise the appropriate exception when an error
    occurred since this is more Pythonic.

    The default VisaLibrary class is :class:`pyvisa.ctwrapper.highlevel.IVIVisaLibrary`,
    which implements a ctypes wrapper around the IVI-VISA library.
    Certainly, IVI-VISA can be NI-VISA, Keysight VISA, R&S VISA, tekVISA etc.

    In general, you should not instantiate it directly. The object exposed to the user
    is the :class:`pyvisa.highlevel.ResourceManager`. If needed, you can access the
    VISA library from it::

        >>> import pyvisa
        >>> rm = pyvisa.ResourceManager("/path/to/my/libvisa.so.7")
        >>> lib = rm.visalib

    """

    #: Default ResourceManager instance for this library.
    resource_manager: Optional["ResourceManager"]

    #: Path to the VISA library used by this instance
    library_path: LibraryPath

    #: Maps library path to VisaLibrary object
    _registry: ClassVar[
        Dict[Tuple[Type["VisaLibraryBase"], LibraryPath], "VisaLibraryBase"]
    ] = WeakValueDictionary()  # type: ignore

    #: Last return value of the library.
    _last_status: StatusCode = StatusCode(0)

    #: Maps session handle to last status.
    _last_status_in_session: Dict[
        Union[VISASession, VISARMSession, VISAEventContext], StatusCode
    ]

    #: Maps session handle to warnings to ignore.
    _ignore_warning_in_session: Dict[int, set]

    #: Extra information used for logging errors
    _logging_extra: Dict[str, str]

    #: Contains all installed event handlers.
    #: Its elements are tuples with four elements: The handler itself (a Python
    #: callable), the user handle (in any format making sense to the lower level
    #: implementation, ie as a ctypes object for the ctypes backend) and the
    #: handler again, this time in a format meaningful to the backend (ie as a
    #: ctypes object created with CFUNCTYPE for the ctypes backend) and
    #: the event type.
    handlers: DefaultDict[VISASession, List[Tuple[VISAHandler, Any, Any, Any]]]

    #: Set error codes on which to issue a warning.
    issue_warning_on: Set[StatusCode]

    def __new__(
        cls: Type[T], library_path: Union[str, LibraryPath] = ""
    ) -> "VisaLibraryBase":
        """Create a new VISA library from the specified path.

        If a library was already created using the same path and class this
        library object is returned instead.

        Parameters
        ----------
        library_path : str | LibraryPath
            Path to the VISA library to use in the backend.

        Raises
        ------
        OSError
            Raised if the VISA library object could not be created.

        """
        if library_path == "":
            errs = []
            for path in cls.get_library_paths():
                try:
                    return cls(path)
                except OSError as e:
                    logger.debug("Could not open VISA library %s: %s", path, str(e))
                    errs.append(str(e))
                except Exception as e:
                    errs.append(str(e))
            else:
                raise OSError("Could not open VISA library:\n" + "\n".join(errs))

        if not isinstance(library_path, LibraryPath):
            lib_path = LibraryPath(library_path, "user specified")
        else:
            lib_path = library_path

        if (cls, lib_path) in cls._registry:
            return cls._registry[(cls, lib_path)]

        obj = super(VisaLibraryBase, cls).__new__(cls)

        obj.library_path = lib_path

        obj._logging_extra = {"library_path": obj.library_path}

        obj._init()

        # Create instance specific registries.
        #: Error codes on which to issue a warning.
        obj.issue_warning_on = set(errors.default_warnings)
        obj._last_status_in_session = {}
        obj._ignore_warning_in_session = defaultdict(set)
        obj.handlers = defaultdict(list)
        obj.resource_manager = None

        logger.debug("Created library wrapper for %s", lib_path)

        cls._registry[(cls, lib_path)] = obj

        return obj

    @staticmethod
    def get_library_paths() -> Iterable[LibraryPath]:
        """Override to list the possible library_paths if no path is specified."""
        return ()

    @staticmethod
    def get_debug_info() -> DebugInfo:
        """Override to return an iterable of lines with the backend debug details."""
        return ["Does not provide debug info"]

    def _init(self) -> None:
        """Override this method to customize VisaLibrary initialization."""
        pass

    def __str__(self) -> str:
        """str representation of the library."""
        return "Visa Library at %s" % self.library_path

    def __repr__(self) -> str:
        """str representation of the library including the type of the library."""
        return "<%s(%r)>" % (type(self).__name__, self.library_path)

    def handle_return_value(
        self,
        session: Optional[Union[VISAEventContext, VISARMSession, VISASession]],
        status_code: int,
    ) -> StatusCode:
        """Helper function handling the return code of a low-level operation.

        Used when implementing concrete subclasses of VISALibraryBase.

        """
        rv: StatusCode
        try:
            rv = StatusCode(status_code)
        except ValueError:
            rv = cast(StatusCode, status_code)

        self._last_status = rv

        if session is not None:
            self._last_status_in_session[session] = rv

        if rv < 0:
            raise errors.VisaIOError(rv)

        if rv in self.issue_warning_on:
            if session and rv not in self._ignore_warning_in_session[session]:
                warnings.warn(errors.VisaIOWarning(rv), stacklevel=2)

        # Return the original value for further processing.
        return rv

    @property
    def last_status(self) -> StatusCode:
        """Last return value of the library."""
        return self._last_status

    def get_last_status_in_session(
        self, session: Union[VISASession, VISARMSession]
    ) -> StatusCode:
        """Last status in session.

        Helper function to be called by resources properties.

        """
        try:
            return self._last_status_in_session[session]
        except KeyError:
            raise errors.Error(
                "The session %r does not seem to be valid as it does not have any last status"
                % session
            )

    @contextlib.contextmanager
    def ignore_warning(
        self,
        session: Union[VISASession, VISARMSession],
        *warnings_constants: StatusCode,
    ) -> Iterator:
        """Ignore warnings for a session for the duration of the context.

        Parameters
        ----------
        session : Union[VISASession, VISARMSession]
            Unique logical identifier to a session.
        warnings_constants : StatusCode
            Constants identifying the warnings to ignore.

        """
        self._ignore_warning_in_session[session].update(warnings_constants)
        yield
        self._ignore_warning_in_session[session].difference_update(warnings_constants)

    def install_visa_handler(
        self,
        session: VISASession,
        event_type: constants.EventType,
        handler: VISAHandler,
        user_handle: Any = None,
    ) -> Any:
        """Installs handlers for event callbacks.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        event_type : constants.EventType,
            Logical event identifier.
        handler : VISAHandler
            Handler to be installed by a client application.
        user_handle :
            A value specified by an application that can be used for identifying
            handlers uniquely for an event type.

        Returns
        -------
        converted_user_handle:
            Converted user handle to match the underlying library. This version
            of the handle should be used in further call to the library.

        """
        try:
            new_handler = self.install_handler(
                session, event_type, handler, user_handle
            )
        except TypeError as e:
            raise errors.VisaTypeError(str(e)) from e

        self.handlers[session].append((*new_handler[:-1], event_type))
        return new_handler[1]

    def uninstall_visa_handler(
        self,
        session: VISASession,
        event_type: constants.EventType,
        handler: VISAHandler,
        user_handle: Any = None,
    ) -> None:
        """Uninstalls handlers for events.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        event_type : constants.EventType
            Logical event identifier.
        handler : VISAHandler
            Handler to be uninstalled by a client application.
        user_handle :
            The user handle returned by install_visa_handler.

        """
        for ndx, element in enumerate(self.handlers[session]):
            # use == rather than is to allow bound methods as handlers
            if (
                element[0] == handler
                and element[1] is user_handle
                and element[3] == event_type
            ):
                del self.handlers[session][ndx]
                break
        else:
            raise errors.UnknownHandler(event_type, handler, user_handle)
        self.uninstall_handler(session, event_type, element[2], user_handle)

    def __uninstall_all_handlers_helper(self, session: VISASession) -> None:
        """Uninstall all VISA handlers for a given session."""
        for element in self.handlers[session]:
            self.uninstall_handler(session, element[3], element[2], element[1])
        del self.handlers[session]

    def uninstall_all_visa_handlers(self, session: Optional[VISASession]) -> None:
        """Uninstalls all previously installed handlers for a particular session.

        Parameters
        ----------
        session : VISASession | None
            Unique logical identifier to a session. If None, operates on all sessions.

        """

        if session is not None:
            self.__uninstall_all_handlers_helper(session)
        else:
            for session in list(self.handlers):
                self.__uninstall_all_handlers_helper(session)

    def read_memory(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        width: Union[Literal[8, 16, 32, 64], constants.DataWidth],
        extended: bool = False,
    ) -> Tuple[int, StatusCode]:
        """Read a value from the specified memory space and offset.

        Corresponds to viIn* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Specifies the address space from which to read.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        width : Union[Literal[8, 16, 32, 64], constants.DataWidth]
            Number of bits to read (8, 16, 32 or 64).
        extended : bool, optional
            Use 64 bits offset independent of the platform.

        Returns
        -------
        data : int
            Data read from memory
        status_code : StatusCode
            Return value of the library call.

        Raises
        ------
        ValueError
            Raised if an invalid width is specified.

        """
        w = width * 8 if isinstance(width, constants.DataWidth) else width
        if w not in (8, 16, 32, 64):
            raise ValueError(
                "%s is not a valid size. Valid values are 8, 16, 32 or 64 "
                "or one member of constants.DataWidth" % width
            )
        return getattr(self, f"in_{w}")(session, space, offset, extended)

    def write_memory(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        data: int,
        width: Union[Literal[8, 16, 32, 64], constants.DataWidth],
        extended: bool = False,
    ) -> StatusCode:
        """Write a value to the specified memory space and offset.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Specifies the address space.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        data : int
            Data to write to bus.
        width : Union[Literal[8, 16, 32, 64], constants.DataWidth]
            Number of bits to read.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        StatusCode
            Return value of the library call.

        Raises
        ------
        ValueError
            Raised if an invalid width is specified.

        """
        w = width * 8 if isinstance(width, constants.DataWidth) else width
        if w not in (8, 16, 32, 64):
            raise ValueError(
                "%s is not a valid size. Valid values are 8, 16, 32 or 64 "
                "or one member of constants.DataWidth" % width
            )
        return getattr(self, f"out_{w}")(session, space, offset, data, extended)

    def move_in(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        width: Union[Literal[8, 16, 32, 64], constants.DataWidth],
        extended: bool = False,
    ) -> Tuple[List[int], StatusCode]:
        """Move a block of data to local memory from the given address space and offset.

        Corresponds to viMoveIn* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space from which to move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
             Number of elements to transfer, where the data width of
             the elements to transfer is identical to the source data width.
        width : Union[Literal[8, 16, 32, 64], constants.DataWidth]
            Number of bits to read per element.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        data : List[int]
            Data read from the bus
        status_code : StatusCode
            Return value of the library call.

        Raises
        ------
        ValueError
            Raised if an invalid width is specified.

        """
        w = width * 8 if isinstance(width, constants.DataWidth) else width
        if w not in (8, 16, 32, 64):
            raise ValueError(
                "%s is not a valid size. Valid values are 8, 16, 32 or 64 "
                "or one member of constants.DataWidth" % width
            )
        return getattr(self, f"move_in_{w}")(session, space, offset, length, extended)

    def move_out(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        data: Iterable[int],
        width: Union[Literal[8, 16, 32, 64], constants.DataWidth],
        extended: bool = False,
    ) -> StatusCode:
        """Move a block of data from local memory to the given address space and offset.

        Corresponds to viMoveOut* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space into which move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
            Number of elements to transfer, where the data width of
            the elements to transfer is identical to the source data width.
        data : Iterable[int]
            Data to write to bus.
        width : Union[Literal[8, 16, 32, 64], constants.DataWidth]
            Number of bits to per element.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        StatusCode
            Return value of the library call.

        Raises
        ------
        ValueError
            Raised if an invalid width is specified.

        """
        w = width * 8 if isinstance(width, constants.DataWidth) else width
        if w not in (8, 16, 32, 64):
            raise ValueError(
                "%s is not a valid size. Valid values are 8, 16, 32 or 64 "
                "or one member of constants.DataWidth" % width
            )
        return getattr(self, f"move_out_{w}")(
            session, space, offset, length, data, extended
        )

    def peek(
        self,
        session: VISASession,
        address: VISAMemoryAddress,
        width: Union[Literal[8, 16, 32, 64], constants.DataWidth],
    ) -> Tuple[int, StatusCode]:
        """Read an 8, 16, 32, or 64-bit value from the specified address.

        Corresponds to viPeek* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        address : VISAMemoryAddress
            Source address to read the value.
        width : Union[Literal[8, 16, 32, 64], constants.DataWidth]
            Number of bits to read.

        Returns
        -------
        data : int
            Data read from bus
        status_code : StatusCode
            Return value of the library call.

        Raises
        ------
        ValueError
            Raised if an invalid width is specified.

        """
        w = width * 8 if isinstance(width, constants.DataWidth) else width
        if w not in (8, 16, 32, 64):
            raise ValueError(
                "%s is not a valid size. Valid values are 8, 16, 32 or 64 "
                "or one member of constants.DataWidth" % width
            )
        return getattr(self, f"peek_{w}")(session, address)

    def poke(
        self,
        session: VISASession,
        address: VISAMemoryAddress,
        width: Union[Literal[8, 16, 32, 64], constants.DataWidth],
        data: int,
    ) -> StatusCode:
        """Writes an 8, 16, 32, or 64-bit value from the specified address.

        Corresponds to viPoke* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        address : VISAMemoryAddress
            Source address to read the value.
        width : Union[Literal[8, 16, 32, 64], constants.DataWidth]
            Number of bits to read.
        data : int
            Data to write to the bus

        Returns
        -------
        status_code : StatusCode
            Return value of the library call.

        Raises
        ------
        ValueError
            Raised if an invalid width is specified.

        """
        w = width * 8 if isinstance(width, constants.DataWidth) else width
        if w not in (8, 16, 32, 64):
            raise ValueError(
                "%s is not a valid size. Valid values are 8, 16, 32 or 64 "
                "or one member of constants.DataWidth" % width
            )
        return getattr(self, f"poke_{w}")(session, address, data)

    # Methods that VISA Library implementations must implement

    def assert_interrupt_signal(
        self,
        session: VISASession,
        mode: constants.AssertSignalInterrupt,
        status_id: int,
    ) -> StatusCode:
        """Asserts the specified interrupt or signal.

        Corresponds to viAssertIntrSignal function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        mode : constants.AssertSignalInterrupt
            How to assert the interrupt.
        status_id : int
            Status value to be presented during an interrupt acknowledge cycle.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def assert_trigger(
        self, session: VISASession, protocol: constants.TriggerProtocol
    ) -> StatusCode:
        """Assert software or hardware trigger.

        Corresponds to viAssertTrigger function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        protocol : constants.TriggerProtocol
            Trigger protocol to use during assertion.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def assert_utility_signal(
        self, session: VISASession, line: constants.UtilityBusSignal
    ) -> StatusCode:
        """Assert or deassert the specified utility bus signal.

        Corresponds to viAssertUtilSignal function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        line : constants.UtilityBusSignal
            Specifies the utility bus signal to assert.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def buffer_read(self, session: VISASession, count: int) -> Tuple[bytes, StatusCode]:
        """Reads data through the use of a formatted I/O read buffer.

        The data can be read from a device or an interface.

        Corresponds to viBufRead function of the VISA library.

        Parameters
        ----------
        session : VISASession\
            Unique logical identifier to a session.
        count : int
            Number of bytes to be read.

        Returns
        -------
        dbytes
            Data read
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def buffer_write(self, session: VISASession, data: bytes) -> Tuple[int, StatusCode]:
        """Writes data to a formatted I/O write buffer synchronously.

        Corresponds to viBufWrite function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        data : bytes
            Data to be written.

        Returns
        -------
        int
            number of written bytes
        StatusCode
            return value of the library call.

        """
        raise NotImplementedError

    def clear(self, session: VISASession) -> StatusCode:
        """Clears a device.

        Corresponds to viClear function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def close(
        self, session: Union[VISASession, VISAEventContext, VISARMSession]
    ) -> StatusCode:
        """Closes the specified session, event, or find list.

        Corresponds to viClose function of the VISA library.

        Parameters
        ---------
        session : Union[VISASession, VISAEventContext, VISARMSession]
            Unique logical identifier to a session, event, resource manager.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def disable_event(
        self,
        session: VISASession,
        event_type: constants.EventType,
        mechanism: constants.EventMechanism,
    ) -> StatusCode:
        """Disable notification for an event type(s) via the specified mechanism(s).

        Corresponds to viDisableEvent function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        event_type : constants.EventType
            Event type.
        mechanism : constants.EventMechanism
            Event handling mechanisms to be disabled.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def discard_events(
        self,
        session: VISASession,
        event_type: constants.EventType,
        mechanism: constants.EventMechanism,
    ) -> StatusCode:
        """Discard event occurrences for a given type and mechanisms in a session.

        Corresponds to viDiscardEvents function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        event_type : constants.EventType
            Logical event identifier.
        mechanism : constants.EventMechanism
            Specifies event handling mechanisms to be discarded.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def enable_event(
        self,
        session: VISASession,
        event_type: constants.EventType,
        mechanism: constants.EventMechanism,
        context: None = None,
    ) -> StatusCode:
        """Enable event occurrences for specified event types and mechanisms in a session.

        Corresponds to viEnableEvent function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        event_type : constants.EventType
            Logical event identifier.
        mechanism : constants.EventMechanism
            Specifies event handling mechanisms to be enabled.
        context : None, optional
            Unused parameter...

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def flush(
        self, session: VISASession, mask: constants.BufferOperation
    ) -> StatusCode:
        """Flush the specified buffers.

        The buffers can be associated with formatted I/O operations and/or
        serial communication.

        Corresponds to viFlush function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        mask : constants.BufferOperation
            Specifies the action to be taken with flushing the buffer.
            The values can be combined using the | operator. However multiple
            operations on a single buffer cannot be combined.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def get_attribute(
        self,
        session: Union[VISASession, VISAEventContext, VISARMSession],
        attribute: Union[constants.ResourceAttribute, constants.EventAttribute],
    ) -> Tuple[Any, StatusCode]:
        """Retrieves the state of an attribute.

        Corresponds to viGetAttribute function of the VISA library.

        Parameters
        ----------
        session : Union[VISASession, VISAEventContext]
            Unique logical identifier to a session, event, or find list.
        attribute : Union[constants.ResourceAttribute, constants.EventAttribute]
            Resource or event attribute for which the state query is made.

        Returns
        -------
        Any
            State of the queried attribute for a specified resource
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def gpib_command(self, session: VISASession, data: bytes) -> Tuple[int, StatusCode]:
        """Write GPIB command bytes on the bus.

        Corresponds to viGpibCommand function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        data : bytes
            Data to write.

        Returns
        -------
        int
            Number of written bytes
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def gpib_control_atn(
        self, session: VISASession, mode: constants.ATNLineOperation
    ) -> StatusCode:
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
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def gpib_control_ren(
        self, session: VISASession, mode: constants.RENLineOperation
    ) -> StatusCode:
        """Controls the state of the GPIB Remote Enable (REN) interface line.

        Optionally the remote/local state of the device can also be set.

        Corresponds to viGpibControlREN function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        mode : constants.RENLineOperation
            State of the REN line and optionally the device remote/local state.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def gpib_pass_control(
        self, session: VISASession, primary_address: int, secondary_address: int
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
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def gpib_send_ifc(self, session: VISASession) -> StatusCode:
        """Pulse the interface clear line (IFC) for at least 100 microseconds.

        Corresponds to viGpibSendIFC function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def in_8(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        extended: bool = False,
    ) -> Tuple[int, StatusCode]:
        """Reads in an 8-bit value from the specified memory space and offset.

        Corresponds to viIn8* function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Specifies the address space.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        extended : bool, optional
            Use 64 bits offset independent of the platform, False by default.

        Returns
        -------
        int
            Data read from memory
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def in_16(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        extended: bool = False,
    ) -> Tuple[int, StatusCode]:
        """Reads in an 16-bit value from the specified memory space and offset.

        Corresponds to viIn16* function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Specifies the address space.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        extended : bool, optional
            Use 64 bits offset independent of the platform, False by default.

        Returns
        -------
        int
            Data read from memory
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def in_32(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        extended: bool = False,
    ) -> Tuple[int, StatusCode]:
        """Reads in an 32-bit value from the specified memory space and offset.

        Corresponds to viIn32* function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Specifies the address space.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        extended : bool, optional
            Use 64 bits offset independent of the platform, False by default.

        Returns
        -------
        int
            Data read from memory
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def in_64(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        extended: bool = False,
    ) -> Tuple[int, StatusCode]:
        """Reads in an 64-bit value from the specified memory space and offset.

        Corresponds to viIn64* function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Specifies the address space.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        extended : bool, optional
            Use 64 bits offset independent of the platform, False by default.

        Returns
        -------
        int
            Data read from memory
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def install_handler(
        self,
        session: VISASession,
        event_type: constants.EventType,
        handler: VISAHandler,
        user_handle: Any,
    ) -> Tuple[VISAHandler, Any, Any, StatusCode]:
        """Install handlers for event callbacks.

        Corresponds to viInstallHandler function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        event_type : constants.EventType
            Logical event identifier.
        handler : VISAHandler
            Reference to a handler to be installed by a client application.
        user_handle : Any
            Value specified by an application that can be used for identifying
            handlers uniquely for an event type.

        Returns
        -------
        handler : VISAHandler
            Handler to be installed by a client application.
        converted_user_handle :
            Converted user handle to match the underlying library. This version
            of the handle should be used in further call to the library.
        converted_handler :
            Converted version of the handler satisfying to backend library.
        status_code : StatusCode
            Return value of the library call

        """
        raise NotImplementedError

    def list_resources(
        self, session: VISARMSession, query: str = "?*::INSTR"
    ) -> Tuple[str, ...]:
        """Return a tuple of all connected devices matching query.

        Parameters
        ----------
        session : VISARMSession
            Unique logical identifier to the resource manager session.
        query : str
            Regular expression used to match devices.

        Returns
        -------
        Tuple[str, ...]
            Resource names of all the connected devices matching the query.

        """
        raise NotImplementedError

    def lock(
        self,
        session: VISASession,
        lock_type: constants.Lock,
        timeout: int,
        requested_key: Optional[str] = None,
    ) -> Tuple[str, StatusCode]:
        """Establishes an access mode to the specified resources.

        Corresponds to viLock function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
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
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def map_address(
        self,
        session: VISASession,
        map_space: constants.AddressSpace,
        map_base: int,
        map_size: int,
        access: Literal[False] = False,
        suggested: Optional[int] = None,
    ) -> Tuple[int, StatusCode]:
        """Maps the specified memory space into the process's address space.

        Corresponds to viMapAddress function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        map_space : constants.AddressSpace
            Specifies the address space to map.
        map_base : int
            Offset (in bytes) of the memory to be mapped.
        map_size : int
            Amount of memory to map (in bytes).
        access : False
            Unused parameter.
        suggested : Optional[int], optional
            If not None, the operating system attempts to map the memory to the
            address specified. There is no guarantee, however, that the memory
            will be mapped to that address. This operation may map the memory
            into an address region different from the suggested one.

        Returns
        -------
        int
            Address in your process space where the memory was mapped
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def map_trigger(
        self,
        session: VISASession,
        trigger_source: constants.InputTriggerLine,
        trigger_destination: constants.OutputTriggerLine,
        mode: None = None,
    ) -> StatusCode:
        """Map the specified trigger source line to the specified destination line.

        Corresponds to viMapTrigger function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        trigger_source : constants.InputTriggerLine
            Source line from which to map.
        trigger_destination : constants.OutputTriggerLine
            Destination line to which to map.
        mode : None, optional
            Always None for this version of the VISA specification.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def memory_allocation(
        self, session: VISASession, size: int, extended: bool = False
    ) -> Tuple[int, StatusCode]:
        """Allocate memory from a resource's memory region.

        Corresponds to viMemAlloc* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        size : int
            Specifies the size of the allocation.
        extended : bool, optional
            Use 64 bits offset independent of the platform.

        Returns
        -------
        int
            offset of the allocated memory
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def memory_free(
        self, session: VISASession, offset: int, extended: bool = False
    ) -> StatusCode:
        """Frees memory previously allocated using the memory_allocation() operation.

        Corresponds to viMemFree* function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        offset : int
            Offset of the memory to free.
        extended : bool, optional
            Use 64 bits offset independent of the platform.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def move(
        self,
        session: VISASession,
        source_space: constants.AddressSpace,
        source_offset: int,
        source_width: constants.DataWidth,
        destination_space: constants.AddressSpace,
        destination_offset: int,
        destination_width: constants.DataWidth,
        length: int,
    ) -> StatusCode:
        """Moves a block of data.

        Corresponds to viMove function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        source_space : constants.AddressSpace
            Specifies the address space of the source.
        source_offset : int
            Offset of the starting address or register from which to read.
        source_width : constants.DataWidth
            Specifies the data width of the source.
        destination_space : constants.AddressSpace
            Specifies the address space of the destination.
        destination_offset : int
            Offset of the starting address or register to which to write.
        destination_width : constants.DataWidth
            Specifies the data width of the destination.
        length: int
            Number of elements to transfer, where the data width of the
            elements to transfer is identical to the source data width.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def move_asynchronously(
        self,
        session: VISASession,
        source_space: constants.AddressSpace,
        source_offset: int,
        source_width: constants.DataWidth,
        destination_space: constants.AddressSpace,
        destination_offset: int,
        destination_width: constants.DataWidth,
        length: int,
    ) -> Tuple[VISAJobID, StatusCode]:
        """Moves a block of data asynchronously.

        Corresponds to viMoveAsync function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        source_space : constants.AddressSpace
            Specifies the address space of the source.
        source_offset : int
            Offset of the starting address or register from which to read.
        source_width : constants.DataWidth
            Specifies the data width of the source.
        destination_space : constants.AddressSpace
            Specifies the address space of the destination.
        destination_offset : int
            Offset of the starting address or register to which to write.
        destination_width : constants.DataWidth
            Specifies the data width of the destination.
        length : int
            Number of elements to transfer, where the data width of the
            elements to transfer is identical to the source data width.

        Returns
        -------
        VISAJobID
            Job identifier of this asynchronous move operation
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def move_in_8(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        extended: bool = False,
    ) -> Tuple[List[int], StatusCode]:
        """Moves an 8-bit block of data to local memory.

        Corresponds to viMoveIn8* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space from which to move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
             Number of elements to transfer, where the data width of
             the elements to transfer is identical to the source data width.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        data : List[int]
            Data read from the bus
        status_code : StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def move_in_16(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        extended: bool = False,
    ) -> Tuple[List[int], StatusCode]:
        """Moves an 16-bit block of data to local memory.

        Corresponds to viMoveIn816 functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space from which to move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
             Number of elements to transfer, where the data width of
             the elements to transfer is identical to the source data width.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        data : List[int]
            Data read from the bus
        status_code : StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def move_in_32(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        extended: bool = False,
    ) -> Tuple[List]:
        """Moves an 32-bit block of data to local memory.

        Corresponds to viMoveIn32* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space from which to move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
             Number of elements to transfer, where the data width of
             the elements to transfer is identical to the source data width.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        data : List[int]
            Data read from the bus
        status_code : StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def move_in_64(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        extended: bool = False,
    ) -> Tuple[List[int], StatusCode]:
        """Moves an 64-bit block of data to local memory.

        Corresponds to viMoveIn8* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space from which to move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
             Number of elements to transfer, where the data width of
             the elements to transfer is identical to the source data width.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        data : List[int]
            Data read from the bus
        status_code : StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def move_out_8(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        data: Iterable[int],
        extended: bool = False,
    ) -> StatusCode:
        """Moves an 8-bit block of data from local memory.

        Corresponds to viMoveOut8* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space into which move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
            Number of elements to transfer, where the data width of
            the elements to transfer is identical to the source data width.
        data : Iterable[int]
            Data to write to bus.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def move_out_16(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        data: Iterable[int],
        extended: bool = False,
    ) -> StatusCode:
        """Moves an 16-bit block of data from local memory.

        Corresponds to viMoveOut16* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space into which move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
            Number of elements to transfer, where the data width of
            the elements to transfer is identical to the source data width.
        data : Iterable[int]
            Data to write to bus.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def move_out_32(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        data: Iterable[int],
        extended: bool = False,
    ) -> StatusCode:
        """Moves an 32-bit block of data from local memory.

        Corresponds to viMoveOut32* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space into which move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
            Number of elements to transfer, where the data width of
            the elements to transfer is identical to the source data width.
        data : Iterable[int]
            Data to write to bus.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        StatusCode
            Return value of the library call.


        """
        raise NotImplementedError

    def move_out_64(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        length: int,
        data: Iterable[int],
        extended: bool = False,
    ) -> StatusCode:
        """Moves an 64-bit block of data from local memory.

        Corresponds to viMoveOut64* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space into which move the data.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        length : int
            Number of elements to transfer, where the data width of
            the elements to transfer is identical to the source data width.
        data : Iterable[int]
            Data to write to bus.
        extended : bool, optional
            Use 64 bits offset independent of the platform, by default False.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def open(
        self,
        session: VISARMSession,
        resource_name: str,
        access_mode: constants.AccessModes = constants.AccessModes.no_lock,
        open_timeout: int = constants.VI_TMO_IMMEDIATE,
    ) -> Tuple[VISASession, StatusCode]:
        """Opens a session to the specified resource.

        Corresponds to viOpen function of the VISA library.

        Parameters
        ----------
        session : VISARMSession
            Resource Manager session (should always be a session returned from
            open_default_resource_manager()).
        resource_name : str
            Unique symbolic name of a resource.
        access_mode : constants.AccessModes, optional
            Specifies the mode by which the resource is to be accessed.
        open_timeout : int
            If the ``access_mode`` parameter requests a lock, then this
            parameter specifies the absolute time period (in milliseconds) that
            the resource waits to get unlocked before this operation returns an
            error.

        Returns
        -------
        VISASession
            Unique logical identifier reference to a session
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def open_default_resource_manager(self) -> Tuple[VISARMSession, StatusCode]:
        """This function returns a session to the Default Resource Manager resource.

        Corresponds to viOpenDefaultRM function of the VISA library.

        Returns
        -------
        VISARMSession
            Unique logical identifier to a Default Resource Manager session
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def out_8(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        data: int,
        extended: bool = False,
    ) -> StatusCode:
        """Write an 8-bit value to the specified memory space and offset.

        Corresponds to viOut8* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space into which to write.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        data : int
            Data to write to bus.
        extended : bool, optional
            Use 64 bits offset independent of the platform.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def out_16(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        data: int,
        extended: bool = False,
    ) -> StatusCode:
        """Write a 16-bit value to the specified memory space and offset.

        Corresponds to viOut16* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space into which to write.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        data : int
            Data to write to bus.
        extended : bool, optional
            Use 64 bits offset independent of the platform.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def out_32(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        data: Iterable[int],
        extended: bool = False,
    ) -> StatusCode:
        """Write a 32-bit value to the specified memory space and offset.

        Corresponds to viOut32* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space into which to write.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        data : int
            Data to write to bus.
        extended : bool, optional
            Use 64 bits offset independent of the platform.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def out_64(
        self,
        session: VISASession,
        space: constants.AddressSpace,
        offset: int,
        data: Iterable[int],
        extended: bool = False,
    ) -> StatusCode:
        """Write a 64-bit value to the specified memory space and offset.

        Corresponds to viOut64* functions of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        space : constants.AddressSpace
            Address space into which to write.
        offset : int
            Offset (in bytes) of the address or register from which to read.
        data : int
            Data to write to bus.
        extended : bool, optional
            Use 64 bits offset independent of the platform.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def parse_resource(
        self, session: VISARMSession, resource_name: str
    ) -> Tuple[ResourceInfo, StatusCode]:
        """Parse a resource string to get the interface information.

        Corresponds to viParseRsrc function of the VISA library.

        Parameters
        ----------
        session : VISARMSession
            Resource Manager session (should always be the Default Resource
            Manager for VISA returned from open_default_resource_manager()).
        resource_name : str
             Unique symbolic name of a resource.

        Returns
        -------
        ResourceInfo
            Resource information with interface type and board number
        StatusCode
            Return value of the library call.

        """
        ri, status = self.parse_resource_extended(session, resource_name)
        if status == StatusCode.success:
            return (
                ResourceInfo(
                    ri.interface_type, ri.interface_board_number, None, None, None
                ),
                StatusCode.success,
            )
        else:
            return ri, status

    def parse_resource_extended(
        self, session: VISARMSession, resource_name: str
    ) -> Tuple[ResourceInfo, StatusCode]:
        """Parse a resource string to get extended interface information.

        Corresponds to viParseRsrcEx function of the VISA library.

        Parameters
        ----------
        session : VISARMSession
            Resource Manager session (should always be the Default Resource
            Manager for VISA returned from open_default_resource_manager()).
        resource_name : str
             Unique symbolic name of a resource.

        Returns
        -------
        ResourceInfo
            Resource information with interface type and board number
        StatusCode
            Return value of the library call.

        """
        try:
            parsed = rname.parse_resource_name(resource_name)
        except ValueError:
            return (
                ResourceInfo(constants.InterfaceType.unknown, 0, None, None, None),
                StatusCode.error_invalid_resource_name,
            )

        board_number: Optional[int]
        try:
            # We can only get concrete classes which have one of those attributes
            board_number = int(
                parsed.board  # type: ignore
                if hasattr(parsed, "board")
                else parsed.interface  # type: ignore
            )
        # In some cases the board number may not be convertible to an int
        # PyVISA-py serial resources on Linux for example. For VICP, there is
        # no such attribute.
        except (ValueError, AttributeError):
            board_number = None

        return (
            ResourceInfo(
                parsed.interface_type_const,
                board_number,
                parsed.resource_class,
                str(parsed),
                None,
            ),
            StatusCode.success,
        )

    def peek_8(
        self, session: VISASession, address: VISAMemoryAddress
    ) -> Tuple[int, StatusCode]:
        """Read an 8-bit value from the specified address.

        Corresponds to viPeek8 function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        address : VISAMemoryAddress
            Source address to read the value.

        Returns
        -------
        int
            Data read from bus
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def peek_16(
        self, session: VISASession, address: VISAMemoryAddress
    ) -> Tuple[int, StatusCode]:
        """Read an 16-bit value from the specified address.

        Corresponds to viPeek16 function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        address : VISAMemoryAddress
            Source address to read the value.

        Returns
        -------
        int
            Data read from bus
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def peek_32(
        self, session: VISASession, address: VISAMemoryAddress
    ) -> Tuple[int, StatusCode]:
        """Read an 32-bit value from the specified address.

        Corresponds to viPeek32 function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        address : VISAMemoryAddress
            Source address to read the value.

        Returns
        -------
        int
            Data read from bus
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def peek_64(
        self, session: VISASession, address: VISAMemoryAddress
    ) -> Tuple[int, StatusCode]:
        """Read an 64-bit value from the specified address.

        Corresponds to viPeek64 function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        address : VISAMemoryAddress
            Source address to read the value.

        Returns
        -------
        int
            Data read from bus
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def poke_8(
        self, session: VISASession, address: VISAMemoryAddress, data: int
    ) -> StatusCode:
        """Write an 8-bit value to the specified address.

        Corresponds to viPoke8 function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        address : VISAMemoryAddress
            Source address to read the value.
        data : int
            Data to write.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def poke_16(
        self, session: VISASession, address: VISAMemoryAddress, data: int
    ) -> StatusCode:
        """Write an 16-bit value to the specified address.

        Corresponds to viPoke16 function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        address : VISAMemoryAddress
            Source address to read the value.
        data : int
            Data to write.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def poke_32(
        self, session: VISASession, address: VISAMemoryAddress, data: int
    ) -> StatusCode:
        """Write an 32-bit value to the specified address.

        Corresponds to viPoke32 function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        address : VISAMemoryAddress
            Source address to read the value.
        data : int
            Data to write.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def poke_64(
        self, session: VISASession, address: VISAMemoryAddress, data: int
    ) -> StatusCode:
        """Write an 64-bit value to the specified address.

        Corresponds to viPoke64 function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        address : VISAMemoryAddress
            Source address to read the value.
        data : int
            Data to write.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def read(self, session: VISASession, count: int) -> Tuple[bytes, StatusCode]:
        """Reads data from device or interface synchronously.

        Corresponds to viRead function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        count : int
            Number of bytes to be read.

        Returns
        -------
        bytes
            Date read
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def read_asynchronously(
        self, session: VISASession, count: int
    ) -> Tuple[SupportsBytes, VISAJobID, StatusCode]:
        """Reads data from device or interface asynchronously.

        Corresponds to viReadAsync function of the VISA library. Since the
        asynchronous operation may complete before the function call return
        implementation should make sure that get_buffer_from_id will be able
        to return the proper buffer before this method returns.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        count : int
            Number of bytes to be read.

        Returns
        -------
        SupportsBytes
            Buffer that will be filled during the asynchronous operation.
        VISAJobID
            Id of the asynchronous job
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def get_buffer_from_id(self, job_id: VISAJobID) -> Optional[SupportsBytes]:
        """Retrieve the buffer associated with a job id created in read_asynchronously

        Parameters
        ----------
        job_id : VISAJobID
            Id of the job for which to retrieve the buffer.

        Returns
        -------
        Optional[SupportsBytes]
            Buffer in which the data are stored or None if the job id is not
            associated with any job.

        """
        raise NotImplementedError

    def read_stb(self, session: VISASession) -> Tuple[int, StatusCode]:
        """Reads a status byte of the service request.

        Corresponds to viReadSTB function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.

        Returns
        -------
        int
            Service request status byte
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def read_to_file(
        self, session: VISASession, filename: str, count: int
    ) -> Tuple[int, StatusCode]:
        """Read data synchronously, and store the transferred data in a file.

        Corresponds to viReadToFile function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        filename : str
            Name of file to which data will be written.
        count : int
            Number of bytes to be read.

        Returns
        -------
        int
            Number of bytes actually transferred
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def set_attribute(
        self,
        session: VISASession,
        attribute: constants.ResourceAttribute,
        attribute_state: Any,
    ) -> StatusCode:
        """Set the state of an attribute.

        Corresponds to viSetAttribute function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        attribute : constants.ResourceAttribute
            Attribute for which the state is to be modified.
        attribute_state : Any
            The state of the attribute to be set for the specified object.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def set_buffer(
        self, session: VISASession, mask: constants.BufferType, size: int
    ) -> StatusCode:
        """Set the size for the formatted I/O and/or low-level I/O communication buffer(s).

        Corresponds to viSetBuf function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        mask : constants.BufferType
            Specifies the type of buffer.
        size : int
            The size to be set for the specified buffer(s).

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def status_description(
        self, session: VISASession, status: StatusCode
    ) -> Tuple[str, StatusCode]:
        """Return a user-readable description of the status code passed to the operation.

        Corresponds to viStatusDesc function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        status : StatusCode
            Status code to interpret.

        Returns
        -------
        str
            User-readable string interpretation of the status code.
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def terminate(
        self, session: VISASession, degree: None, job_id: VISAJobID
    ) -> StatusCode:
        """Request a VISA session to terminate normal execution of an operation.

        Corresponds to viTerminate function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        degree : None
            Not used in this version of the VISA specification.
        job_id : VISAJobId
            Specifies an operation identifier. If a user passes None as the
            job_id value to viTerminate(), a VISA implementation should abort
            any calls in the current process executing on the specified vi.
            Any call that is terminated this way should return VI_ERROR_ABORT.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def uninstall_handler(
        self,
        session: VISASession,
        event_type: constants.EventType,
        handler: VISAHandler,
        user_handle: Any = None,
    ) -> StatusCode:
        """Uninstall handlers for events.

        Corresponds to viUninstallHandler function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        event_type : constants.EventType
            Logical event identifier.
        handler : VISAHandler
            Handler to be uninstalled by a client application.
        user_handle:
            A value specified by an application that can be used for
            identifying handlers uniquely in a session for an event.
            The modified value of the user_handle as returned by install_handler
            should be used instead of the original value.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def unlock(self, session: VISASession) -> StatusCode:
        """Relinquish a lock for the specified resource.

        Corresponds to viUnlock function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def unmap_address(self, session: VISASession) -> StatusCode:
        """Unmap memory space previously mapped by map_address().

        Corresponds to viUnmapAddress function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def unmap_trigger(
        self,
        session: VISASession,
        trigger_source: constants.InputTriggerLine,
        trigger_destination: constants.OutputTriggerLine,
    ) -> StatusCode:
        """Undo a previous map between a trigger source line and a destination line.

        Corresponds to viUnmapTrigger function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        trigger_source : constants.InputTriggerLine
            Source line used in previous map.
        trigger_destination : constants.OutputTriggerLine
            Destination line used in previous map.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def usb_control_in(
        self,
        session: VISASession,
        request_type_bitmap_field: int,
        request_id: int,
        request_value: int,
        index: int,
        length: int = 0,
    ) -> Tuple[bytes, StatusCode]:
        """Perform a USB control pipe transfer from the device.

        Corresponds to viUsbControlIn function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        request_type_bitmap_field : int
            bmRequestType parameter of the setup stage of a USB control transfer.
        request_id : int
            bRequest parameter of the setup stage of a USB control transfer.
        request_value : int
            wValue parameter of the setup stage of a USB control transfer.
        index : int
            wIndex parameter of the setup stage of a USB control transfer.
            This is usually the index of the interface or endpoint.
        length : int, optional
            wLength parameter of the setup stage of a USB control transfer.
            This value also specifies the size of the data buffer to receive
            the data from the optional data stage of the control transfer.

        Returns
        -------
        bytes
            The data buffer that receives the data from the optional data stage
            of the control transfer
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def usb_control_out(
        self,
        session: VISASession,
        request_type_bitmap_field: int,
        request_id: int,
        request_value: int,
        index: int,
        data: bytes = b"",
    ) -> StatusCode:
        """Perform a USB control pipe transfer to the device.

        Corresponds to viUsbControlOut function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        request_type_bitmap_field : int
            bmRequestType parameter of the setup stage of a USB control transfer.
        request_id : int
            bRequest parameter of the setup stage of a USB control transfer.
        request_value : int
            wValue parameter of the setup stage of a USB control transfer.
        index : int
            wIndex parameter of the setup stage of a USB control transfer.
            This is usually the index of the interface or endpoint.
        data : bytes, optional
            The data buffer that sends the data in the optional data stage of
            the control transfer.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def vxi_command_query(
        self, session: VISASession, mode: constants.VXICommands, command: int
    ) -> Tuple[int, StatusCode]:
        """Send the device a miscellaneous command or query and/or retrieves the response to a previous query.

        Corresponds to viVxiCommandQuery function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        mode : constants.VXICommands
            Specifies whether to issue a command and/or retrieve a response.
        command : int
            The miscellaneous command to send.

        Returns
        -------
        int
            The response retrieved from the device
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def wait_on_event(
        self, session: VISASession, in_event_type: constants.EventType, timeout: int
    ) -> Tuple[constants.EventType, VISAEventContext, StatusCode]:
        """Wait for an occurrence of the specified event for a given session.

        Corresponds to viWaitOnEvent function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        in_event_type : constants.EventType
            Logical identifier of the event(s) to wait for.
        timeout : int
            Absolute time period in time units that the resource shall wait for
            a specified event to occur before returning the time elapsed error.
            The time unit is in milliseconds.

        Returns
        -------
        constants.EventType
            Logical identifier of the event actually received
        VISAEventContext
            A handle specifying the unique occurrence of an event
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def write(self, session: VISASession, data: bytes) -> Tuple[int, StatusCode]:
        """Write data to device or interface synchronously.

        Corresponds to viWrite function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
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

    def write_asynchronously(
        self, session: VISASession, data: bytes
    ) -> Tuple[VISAJobID, StatusCode]:
        """Write data to device or interface asynchronously.

        Corresponds to viWriteAsync function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        data : bytes
            Data to be written.

        Returns
        -------
        VISAJobID
            Job ID of this asynchronous write operation
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError

    def write_from_file(
        self, session: VISASession, filename: str, count: int
    ) -> Tuple[int, StatusCode]:
        """Take data from a file and write it out synchronously.

        Corresponds to viWriteFromFile function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        filename : str
            Name of file from which data will be read.
        count : int
            Number of bytes to be written.

        Returns
        -------
        int
            Number of bytes actually transferred
        StatusCode
            Return value of the library call.

        """
        raise NotImplementedError


def list_backends() -> List[str]:
    """Return installed backends.

    Backends are installed python packages named pyvisa_<something> where <something>
    is the name of the backend.

    """
    return ["ivi"] + [
        name[7:]
        for (loader, name, ispkg) in pkgutil.iter_modules()
        if (name.startswith("pyvisa_") or name.startswith("pyvisa-"))
        and not name.endswith("-script")
    ]


#: Maps backend name to VisaLibraryBase derived class
_WRAPPERS: Dict[str, Type[VisaLibraryBase]] = {}


class PyVISAModule(ModuleType):
    WRAPPER_CLASS: Type[VisaLibraryBase]


def get_wrapper_class(backend_name: str) -> Type[VisaLibraryBase]:
    """Return the WRAPPER_CLASS for a given backend."""
    try:
        return _WRAPPERS[backend_name]
    except KeyError:
        if backend_name == "ivi":
            from .ctwrapper import IVIVisaLibrary

            _WRAPPERS["ivi"] = IVIVisaLibrary
            return IVIVisaLibrary

    pkg: PyVISAModule
    try:
        pkg = cast(PyVISAModule, import_module("pyvisa_" + backend_name))
        _WRAPPERS[backend_name] = cls = pkg.WRAPPER_CLASS
        return cls
    except ImportError:
        raise ValueError("Wrapper not found: No package named pyvisa_%s" % backend_name)


def _get_default_wrapper() -> str:
    """Return an available default VISA wrapper as a string ('ivi' or 'py').

    Use IVI if the binary is found, else try to use pyvisa-py.

    'ni' VISA wrapper is NOT used since version > 1.10.0
    and will be removed in 1.12

    Raises
    ------
    ValueError
        If no backend can be found

    """

    from .ctwrapper import IVIVisaLibrary

    ivi_binary_found = bool(IVIVisaLibrary.get_library_paths())
    if ivi_binary_found:
        logger.debug("The IVI implementation available")
        return "ivi"
    else:
        logger.debug("Did not find IVI binary")

    try:
        get_wrapper_class("py")  # check for pyvisa-py availability
        logger.debug("pyvisa-py is available.")
        return "py"
    except ValueError:
        logger.debug("Did not find pyvisa-py package")
    raise ValueError(
        "Could not locate a VISA implementation. Install either the IVI binary or pyvisa-py."
    )


def open_visa_library(specification: str = "") -> VisaLibraryBase:
    """Helper function to create a VISA library wrapper.

    In general, you should not use the function directly. The VISA library
    wrapper will be created automatically when you create a ResourceManager object.

    Parameters
    ----------
    specification : str, optional
        The specification has 2 parts separated by a '@', ex: path/visa.dll@ivi
        The second part is the backend name (ivi, py, ...) while the first part
        will be passed to the library object. In the case of the ivi backend it
        should the path to the VISA library.
        Either part can be omitted. If the second part is omitted, the system
        will fall back to the ivi backend if available or the pyvisa-py backend
        if the first part is also omitted.

    """

    if not specification:
        logger.debug("No visa library specified, trying to find alternatives.")
        try:
            specification = os.environ["PYVISA_LIBRARY"]
        except KeyError:
            logger.debug("Environment variable PYVISA_LIBRARY is unset.")

    wrapper: Optional[str]
    try:
        argument, wrapper = specification.rsplit("@", 1)
    except ValueError:
        argument = specification
        wrapper = None  # Flag that we need a fallback, but avoid nested exceptions
    if wrapper is None:
        if argument:  # some filename given
            wrapper = "ivi"
        else:
            wrapper = _get_default_wrapper()

    cls = get_wrapper_class(wrapper)

    try:
        return cls(argument)
    except Exception as e:
        logger.debug("Could not open VISA wrapper %s: %s\n%s", cls, str(argument), e)
        raise


class ResourceManager(object):
    """VISA Resource Manager."""

    #: Maps (Interface Type, Resource Class) to Python class encapsulating that resource.
    _resource_classes: ClassVar[
        Dict[Tuple[constants.InterfaceType, str], Type["Resource"]]
    ] = {}

    #: Session handler for the resource manager.
    _session: Optional[VISARMSession] = None

    #: Reference to the VISA library used by the ResourceManager
    visalib: VisaLibraryBase

    #: Resources created by this manager to allow closing them when the manager is closed
    _created_resources: WeakSet

    #: Handler for atexit using a weakref to close
    _atexit_handler: Callable

    @classmethod
    def register_resource_class(
        cls,
        interface_type: constants.InterfaceType,
        resource_class: str,
        python_class: Type["Resource"],
    ) -> None:
        """Register a class for a specific interface type, resource class pair.

        Parameters
        ----------
        interface_type : constants.InterfaceType
            Interface type for which to use the provided class.
        resource_class : str
            Resource class (INSTR, INTFC, ...)  for which to use the provided class.
        python_class : Type[Resource]
            Subclass of ``Resource`` to use when opening a resource matching the
            specified interface type and resource class.

        """
        if (interface_type, resource_class) in cls._resource_classes:
            logger.warning(
                "%s is already registered in the ResourceManager. "
                "Overwriting with %s" % ((interface_type, resource_class), python_class)
            )

        # If the class already has this attribute, it means that a parent class
        # was registered first. We need to copy the current set and extend it.
        attrs: Set[Type[attributes.Attribute]] = copy.copy(
            getattr(python_class, "visa_attributes_classes", set())
        )

        for attr in chain(
            attributes.AttributesPerResource[(interface_type, resource_class)],
            attributes.AttributesPerResource[attributes.AllSessionTypes],
        ):
            attrs.add(attr)
            # Error on non-properly set descriptor (this ensures that we are
            # consistent)
            if attr.py_name != "" and not hasattr(python_class, attr.py_name):
                raise TypeError(
                    "%s was expected to have a visa attribute %s"
                    % (python_class, attr.py_name)
                )

        setattr(python_class, "visa_attributes_classes", attrs)

        cls._resource_classes[(interface_type, resource_class)] = python_class

    def __new__(
        cls: Type["ResourceManager"], visa_library: Union[str, VisaLibraryBase] = ""
    ) -> "ResourceManager":
        """Create a new resource manager tied to the specified VISA library.

        Parameters
        ----------
        visa_library : Union[str, VisaLibraryBase]
            Either a fully initialized VisaLibraryBase subclass instance or
            a str specification (see open_visa_library for the format).

        """
        if not isinstance(visa_library, VisaLibraryBase):
            visa_library = open_visa_library(visa_library)

        if visa_library.resource_manager is not None:
            obj = visa_library.resource_manager
            logger.debug("Reusing ResourceManager with session %s", obj.session)
            return obj

        obj = super(ResourceManager, cls).__new__(cls)

        obj.session, _err = visa_library.open_default_resource_manager()

        obj.visalib = visa_library
        obj.visalib.resource_manager = obj
        obj._created_resources = WeakSet()

        # Register an atexit handler to ensure the Resource Manager is properly
        # closed.
        close_ref = WeakMethod(obj.close)  # type: ignore

        def call_close():
            try:
                meth = close_ref()
                if meth:
                    meth()
            except Exception:
                # Suppress exceptions during exit
                logger.warning(
                    "Exception suppressed while closing a ResourceManager at system exit",
                    exc_info=True,
                )

        atexit.register(call_close)
        obj._atexit_handler = call_close  # type: ignore

        logger.debug("Created ResourceManager with session %s", obj.session)
        return obj

    @property
    def session(self) -> VISARMSession:
        """Resource Manager session handle.

        Raises
        ------
        errors.InvalidSession
            Raised if the session is closed.

        """
        if self._session is None:
            raise errors.InvalidSession()
        return self._session

    @session.setter
    def session(self, value: Optional[VISARMSession]) -> None:
        self._session = value

    def __str__(self) -> str:
        return "Resource Manager of %s" % self.visalib

    def __repr__(self) -> str:
        return "<ResourceManager(%r)>" % self.visalib

    def __del__(self) -> None:
        if self._session is not None:
            self.close()

    def ignore_warning(self, *warnings_constants: StatusCode) -> ContextManager:
        """Ignoring warnings context manager for the current resource.

        Parameters
        ----------
        warnings_constants : StatusCode
            Constants identifying the warnings to ignore.

        """
        return self.visalib.ignore_warning(self.session, *warnings_constants)

    @property
    def last_status(self) -> StatusCode:
        """Last status code returned for an operation with this Resource Manager."""
        return self.visalib.get_last_status_in_session(self.session)

    def close(self) -> None:
        """Close the resource manager session.

        Notes
        -----
        Since the resource manager session is shared between instances
        this will also terminate connections obtained from other
        ResourceManager instances.

        """
        atexit.unregister(self._atexit_handler)
        try:
            logger.debug("Closing ResourceManager (session: %s)", self.session)
            # Cleanly close all resources when closing the manager.
            for resource in self._created_resources:
                resource.close()
            self.visalib.close(self.session)
            # mypy don't get that we can set a value we cannot get
            self.session = None  # type: ignore
            self.visalib.resource_manager = None
        except errors.InvalidSession:
            pass

    def list_resources(self, query: str = "?*::INSTR") -> Tuple[str, ...]:
        r"""Return a tuple of all connected devices matching query.

        Notes
        -----
        The query uses the VISA Resource Regular Expression syntax - which is
        not the same as the Python regular expression syntax. (see below)

        The VISA Resource Regular Expression syntax is defined in the VISA
        Library specification:
        http://www.ivifoundation.org/docs/vpp43.pdf

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

        Thus the default query, '?*::INSTR', matches any sequences of characters
        ending ending with '::INSTR'.

        On some platforms, devices that are already open are not returned.

        """

        return self.visalib.list_resources(self.session, query)

    def list_resources_info(self, query: str = "?*::INSTR") -> Dict[str, ResourceInfo]:
        """Get extended information about all connected devices matching query.

        For details of the VISA Resource Regular Expression syntax used in query,
        refer to list_resources().

        Returns
        -------
        Dict[str, ResourceInfo]
            Mapping of resource name to ResourceInfo

        """

        return {
            resource: self.resource_info(resource)
            for resource in self.list_resources(query)
        }

    def list_opened_resources(self) -> List["Resource"]:
        """Return a list of all the opened resources."""
        opened = []
        for resource in self._created_resources:
            try:
                resource.session
            except errors.InvalidSession:
                pass
            else:
                opened.append(resource)
        return opened

    def resource_info(self, resource_name: str, extended: bool = True) -> ResourceInfo:
        """Get the (extended) information of a particular resource.

        Parameters
        ----------
        resource_name : str
            Unique symbolic name of a resource.
        extended : bool, optional
            Also get extended information (ie. resource_class, resource_name, alias)

        """

        if extended:
            ret, _err = self.visalib.parse_resource_extended(
                self.session, resource_name
            )
        else:
            ret, _err = self.visalib.parse_resource(self.session, resource_name)

        return ret

    def open_bare_resource(
        self,
        resource_name: str,
        access_mode: constants.AccessModes = constants.AccessModes.no_lock,
        open_timeout: int = constants.VI_TMO_IMMEDIATE,
    ) -> Tuple[VISASession, StatusCode]:
        """Open the specified resource without wrapping into a class.

        Parameters
        ----------
        resource_name : str
            Name or alias of the resource to open.
        access_mode : constants.AccessModes, optional
            Specifies the mode by which the resource is to be accessed,
            by default constants.AccessModes.no_lock
        open_timeout : int, optional
            If the ``access_mode`` parameter requests a lock, then this
            parameter specifies the absolute time period (in milliseconds) that
            the resource waits to get unlocked before this operation returns an
            error, by default constants.VI_TMO_IMMEDIATE.

        Returns
        -------
        VISASession
            Unique logical identifier reference to a session.
        StatusCode
            Return value of the library call.

        """
        return self.visalib.open(self.session, resource_name, access_mode, open_timeout)

    def open_resource(
        self,
        resource_name: str,
        access_mode: constants.AccessModes = constants.AccessModes.no_lock,
        open_timeout: int = constants.VI_TMO_IMMEDIATE,
        resource_pyclass: Optional[Type["Resource"]] = None,
        **kwargs: Any,
    ) -> "Resource":
        """Return an instrument for the resource name.

        Parameters
        ----------
        resource_name : str
            Name or alias of the resource to open.
        access_mode : constants.AccessModes, optional
            Specifies the mode by which the resource is to be accessed,
            by default constants.AccessModes.no_lock
        open_timeout : int, optional
            If the ``access_mode`` parameter requests a lock, then this
            parameter specifies the absolute time period (in milliseconds) that
            the resource waits to get unlocked before this operation returns an
            error, by default constants.VI_TMO_IMMEDIATE.
        resource_pyclass : Optional[Type[Resource]], optional
            Resource Python class to use to instantiate the Resource.
            Defaults to None: select based on the resource name.
        kwargs : Any
            Keyword arguments to be used to change instrument attributes
            after construction.

        Returns
        -------
        Resource
            Subclass of Resource matching the resource.

        """
        if resource_pyclass is None:
            info = self.resource_info(resource_name, extended=True)

            try:
                # When using querying extended resource info the resource_class is not
                # None
                resource_pyclass = self._resource_classes[
                    (info.interface_type, info.resource_class)  # type: ignore
                ]
            except KeyError:
                resource_pyclass = self._resource_classes[
                    (constants.InterfaceType.unknown, "")
                ]
                logger.warning(
                    "There is no class defined for %r. Using Resource",
                    (info.interface_type, info.resource_class),
                )
        if hasattr(self.visalib, "open_resource"):
            res = self.visalib.open_resource(  # type: ignore
                resource_name, access_mode, open_timeout, resource_pyclass, **kwargs
            )
        else:
            res = resource_pyclass(self, resource_name)
            for key in kwargs.keys():
                try:
                    getattr(res, key)
                    present = True
                except AttributeError:
                    present = False
                except errors.InvalidSession:
                    present = True

                if not present:
                    raise ValueError(
                        "%r is not a valid attribute for type %s"
                        % (key, res.__class__.__name__)
                    )

            res.open(access_mode, open_timeout)

            for key, value in kwargs.items():
                setattr(res, key, value)

        self._created_resources.add(res)

        return res
