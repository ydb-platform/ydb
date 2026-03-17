# -*- coding: utf-8 -*-
"""Highlevel wrapper of the VISA Library.


:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import random
from collections import OrderedDict
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Tuple,
    Union,
    cast,
)

from pyvisa import constants, highlevel, rname
from pyvisa.constants import StatusCode
from pyvisa.typing import VISAEventContext, VISARMSession, VISASession
from pyvisa.util import DebugInfo, LibraryPath

from .common import LOGGER
from .sessions import OpenError, Session


class PyVisaLibrary(highlevel.VisaLibraryBase):
    """A pure Python backend for PyVISA.

    The object is basically a dispatcher with some common functions implemented.

    When a new resource object is requested to pyvisa, the library creates a
    Session object (that knows how to perform low-level communication operations)
    associated with a session handle (a number, usually refered just as session).

    A call to a library function is handled by PyVisaLibrary if it involves a
    resource agnostic function or dispatched to the correct session object
    (obtained from the session id).

    Importantly, the user is unaware of this. PyVisaLibrary behaves for
    the user just as NIVisaLibrary.

    """

    #: Live session object identified by a randon session ID
    sessions: MutableMapping[
        Union[VISASession, VISAEventContext, VISARMSession], Session
    ]

    # Try to import packages implementing lower level functionality.
    try:
        from .serial import SerialSession

        LOGGER.debug("SerialSession was correctly imported.")
    except Exception as e:
        LOGGER.debug("SerialSession was not imported %s." % e)

    try:
        from .usb import USBRawSession, USBSession

        LOGGER.debug("USBSession and USBRawSession were correctly imported.")
    except Exception as e:
        LOGGER.debug("USBSession and USBRawSession were not imported %s." % e)

    try:
        from .tcpip import TCPIPInstrSession, TCPIPSocketSession

        LOGGER.debug("TCPIPSession was correctly imported.")
    except Exception as e:
        LOGGER.debug("TCPIPSession was not imported %s." % e)

    try:
        from . import prologix

        if hasattr(prologix, "PrologixASRLIntfcSession"):
            ss = "PrologixASRLIntfcSession and PrologixTCPIPIntfcSession"
        else:
            ss = "PrologixTCPIPIntfcSession"

        LOGGER.debug(f"{ss} were correctly imported.")
    except Exception as e:
        LOGGER.debug(
            "PrologixASRLIntfcSession and PrologixTCPIPIntfcSession were not imported: %s."
            % e
        )

    try:
        from .gpib import GPIBSession

        LOGGER.debug("GPIBSession was correctly imported.")
    except Exception as e:
        LOGGER.debug("GPIBSession was not imported %s." % e)

    @staticmethod
    def get_library_paths() -> Iterable[LibraryPath]:
        """List a dummy library path to allow to create the library."""
        return (LibraryPath("py"),)

    @staticmethod
    def get_debug_info() -> DebugInfo:
        """Return a list of lines with backend info."""
        from . import __version__

        d: OrderedDict[str, Union[str, List[str], Dict[str, str]]] = OrderedDict()
        d["Version"] = "%s" % __version__

        for key, val in Session.iter_valid_session_classes():
            key_name = "%s %s" % (key[0].name.upper(), key[1])
            d[key_name] = "Available " + val.get_low_level_info()

        for key, issue in Session.iter_session_classes_issues():
            key_name = "%s %s" % (key[0].name.upper(), key[1])
            d[key_name] = issue.split("\n")

        return d

    def _init(self) -> None:
        """Custom initialization code."""
        # Map session handle to session object.
        self.sessions = {}

    def _register(self, obj: Session) -> VISASession:
        """Creates a random but unique session handle for a session object.

        Register it in the sessions dictionary and return the value.

        """
        while True:
            session = VISASession(random.randint(1000000, 9999999))
            if session not in self.sessions:
                break

        self.sessions[session] = obj
        return session

    def open(
        self,
        session: VISARMSession,
        resource_name: str,
        access_mode: constants.AccessModes = constants.AccessModes.no_lock,
        open_timeout: Optional[int] = constants.VI_TMO_IMMEDIATE,
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
            Specifies the maximum time period (in milliseconds) that this
            operation waits before returning an error. constants.VI_TMO_IMMEDIATE
            and constants.VI_TMO_INFINITE are used as min and max.

        Returns
        -------
        VISASession
            Unique logical identifier reference to a session
        StatusCode
            Return value of the library call.

        """
        try:
            open_timeout = None if open_timeout is None else int(open_timeout)
        except ValueError:
            raise ValueError(
                "open_timeout (%r) must be an integer (or compatible type)"
                % open_timeout
            )

        try:
            parsed = rname.parse_resource_name(resource_name)
        except rname.InvalidResourceName:
            return (
                VISASession(0),
                self.handle_return_value(None, StatusCode.error_invalid_resource_name),
            )

        cls = Session.get_session_class(
            parsed.interface_type_const, parsed.resource_class
        )

        try:
            sess = cls(session, resource_name, parsed, open_timeout)
        except OpenError as e:
            return VISASession(0), self.handle_return_value(None, e.error_code)

        return self._register(sess), StatusCode.success

    def clear(self, session: VISASession) -> StatusCode:
        """Clears a device.

        Corresponds to viClear function of the VISA library.

        Parameters
        ----------
        session : typing.VISASession
            Unique logical identifier to a session.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        try:
            sess = self.sessions[session]
        except KeyError:
            return self.handle_return_value(session, StatusCode.error_invalid_object)
        return self.handle_return_value(session, sess.clear())

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
        try:
            sess = self.sessions[session]
        except KeyError:
            return self.handle_return_value(session, StatusCode.error_invalid_object)
        return self.handle_return_value(session, sess.flush(mask))

    def gpib_command(
        self, session: VISASession, command_byte: bytes
    ) -> Tuple[int, StatusCode]:
        """Write GPIB command bytes on the bus.

        Corresponds to viGpibCommand function of the VISA library.

        Parameters
        ----------
        session : VISASession
            Unique logical identifier to a session.
        command_byte : bytes
            Data to write.

        Returns
        -------
        int
            Number of written bytes
        StatusCode
            Return value of the library call.

        """
        try:
            written, st = self.sessions[session].gpib_command(command_byte)
            return written, self.handle_return_value(session, st)
        except KeyError:
            return 0, self.handle_return_value(session, StatusCode.error_invalid_object)

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
        try:
            return self.handle_return_value(
                session, self.sessions[session].assert_trigger(protocol)
            )
        except KeyError:
            return self.handle_return_value(session, StatusCode.error_invalid_object)

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
        try:
            return self.handle_return_value(
                session, self.sessions[session].gpib_send_ifc()
            )
        except KeyError:
            return self.handle_return_value(session, StatusCode.error_invalid_object)

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
        try:
            return self.handle_return_value(
                session, self.sessions[session].gpib_control_ren(mode)
            )
        except KeyError:
            return self.handle_return_value(session, StatusCode.error_invalid_object)

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
        try:
            return self.handle_return_value(
                session, self.sessions[session].gpib_control_atn(mode)
            )
        except KeyError:
            return self.handle_return_value(session, StatusCode.error_invalid_object)

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
        try:
            return self.handle_return_value(
                session,
                self.sessions[session].gpib_pass_control(
                    primary_address, secondary_address
                ),
            )
        except KeyError:
            return self.handle_return_value(session, StatusCode.error_invalid_object)

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
        try:
            sess = self.sessions[session]
        except KeyError:
            return 0, self.handle_return_value(session, StatusCode.error_invalid_object)
        stb, status_code = sess.read_stb()
        return stb, self.handle_return_value(session, status_code)

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
        try:
            sess = self.sessions[session]
            # The RM session directly references the library.
            if sess is not self:
                return self.handle_return_value(session, sess.close())
            else:
                return self.handle_return_value(session, StatusCode.success)
        except KeyError:
            return self.handle_return_value(session, StatusCode.error_invalid_object)

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
        return (
            cast(VISARMSession, self._register(cast(Session, self))),
            self.handle_return_value(None, StatusCode.success),
        )

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
        # For each session type, ask for the list of connected resources and
        # merge them into a single list.
        # HINT: the cast should not be necessary here
        resources: List[str] = []
        for key, st in Session.iter_valid_session_classes():
            resources += st.list_resources()

        return rname.filter(resources, query)

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
        # from the session handle, dispatch to the read method of the session object.
        try:
            data, status_code = self.sessions[session].read(count)
        except KeyError:
            return (
                b"",
                self.handle_return_value(session, StatusCode.error_invalid_object),
            )

        return data, self.handle_return_value(session, status_code)

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
        # from the session handle, dispatch to the write method of the session object.
        try:
            written, status_code = self.sessions[session].write(data)
        except KeyError:
            return 0, self.handle_return_value(session, StatusCode.error_invalid_object)

        return written, self.handle_return_value(session, status_code)

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
        bytes
            Data read
        StatusCode
            Return value of the library call.

        """
        return self.read(session, count)

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
        return self.write(session, data)

    def get_attribute(
        self,
        session: Union[VISASession, VISAEventContext, VISARMSession],
        attribute: Union[constants.ResourceAttribute, constants.EventAttribute],
    ) -> Tuple[Any, StatusCode]:
        """Retrieves the state of an attribute.

        Corresponds to viGetAttribute function of the VISA library.

        Parameters
        ----------
        session : Union[VISASession, VISAEventContext, VISARMSession]
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
        try:
            sess = self.sessions[session]
        except KeyError:
            return (
                None,
                self.handle_return_value(session, StatusCode.error_invalid_object),
            )

        state, status_code = sess.get_attribute(
            cast(constants.ResourceAttribute, attribute)
        )
        return state, self.handle_return_value(session, status_code)

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
        try:
            return self.handle_return_value(
                session,
                self.sessions[session].set_attribute(attribute, attribute_state),
            )
        except KeyError:
            return self.handle_return_value(session, StatusCode.error_invalid_object)

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
        str
            Key that can then be passed to other sessions to share the lock, or
            None for an exclusive lock.
        StatusCode
            Return value of the library call.

        """
        try:
            sess = self.sessions[session]
        except KeyError:
            return (
                "",
                self.handle_return_value(session, StatusCode.error_invalid_object),
            )
        key, status_code = sess.lock(lock_type, timeout, requested_key)
        return key, self.handle_return_value(session, status_code)

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
        try:
            sess = self.sessions[session]
        except KeyError:
            return self.handle_return_value(session, StatusCode.error_invalid_object)
        return self.handle_return_value(session, sess.unlock())

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
        return StatusCode.error_nonimplemented_operation

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
        event_type : constans.EventType
            Logical event identifier.
        mechanism : constants.EventMechanism
            Specifies event handling mechanisms to be discarded.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        return StatusCode.error_nonimplemented_operation
