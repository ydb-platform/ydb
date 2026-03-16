# -*- coding: utf-8 -*-
"""High level wrapper for a Resource.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import contextlib
import time
from functools import update_wrapper
from typing import (
    Any,
    Callable,
    ContextManager,
    Iterator,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
)

from typing_extensions import ClassVar, Literal

from .. import attributes, constants, errors, highlevel, logger, rname, typing, util
from ..attributes import Attribute
from ..events import Event
from ..typing import VISAEventContext, VISAHandler, VISASession


class WaitResponse:
    """Class used in return of wait_on_event.

    It properly closes the context upon delete.

    A call with event_type of 0 (normally used when timed_out is True) will store
    None as the event and event type, otherwise it records the proper Event.

    """

    #: Reference to the event object that was waited for.
    event: Event

    #: Status code returned by the VISA library
    ret: constants.StatusCode

    #: Did a timeout occurs
    timed_out: bool

    def __init__(
        self,
        event_type: constants.EventType,
        context: Optional[VISAEventContext],
        ret: constants.StatusCode,
        visalib: highlevel.VisaLibraryBase,
        timed_out: bool = False,
    ):
        self.event = Event(visalib, event_type, context)
        self._event_type = constants.EventType(event_type)
        self._context = context
        self.ret = ret
        self._visalib = visalib
        self.timed_out = timed_out

    def __del__(self) -> None:
        if self.event._context is not None:
            try:
                self._visalib.close(self.event._context)
                self.event.close()
            except errors.VisaIOError:
                pass


T = TypeVar("T", bound="Resource")


class Resource(object):
    """Base class for resources.

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    #: Reference to the resource manager used by this resource
    resource_manager: highlevel.ResourceManager

    #: Reference to the VISA library instance used by the resource
    visalib: highlevel.VisaLibraryBase

    #: VISA attribute descriptor classes that can be used to introspect the
    #: supported attributes and the possible values. The "often used" ones
    #: are generally directly available on the resource.
    visa_attributes_classes: ClassVar[Set[Type[attributes.Attribute]]]

    @classmethod
    def register(
        cls, interface_type: constants.InterfaceType, resource_class: str
    ) -> Callable[[Type[T]], Type[T]]:
        """Create a decorator to register a class.

        The class is associated to an interface type, resource class pair.

        Parameters
        ----------
        interface_type : constants.InterfaceType
            Interface type for which to register a wrapper class.
        resource_class : str
            Resource class for which to register a wrapper class.

        Returns
        -------
        Callable[[Type[T]], Type[T]]
            Decorator registering the class. Raises TypeError if some VISA
            attributes are missing on the registered class.

        """

        def _internal(python_class):
            highlevel.ResourceManager.register_resource_class(
                interface_type, resource_class, python_class
            )

            return python_class

        return _internal

    def __init__(
        self, resource_manager: highlevel.ResourceManager, resource_name: str
    ) -> None:
        self._resource_manager = resource_manager
        self.visalib = self._resource_manager.visalib

        # We store the resource name and use preferably the private attr over
        # the public descriptor internally because the public descriptor
        # requires a live instance the VISA library, which means it is much
        # slower but also can cause issue in error reporting when accessing the
        # repr
        self._resource_name: str
        try:
            # Attempt to normalize the resource name. Can fail for aliases
            self._resource_name = str(rname.ResourceName.from_string(resource_name))
        except rname.InvalidResourceName:
            self._resource_name = resource_name

        self._logging_extra = {
            "library_path": self.visalib.library_path,
            "resource_manager.session": self._resource_manager.session,
            "resource_name": self._resource_name,
            "session": None,
        }

        #: Session handle.
        self._session: Optional[VISASession] = None

    @property
    def session(self) -> VISASession:
        """Resource session handle.

        Raises
        ------
        errors.InvalidSession
            Raised if session is closed.

        """
        if self._session is None:
            raise errors.InvalidSession()
        return self._session

    @session.setter
    def session(self, value: Optional[VISASession]) -> None:
        self._session = value

    def __del__(self) -> None:
        if self._session is not None:
            try:
                self.close()
            except Exception:
                # Exceptions must be suppressed during __del__, because they
                # can't be caught by normal exception handling techniques and
                # get printed to stderr instead
                logger.warning(
                    "Exception suppressed while destroying a resource", exc_info=True
                )

    def __str__(self) -> str:
        return "%s at %s" % (self.__class__.__name__, self._resource_name)

    def __repr__(self) -> str:
        return "<%r(%r)>" % (self.__class__.__name__, self._resource_name)

    def __enter__(self: T) -> T:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    @property
    def last_status(self) -> constants.StatusCode:
        """Last status code for this session."""
        return self.visalib.get_last_status_in_session(self.session)

    @property
    def resource_info(self) -> highlevel.ResourceInfo:
        """Get the extended information of this resource."""
        return self.visalib.parse_resource_extended(
            self._resource_manager.session, self._resource_name
        )[0]

    # --- VISA attributes --------------------------------------------------------------

    #: VISA attributes require the resource to be opened in order to get accessed.
    #: Please have a look at the attributes definition for more details

    # VI_ATTR_RM_SESSION is not implemented as resource property,
    # use .resource_manager.session instead
    # resource_manager_session: Attribute[int] = attributes.AttrVI_ATTR_RM_SESSION()

    #: Interface type of the given session.
    interface_type: Attribute[constants.InterfaceType] = (
        attributes.AttrVI_ATTR_INTF_TYPE()
    )

    #: Board number for the given interface.
    interface_number: Attribute[int] = attributes.AttrVI_ATTR_INTF_NUM()

    #: Resource class (for example, "INSTR") as defined by the canonical resource name.
    resource_class: Attribute[str] = attributes.AttrVI_ATTR_RSRC_CLASS()

    #: Unique identifier for a resource compliant with the address structure.
    resource_name: Attribute[str] = attributes.AttrVI_ATTR_RSRC_NAME()

    #: Resource version that identifies the revisions or implementations of a resource.
    implementation_version: Attribute[int] = attributes.AttrVI_ATTR_RSRC_IMPL_VERSION()

    #: Current locking state of the resource.
    lock_state: Attribute[constants.AccessModes] = (
        attributes.AttrVI_ATTR_RSRC_LOCK_STATE()
    )

    #: Version of the VISA specification to which the implementation is compliant.
    spec_version: Attribute[int] = attributes.AttrVI_ATTR_RSRC_SPEC_VERSION()

    #: Manufacturer name of the vendor that implemented the VISA library.
    resource_manufacturer_name: Attribute[str] = attributes.AttrVI_ATTR_RSRC_MANF_NAME()

    #: Timeout in milliseconds for all resource I/O operations.
    timeout: Attribute[float] = attributes.AttrVI_ATTR_TMO_VALUE()

    def ignore_warning(
        self, *warnings_constants: constants.StatusCode
    ) -> ContextManager:
        """Ignoring warnings context manager for the current resource.

        Parameters
        ----------
        warnings_constants : constants.StatusCode
            Constants identifying the warnings to ignore.

        """
        return self.visalib.ignore_warning(self.session, *warnings_constants)

    def open(
        self,
        access_mode: constants.AccessModes = constants.AccessModes.no_lock,
        open_timeout: int = 5000,
    ) -> None:
        """Opens a session to the specified resource.

        Parameters
        ----------
        access_mode : constants.AccessModes, optional
            Specifies the mode by which the resource is to be accessed.
            Defaults to constants.AccessModes.no_lock.
        open_timeout : int, optional
            If the ``access_mode`` parameter requests a lock, then this parameter
            specifies the absolute time period (in milliseconds) that the
            resource waits to get unlocked before this operation returns an error.
            Defaults to 5000.

        """
        logger.debug("%s - opening ...", self._resource_name, extra=self._logging_extra)
        with self._resource_manager.ignore_warning(
            constants.StatusCode.success_device_not_present
        ):
            self.session, status = self._resource_manager.open_bare_resource(
                self._resource_name, access_mode, open_timeout
            )

            if status == constants.StatusCode.success_device_not_present:
                # The device was not ready when we opened the session.
                # Now it gets five seconds more to become ready.
                # Every 0.1 seconds we probe it with viClear.
                start_time = time.time()
                sleep_time = 0.1
                try_time = 5
                while time.time() - start_time < try_time:
                    time.sleep(sleep_time)
                    try:
                        self.clear()
                        break
                    except errors.VisaIOError as error:
                        if error.error_code != constants.StatusCode.error_no_listeners:
                            raise

        self._logging_extra["session"] = self.session
        logger.debug(
            "%s - is open with session %s",
            self._resource_name,
            self.session,
            extra=self._logging_extra,
        )

    def before_close(self) -> None:
        """Called just before closing an instrument."""
        self.__switch_events_off()

    def close(self) -> None:
        """Closes the VISA session and marks the handle as invalid."""
        try:
            logger.debug("%s - closing", self._resource_name, extra=self._logging_extra)
            self.before_close()
            self.visalib.close(self.session)
            # Mypy is confused by the idea that we can set a value we cannot get
            self.session = None  # type: ignore
        except errors.InvalidSession:
            pass

        logger.debug("%s - is closed", self._resource_name, extra=self._logging_extra)

    def __switch_events_off(self) -> None:
        """Switch off and discards all events."""
        self.disable_event(
            constants.EventType.all_enabled, constants.EventMechanism.all
        )
        self.discard_events(
            constants.EventType.all_enabled, constants.EventMechanism.all
        )
        self.visalib.uninstall_all_visa_handlers(self.session)

    def get_visa_attribute(self, name: constants.ResourceAttribute) -> Any:
        """Retrieves the state of an attribute in this resource.

        One should prefer the dedicated descriptor for often used attributes
        since those perform checks and automatic conversion on the value.

        Parameters
        ----------
        name : constants.ResourceAttribute
            Resource attribute for which the state query is made.

        Returns
        -------
        Any
            The state of the queried attribute for a specified resource.

        """
        return self.visalib.get_attribute(self.session, name)[0]

    def set_visa_attribute(
        self, name: constants.ResourceAttribute, state: Any
    ) -> constants.StatusCode:
        """Set the state of an attribute.

        One should prefer the dedicated descriptor for often used attributes
        since those perform checks and automatic conversion on the value.

        Parameters
        ----------
        name : constants.ResourceAttribute
            Attribute for which the state is to be modified.
        state : Any
            The state of the attribute to be set for the specified object.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        return self.visalib.set_attribute(self.session, name, state)

    def clear(self) -> None:
        """Clear this resource."""
        self.visalib.clear(self.session)

    def install_handler(
        self, event_type: constants.EventType, handler: VISAHandler, user_handle=None
    ) -> Any:
        """Install handlers for event callbacks in this resource.

        Parameters
        ----------
        event_type : constants.EventType
            Logical event identifier.
        handler : VISAHandler
            Handler function to be installed by a client application.
        user_handle :
            A value specified by an application that can be used for identifying
            handlers uniquely for an event type. Depending on the backend they
            may be restriction on the possible values. Look at the backend
            `install_visa_handler` for more details.

        Returns
        -------
        Any
            User handle in a format amenable to the backend. This is this
            representation of the handle that should be used when unistalling
            a handler.

        """
        return self.visalib.install_visa_handler(
            self.session, event_type, handler, user_handle
        )

    def wrap_handler(
        self, callable: Callable[["Resource", Event, Any], None]
    ) -> VISAHandler:
        """Wrap an event handler to provide the signature expected by VISA.

        The handler is expected to have the following signature:
        handler(resource: Resource, event: Event, user_handle: Any) -> None.

        The wrapped handler should be used only to handle events on the resource
        used to wrap the handler.

        """

        def event_handler(
            session: VISASession,
            event_type: constants.EventType,
            event_context: typing.VISAEventContext,
            user_handle: Any,
        ) -> None:
            if session != self.session:
                raise RuntimeError(
                    "When wrapping a handler, the resource used to wrap the handler"
                    "must be the same on which the handler will be installed."
                    f"Wrapping session: {self.session}, event on session: {session}"
                )
            event = Event(self.visalib, event_type, event_context)
            try:
                return callable(self, event, user_handle)
            finally:
                event.close()

        update_wrapper(event_handler, callable)

        return event_handler

    def uninstall_handler(
        self, event_type: constants.EventType, handler: VISAHandler, user_handle=None
    ) -> None:
        """Uninstalls handlers for events in this resource.

        Parameters
        ----------
        event_type : constants.EventType
            Logical event identifier.
        handler : VISAHandler
            Handler function to be uninstalled by a client application.
        user_handle : Any
            The user handle returned by install_handler.

        """
        self.visalib.uninstall_visa_handler(
            self.session, event_type, handler, user_handle
        )

    def disable_event(
        self, event_type: constants.EventType, mechanism: constants.EventMechanism
    ) -> None:
        """Disable notification for an event type(s) via the specified mechanism(s).

        Parameters
        ----------
        event_type : constants.EventType
            Logical event identifier.
        mechanism : constants.EventMechanism
            Specifies event handling mechanisms to be disabled.

        """
        self.visalib.disable_event(self.session, event_type, mechanism)

    def discard_events(
        self, event_type: constants.EventType, mechanism: constants.EventMechanism
    ) -> None:
        """Discards event occurrences for an event type and mechanism in this resource.

        Parameters
        ----------
        event_type : constants.EventType
            Logical event identifier.
        mechanism : constants.EventMechanism
            Specifies event handling mechanisms to be disabled.

        """
        self.visalib.discard_events(self.session, event_type, mechanism)

    def enable_event(
        self,
        event_type: constants.EventType,
        mechanism: constants.EventMechanism,
        context: None = None,
    ) -> None:
        """Enable event occurrences for specified event types and mechanisms in this resource.

        Parameters
        ----------
        event_type : constants.EventType
            Logical event identifier.
        mechanism : constants.EventMechanism
            Specifies event handling mechanisms to be enabled
        context : None
            Not currently used, leave as None.

        """
        self.visalib.enable_event(self.session, event_type, mechanism, context)

    def wait_on_event(
        self,
        in_event_type: constants.EventType,
        timeout: int,
        capture_timeout: bool = False,
    ) -> WaitResponse:
        """Waits for an occurrence of the specified event in this resource.

        in_event_type : constants.EventType
            Logical identifier of the event(s) to wait for.
        timeout : int
            Absolute time period in time units that the resource shall wait for
            a specified event to occur before returning the time elapsed error.
            The time unit is in milliseconds. None means waiting forever if
            necessary.
        capture_timeout : bool, optional
            When True will not produce a VisaIOError(VI_ERROR_TMO) but instead
            return a WaitResponse with timed_out=True.

        Returns
        -------
        WaitResponse
            Object that contains event_type, context and ret value.

        """
        try:
            event_type, context, ret = self.visalib.wait_on_event(
                self.session, in_event_type, timeout
            )
        except errors.VisaIOError as exc:
            if capture_timeout and exc.error_code == constants.StatusCode.error_timeout:
                return WaitResponse(
                    in_event_type,
                    None,
                    constants.StatusCode.error_timeout,
                    self.visalib,
                    timed_out=True,
                )
            raise
        return WaitResponse(event_type, context, ret, self.visalib)

    def lock(
        self,
        timeout: Union[float, Literal["default"]] = "default",
        requested_key: Optional[str] = None,
    ) -> str:
        """Establish a shared lock to the resource.

        Parameters
        ----------
        timeout : Union[float, Literal["default"]], optional
            Absolute time period (in milliseconds) that a resource waits to get
            unlocked by the locking session before returning an error.
            Defaults to "default" which means use self.timeout.
        requested_key : Optional[str], optional
            Access key used by another session with which you want your session
            to share a lock or None to generate a new shared access key.

        Returns
        -------
        str
            A new shared access key if requested_key is None, otherwise, same
            value as the requested_key

        """
        tout = cast(float, self.timeout if timeout == "default" else timeout)
        clean_timeout = util.cleanup_timeout(tout)
        return self.visalib.lock(
            self.session, constants.Lock.shared, clean_timeout, requested_key
        )[0]

    def lock_excl(self, timeout: Union[float, Literal["default"]] = "default") -> None:
        """Establish an exclusive lock to the resource.

        Parameters
        ----------
        timeout : Union[float, Literal["default"]], optional
            Absolute time period (in milliseconds) that a resource waits to get
            unlocked by the locking session before returning an error.
            Defaults to "default" which means use self.timeout.

        """
        tout = cast(float, self.timeout if timeout == "default" else timeout)
        clean_timeout = util.cleanup_timeout(tout)
        self.visalib.lock(self.session, constants.Lock.exclusive, clean_timeout, None)

    def unlock(self) -> None:
        """Relinquishes a lock for the specified resource."""
        self.visalib.unlock(self.session)

    @contextlib.contextmanager
    def lock_context(
        self,
        timeout: Union[float, Literal["default"]] = "default",
        requested_key: Optional[str] = "exclusive",
    ) -> Iterator[Optional[str]]:
        """A context that locks

        Parameters
        ----------
        timeout : Union[float, Literal["default"]], optional
            Absolute time period (in milliseconds) that a resource waits to get
            unlocked by the locking session before returning an error.
            Defaults to "default" which means use self.timeout.
        requested_key : Optional[str], optional
            When using default of 'exclusive' the lock is an exclusive lock.
            Otherwise it is the access key for the shared lock or None to
            generate a new shared access key.

        Yields
        ------
        Optional[str]
            The access_key if applicable.

        """
        if requested_key == "exclusive":
            self.lock_excl(timeout)
            access_key = None
        else:
            access_key = self.lock(timeout, requested_key)
        try:
            yield access_key
        finally:
            self.unlock()


Resource.register(constants.InterfaceType.unknown, "")(Resource)
