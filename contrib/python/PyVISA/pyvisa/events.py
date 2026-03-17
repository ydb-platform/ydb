# -*- coding: utf-8 -*-
"""VISA events with convenient access to the available attributes.

This file is part of PyVISA.

:copyright: 2020-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from typing import TYPE_CHECKING, Callable, Dict, Optional, Type

from typing_extensions import ClassVar

from . import attributes, constants, errors, logger
from .attributes import Attribute
from .typing import Any, VISAEventContext, VISAJobID

if TYPE_CHECKING:
    from . import highlevel  # pragma: no cover


class Event:
    """Event that lead to the call of an event handler.

    Do not instantiate directly use the visa_event context manager of the
    Resource object passed to the handler.

    Notes
    -----
    When using the queuing mechanism events are expected to be closed manually
    after handling them.
    When using callbacks, the events should only be closed if VISA will never
    get control again which since we always attempt to properly close all
    resources should never happend and hence events should never be closed manually.

    """

    #: Reference to the visa library
    visalib: "highlevel.VisaLibraryBase"

    #: Type of the event.
    event_type: constants.EventType

    #: Context use to query the event attributes.
    _context: Optional[VISAEventContext]

    #: Maps Event type to Python class encapsulating that event.
    _event_classes: ClassVar[Dict[constants.EventType, Type["Event"]]] = {}

    @classmethod
    def register(
        cls, event_type: constants.EventType
    ) -> Callable[[Type["Event"]], Type["Event"]]:
        """Register a class with a given event type."""

        def _internal(python_class: Type["Event"]) -> Type["Event"]:
            if event_type in cls._event_classes:
                logger.warning(
                    "%s is already registered. "
                    "Overwriting with %s" % (event_type, python_class)
                )

            for attr in attributes.AttributesPerResource[event_type]:
                if not hasattr(python_class, attr.py_name):
                    raise TypeError(
                        "%s is expected to have an attribute %s"
                        % (python_class, attr.py_name)
                    )

            cls._event_classes[event_type] = python_class

            return python_class

        return _internal

    def __new__(
        cls,
        visalib: "highlevel.VisaLibraryBase",
        event_type: constants.EventType,
        context: VISAEventContext,
    ) -> "Event":
        event_cls = cls._event_classes.get(event_type, Event)
        return object.__new__(event_cls)

    def __init__(
        self,
        visalib: "highlevel.VisaLibraryBase",
        event_type: constants.EventType,
        context: Optional[VISAEventContext],
    ) -> None:
        self.visalib = visalib
        self.event_type = event_type
        self._context = context

    @property
    def context(self) -> VISAEventContext:
        """Access the VISA context used to retrieve attributes.

        This is equivalent to the session on a resource.

        """
        c = self._context
        if c is None:
            raise errors.InvalidSession()
        return c

    def get_visa_attribute(self, attribute_id: constants.EventAttribute) -> Any:
        """Get the specified VISA attribute."""
        return self.visalib.get_attribute(self.context, attribute_id)[0]

    def close(self):
        """Simply invalidate the context.

        The event is not closed since it is only ever required when using
        the queue mechanism and this is handled in WaitResponse.

        """
        self._context = None


# Those events do not have any payload beyond their type, for those use the base class
for event in (
    constants.EventType.clear,
    constants.EventType.service_request,
    constants.EventType.gpib_listen,
    constants.EventType.gpib_talk,
    constants.EventType.vxi_vme_sysfail,
    constants.EventType.vxi_vme_sysreset,
):
    Event.register(event)(Event)


@Event.register(constants.EventType.exception)
class ExceptionEvent(Event):
    """Event corresponding to an exception."""

    #: Status code of the operation that generated the exception
    status: Attribute[constants.StatusCode] = attributes.AttrVI_ATTR_STATUS()

    #: Name of the operation that led to the exception
    operation_name: Attribute[str] = attributes.AttrVI_ATTR_OPER_NAME()


@Event.register(constants.EventType.gpib_controller_in_charge)
class GPIBCICEvent(Event):
    """GPIB Controller in Charge event.

    The event is emitted if the status is gained or lost.

    """

    #: New state of the controller in charge status
    cic_state: Attribute[constants.LineState] = (
        attributes.AttrVI_ATTR_GPIB_RECV_CIC_STATE()
    )


@Event.register(constants.EventType.io_completion)
class IOCompletionEvent(Event):
    """Event marking the completion of an IO operation."""

    #: Status code of the asynchronous I/O operation that has completed.
    status: Attribute[constants.StatusCode] = attributes.AttrVI_ATTR_STATUS()

    #: Buffer that was used in an asynchronous operation.
    buffer: Attribute[bytes] = attributes.AttrVI_ATTR_BUFFER()

    #: Actual number of elements that were asynchronously transferred.
    return_count: Attribute[int] = attributes.AttrVI_ATTR_RET_COUNT()

    #: Name of the operation generating the event.
    operation_name: Attribute[str] = attributes.AttrVI_ATTR_OPER_NAME()

    #: Job ID of the asynchronous operation that has completed.
    job_id: Attribute[VISAJobID] = attributes.AttrVI_ATTR_JOB_ID()

    @property
    def data(self):
        """Portion of the buffer that was actually filled during the call."""
        return bytes(self.buffer[: self.return_count])


@Event.register(constants.EventType.trig)
class TrigEvent(Event):
    """Trigger event."""

    #: Identifier of the triggering mechanism on which the specified trigger event
    #: was received.
    received_trigger_id: Attribute[constants.TriggerID] = (
        attributes.AttrVI_ATTR_TRIG_ID()
    )


@Event.register(constants.EventType.usb_interrupt)
class USBInteruptEvent(Event):
    """USB interruption event."""

    #: Status of the read operation from the USB interrupt-IN pipe.
    status: Attribute[constants.StatusCode] = attributes.AttrVI_ATTR_STATUS()

    #: Size of the data that was received from the USB interrupt-IN pipe.
    size: Attribute[int] = attributes.AttrVI_ATTR_USB_RECV_INTR_SIZE()

    #: Actual data that was received from the USB interrupt-IN pipe.
    data = attributes.AttrVI_ATTR_USB_RECV_INTR_DATA()


@Event.register(constants.EventType.vxi_signal_interrupt)
class VXISignalInteruptEvent(Event):
    """VXI signal event."""

    #: 16-bit Status/ID value retrieved during the IACK cycle or
    #: from the Signal register.
    signal_register_status_id: Attribute[int] = attributes.AttrVI_ATTR_SIGP_STATUS_ID()


@Event.register(constants.EventType.vxi_vme_interrupt)
class VXIInterruptEvent(Event):
    """VXI interrupt event."""

    #: 32-bit status/ID retrieved during the IACK cycle.
    status_id: Attribute[int] = attributes.AttrVI_ATTR_INTR_STATUS_ID()

    #: VXI interrupt level on which the interrupt was received.
    level: Attribute[int] = attributes.AttrVI_ATTR_RECV_INTR_LEVEL()


@Event.register(constants.EventType.pxi_interrupt)
class PXIInteruptEvent(Event):
    """PXI interruption event."""

    #: Index of the interrupt sequence that detected the interrupt condition.
    sequence: Attribute[int] = attributes.AttrVI_ATTR_PXI_RECV_INTR_SEQ()

    #: First PXI/PCI register read in the successful interrupt detection sequence.
    data: Attribute[int] = attributes.AttrVI_ATTR_PXI_RECV_INTR_DATA()
