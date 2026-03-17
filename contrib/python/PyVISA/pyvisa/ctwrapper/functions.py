# -*- coding: utf-8 -*-
"""Defines VPP 4.3.2 wrapping functions using ctypes, adding signatures to the library.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import warnings
from contextlib import contextmanager
from ctypes import (
    POINTER,
    byref,
    c_double,
    c_long,
    c_void_p,
    c_wchar_p,
    create_string_buffer,
)
from functools import update_wrapper
from threading import Lock
from typing import Any, Callable, Optional, Tuple

from pyvisa import attributes, constants, ctwrapper, typing
from pyvisa.highlevel import ResourceInfo

from . import types
from .types import (
    FUNCTYPE,
    ViAccessMode,
    ViAChar,
    ViAddr,
    ViAttr,
    ViAttrState,
    ViAUInt8,
    ViAUInt16,
    ViAUInt32,
    ViAUInt64,
    ViBoolean,
    ViBuf,
    ViBusAddress,
    ViBusAddress64,
    ViBusSize,
    ViEvent,
    ViEventFilter,
    ViEventType,
    ViFindList,
    ViHndlr,
    ViInt16,
    ViJobId,
    ViKeyId,
    ViObject,
    ViPAddr,
    ViPBuf,
    ViPBusAddress,
    ViPEvent,
    ViPEventType,
    ViPFindList,
    ViPJobId,
    ViPSession,
    ViPUInt8,
    ViPUInt16,
    ViPUInt32,
    ViPUInt64,
    ViRsrc,
    ViSession,
    ViStatus,
    ViString,
    ViUInt8,
    ViUInt16,
    ViUInt32,
    ViUInt64,
    buffer_to_text,
)

visa_functions = [
    "assert_interrupt_signal",
    "assert_trigger",
    "assert_utility_signal",
    "buffer_read",
    "buffer_write",
    "clear",
    "close",
    "disable_event",
    "discard_events",
    "enable_event",
    "_find_next",
    "_find_resources",
    "flush",
    "get_attribute",
    "gpib_command",
    "gpib_control_atn",
    "gpib_control_ren",
    "gpib_pass_control",
    "gpib_send_ifc",
    "in_16",
    "in_32",
    "in_8",
    "install_handler",
    "lock",
    "map_address",
    "map_trigger",
    "memory_allocation",
    "memory_free",
    "move",
    "move_asynchronously",
    "move_in_16",
    "move_in_32",
    "move_in_8",
    "move_out_16",
    "move_out_32",
    "move_out_8",
    "open",
    "open_default_resource_manager",
    "out_16",
    "out_32",
    "out_8",
    "parse_resource",
    "parse_resource_extended",
    "peek_16",
    "peek_32",
    "peek_8",
    "poke_16",
    "poke_32",
    "poke_8",
    "read",
    "read_to_file",
    "read_stb",
    "set_attribute",
    "set_buffer",
    "status_description",
    "terminate",
    "uninstall_handler",
    "unlock",
    "unmap_address",
    "unmap_trigger",
    "usb_control_in",
    "usb_control_out",
    "vxi_command_query",
    "wait_on_event",
    "write",
    "write_asynchronously",
    "write_from_file",
    "in_64",
    "move_in_64",
    "out_64",
    "move_out_64",
    "poke_64",
    "peek_64",
]

__all__ = ["visa_functions", "set_signatures", *visa_functions]

VI_SPEC_VERSION = 0x00300000

#: Global lock to ensure that we cannot have one thread change the type while
#: another is trying to interact with VISA
ViHndlr_lock = Lock()


@contextmanager
def set_user_handle_type(library, user_handle: Any):
    """Set the type of the user handle to install and uninstall handler signature.

    Parameters
    ----------
    library :
        The visa library wrapped by ctypes.
    user_handle :
        User handle used when registering an event handler. Use None for a void_p.

    """
    with ViHndlr_lock:
        # Actually, it's not necessary to change ViHndlr *globally*.  However,
        # I don't want to break symmetry too much with all the other VPP43
        # routines.
        global ViHndlr

        if user_handle is None:
            user_handle_p = c_void_p
        else:
            user_handle_p = POINTER(type(user_handle))  # type: ignore

        ViHndlr = FUNCTYPE(ViStatus, ViSession, ViEventType, ViEvent, user_handle_p)
        library.viInstallHandler.argtypes = [
            ViSession,
            ViEventType,
            ViHndlr,
            user_handle_p,
        ]
        library.viUninstallHandler.argtypes = [
            ViSession,
            ViEventType,
            ViHndlr,
            user_handle_p,
        ]
        yield


def set_signatures(
    library, errcheck: Optional[Callable[[int, Callable, tuple], int]] = None
):
    """Set the signatures of most visa functions in the library.

    All instrumentation related functions are specified here.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        The visa library wrapped by ctypes.
    errcheck : Optional[Callable[[int, Callable, tuple], int]]
        Error checking callable used for visa functions that return ViStatus.
        It should be take three arguments (result, func, arguments).
        See errcheck in ctypes.

    """
    # Somehow hasattr(library, '_functions') segfaults in cygwin (See #131)
    if "_functions" not in dir(library):
        library._functions = []
        library._functions_failed = []

    def _applier(restype, errcheck_):
        def _internal(function_name, argtypes, required=False):
            try:
                set_signature(library, function_name, argtypes, restype, errcheck_)
                library._functions.append(function_name)
            except AttributeError:
                library._functions_failed.append(function_name)
                if required:
                    raise

        return _internal

    # Visa functions with ViStatus return code
    apply = _applier(ViStatus, errcheck)
    apply("viAssertIntrSignal", [ViSession, ViInt16, ViUInt32])
    apply("viAssertTrigger", [ViSession, ViUInt16])
    apply("viAssertUtilSignal", [ViSession, ViUInt16])
    apply("viBufRead", [ViSession, ViPBuf, ViUInt32, ViPUInt32])
    apply("viBufWrite", [ViSession, ViBuf, ViUInt32, ViPUInt32])
    apply("viClear", [ViSession])
    apply("viClose", [ViObject])
    apply("viDisableEvent", [ViSession, ViEventType, ViUInt16])
    apply("viDiscardEvents", [ViSession, ViEventType, ViUInt16])
    apply("viEnableEvent", [ViSession, ViEventType, ViUInt16, ViEventFilter])
    apply("viFindNext", [ViSession, ViAChar])
    apply("viFindRsrc", [ViSession, ViString, ViPFindList, ViPUInt32, ViAChar])
    apply("viFlush", [ViSession, ViUInt16])
    apply("viGetAttribute", [ViObject, ViAttr, c_void_p])
    apply("viGpibCommand", [ViSession, ViBuf, ViUInt32, ViPUInt32])
    apply("viGpibControlATN", [ViSession, ViUInt16])
    apply("viGpibControlREN", [ViSession, ViUInt16])
    apply("viGpibPassControl", [ViSession, ViUInt16, ViUInt16])
    apply("viGpibSendIFC", [ViSession])

    apply("viIn8", [ViSession, ViUInt16, ViBusAddress, ViPUInt8])
    apply("viIn16", [ViSession, ViUInt16, ViBusAddress, ViPUInt16])
    apply("viIn32", [ViSession, ViUInt16, ViBusAddress, ViPUInt32])
    apply("viIn64", [ViSession, ViUInt16, ViBusAddress, ViPUInt64])

    apply("viIn8Ex", [ViSession, ViUInt16, ViBusAddress64, ViPUInt8])
    apply("viIn16Ex", [ViSession, ViUInt16, ViBusAddress64, ViPUInt16])
    apply("viIn32Ex", [ViSession, ViUInt16, ViBusAddress64, ViPUInt32])
    apply("viIn64Ex", [ViSession, ViUInt16, ViBusAddress64, ViPUInt64])

    apply("viInstallHandler", [ViSession, ViEventType, ViHndlr, ViAddr])
    apply("viLock", [ViSession, ViAccessMode, ViUInt32, ViKeyId, ViAChar])
    apply(
        "viMapAddress",
        [ViSession, ViUInt16, ViBusAddress, ViBusSize, ViBoolean, ViAddr, ViPAddr],
    )
    apply("viMapTrigger", [ViSession, ViInt16, ViInt16, ViUInt16])
    apply("viMemAlloc", [ViSession, ViBusSize, ViPBusAddress])
    apply("viMemFree", [ViSession, ViBusAddress])
    apply(
        "viMove",
        [
            ViSession,
            ViUInt16,
            ViBusAddress,
            ViUInt16,
            ViUInt16,
            ViBusAddress,
            ViUInt16,
            ViBusSize,
        ],
    )
    apply(
        "viMoveAsync",
        [
            ViSession,
            ViUInt16,
            ViBusAddress,
            ViUInt16,
            ViUInt16,
            ViBusAddress,
            ViUInt16,
            ViBusSize,
            ViPJobId,
        ],
    )

    apply("viMoveIn8", [ViSession, ViUInt16, ViBusAddress, ViBusSize, ViAUInt8])
    apply("viMoveIn16", [ViSession, ViUInt16, ViBusAddress, ViBusSize, ViAUInt16])
    apply("viMoveIn32", [ViSession, ViUInt16, ViBusAddress, ViBusSize, ViAUInt32])
    apply("viMoveIn64", [ViSession, ViUInt16, ViBusAddress, ViBusSize, ViAUInt64])

    apply("viMoveIn8Ex", [ViSession, ViUInt16, ViBusAddress64, ViBusSize, ViAUInt8])
    apply("viMoveIn16Ex", [ViSession, ViUInt16, ViBusAddress64, ViBusSize, ViAUInt16])
    apply("viMoveIn32Ex", [ViSession, ViUInt16, ViBusAddress64, ViBusSize, ViAUInt32])
    apply("viMoveIn64Ex", [ViSession, ViUInt16, ViBusAddress64, ViBusSize, ViAUInt64])

    apply("viMoveOut8", [ViSession, ViUInt16, ViBusAddress, ViBusSize, ViAUInt8])
    apply("viMoveOut16", [ViSession, ViUInt16, ViBusAddress, ViBusSize, ViAUInt16])
    apply("viMoveOut32", [ViSession, ViUInt16, ViBusAddress, ViBusSize, ViAUInt32])
    apply("viMoveOut64", [ViSession, ViUInt16, ViBusAddress, ViBusSize, ViAUInt64])

    apply("viMoveOut8Ex", [ViSession, ViUInt16, ViBusAddress64, ViBusSize, ViAUInt8])
    apply("viMoveOut16Ex", [ViSession, ViUInt16, ViBusAddress64, ViBusSize, ViAUInt16])
    apply("viMoveOut32Ex", [ViSession, ViUInt16, ViBusAddress64, ViBusSize, ViAUInt32])
    apply("viMoveOut64Ex", [ViSession, ViUInt16, ViBusAddress64, ViBusSize, ViAUInt64])

    apply(
        "viOpen", [ViSession, ViRsrc, ViAccessMode, ViUInt32, ViPSession], required=True
    )

    apply("viOpenDefaultRM", [ViPSession], required=True)

    apply("viOut8", [ViSession, ViUInt16, ViBusAddress, ViUInt8])
    apply("viOut16", [ViSession, ViUInt16, ViBusAddress, ViUInt16])
    apply("viOut32", [ViSession, ViUInt16, ViBusAddress, ViUInt32])
    apply("viOut64", [ViSession, ViUInt16, ViBusAddress, ViUInt64])

    apply("viOut8Ex", [ViSession, ViUInt16, ViBusAddress64, ViUInt8])
    apply("viOut16Ex", [ViSession, ViUInt16, ViBusAddress64, ViUInt16])
    apply("viOut32Ex", [ViSession, ViUInt16, ViBusAddress64, ViUInt32])
    apply("viOut64Ex", [ViSession, ViUInt16, ViBusAddress64, ViUInt64])

    apply("viParseRsrc", [ViSession, ViRsrc, ViPUInt16, ViPUInt16])
    apply(
        "viParseRsrcEx",
        [ViSession, ViRsrc, ViPUInt16, ViPUInt16, ViAChar, ViAChar, ViAChar],
    )

    apply("viRead", [ViSession, ViPBuf, ViUInt32, ViPUInt32])
    apply("viReadAsync", [ViSession, ViPBuf, ViUInt32, ViPJobId])
    apply("viReadSTB", [ViSession, ViPUInt16])
    apply("viReadToFile", [ViSession, ViString, ViUInt32, ViPUInt32])

    apply("viSetAttribute", [ViObject, ViAttr, ViAttrState])
    apply("viSetBuf", [ViSession, ViUInt16, ViUInt32])

    apply("viStatusDesc", [ViObject, ViStatus, ViAChar])
    apply("viTerminate", [ViSession, ViUInt16, ViJobId])
    apply("viUninstallHandler", [ViSession, ViEventType, ViHndlr, ViAddr])
    apply("viUnlock", [ViSession])
    apply("viUnmapAddress", [ViSession])
    apply("viUnmapTrigger", [ViSession, ViInt16, ViInt16])
    apply(
        "viUsbControlIn",
        [ViSession, ViInt16, ViInt16, ViUInt16, ViUInt16, ViUInt16, ViPBuf, ViPUInt16],
    )
    apply(
        "viUsbControlOut",
        [ViSession, ViInt16, ViInt16, ViUInt16, ViUInt16, ViUInt16, ViPBuf],
    )

    # The following "V" routines are *not* implemented in PyVISA, and will
    # never be: viVPrintf, viVQueryf, viVScanf, viVSPrintf, viVSScanf

    apply("viVxiCommandQuery", [ViSession, ViUInt16, ViUInt32, ViPUInt32])
    apply("viWaitOnEvent", [ViSession, ViEventType, ViUInt32, ViPEventType, ViPEvent])
    apply("viWrite", [ViSession, ViBuf, ViUInt32, ViPUInt32])
    apply("viWriteAsync", [ViSession, ViBuf, ViUInt32, ViPJobId])
    apply("viWriteFromFile", [ViSession, ViString, ViUInt32, ViPUInt32])

    # Functions that return void.
    apply = _applier(None, None)
    apply("viPeek8", [ViSession, ViAddr, ViPUInt8])
    apply("viPeek16", [ViSession, ViAddr, ViPUInt16])
    apply("viPeek32", [ViSession, ViAddr, ViPUInt32])
    apply("viPeek64", [ViSession, ViAddr, ViPUInt64])

    apply("viPoke8", [ViSession, ViAddr, ViUInt8])
    apply("viPoke16", [ViSession, ViAddr, ViUInt16])
    apply("viPoke32", [ViSession, ViAddr, ViUInt32])
    apply("viPoke64", [ViSession, ViAddr, ViUInt64])


def set_signature(
    library,
    function_name: str,
    argtypes: tuple,
    restype,
    errcheck: Optional[Callable[[int, Callable, tuple], int]],
):
    """Set the signature of single function in a library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    function_name : str
        Name of the function as appears in the header file.
    argtypes : tuple
        ctypes types to specify the argument types that the function accepts.
    restype :
        A ctypes type to specify the result type of the foreign function.
        Use None for void, a function not returning anything.
    errcheck : Optional[Callable[[int, Callable, tuple], int]]
        Error checking callable used for visa functions that return ViStatus.
        It should be take three arguments (result, func, arguments).
        See errcheck in ctypes.

    """

    func = getattr(library, function_name)
    func.argtypes = argtypes
    if restype is not None:
        func.restype = restype
    if errcheck is not None:
        func.errcheck = errcheck


# The VPP-4.3.2 routines

# Usually, there is more than one way to pass parameters to ctypes calls.  The
# ctypes policy used in this code goes as follows:
#
# * Null pointers are passed as "None" rather than "0".  This is a little bit
#   unfortunate, since the VPP specification calls this "VI_NULL", but I can't
#   use "VI_NULL" since it's an integer and may not be compatible with a
#   pointer type (don't know whether this is really dangerous).
#
# * Strings must have been created with "create_string_buffer" and are passed
#   without any further conversion; they stand in the parameter list as is.
#   The same applies to pseudo-string types as ViRsrc or VuBuf.  Their Pythonic
#   counterpats are strings as well.
#
# * All other types are explicitly cast using the types defined by ctypes'
#   "restype".
#
# Further notes:
#
# * The following Python routines take and give handles as ctypes objects.
#   Since the user shouldn't be interested in handle values anyway, I see no
#   point in converting them to Python strings or integers.
#
# * All other parameters are natural Python types, i.e. strings (may contain
#   binary data) and integers.  The same is true for return values.
#
# * The original VPP function signatures cannot be realised in Python, at least
#   not in a sensible way, because a) Python has no real call-by-reference, and
#   b) Python allows for more elegant solutions, e.g. using len(buffer) instead
#   of a separate "count" parameter, or using tuples as return values.
#
#   Therefore, all function signatures have been carefully adjusted.  I think
#   this is okay, since the original standard must be adopted to at least C and
#   Visual Basic anyway, with slight modifications.  I also made the function
#   names and parameters more legible, but in a way that it's perfectly clear
#   which original function is meant.
#
#   The important thing is that the semantics of functions and parameters are
#   totally intact, and the inner order of parameters, too.  There is a 1:1
#   mapping.


def assert_interrupt_signal(library, session, mode, status_id):
    """Asserts the specified interrupt or signal.

    Corresponds to viAssertIntrSignal function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    mode : constants.AssertSignalInterrupt
        How to assert the interrupt.
    status_id : int
        Status value to be presented during an interrupt acknowledge cycle.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viAssertIntrSignal(session, mode, status_id)


def assert_trigger(library, session, protocol):
    """Assert software or hardware trigger.

    Corresponds to viAssertTrigger function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    protocol : constants.TriggerProtocol
        Trigger protocol to use during assertion.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viAssertTrigger(session, protocol)


def assert_utility_signal(library, session, line):
    """Assert or deassert the specified utility bus signal.

    Corresponds to viAssertUtilSignal function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    line : constants.UtilityBusSignal
        Specifies the utility bus signal to assert.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viAssertUtilSignal(session, line)


def buffer_read(library, session, count):
    """Reads data through the use of a formatted I/O read buffer.

    The data can be read from a device or an interface.

    Corresponds to viBufRead function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    count : int
        Number of bytes to be read.

    Returns
    -------
    dbytes
        Data read
    constants.StatusCode
        Return value of the library call.

    """
    buffer = create_string_buffer(count)
    return_count = ViUInt32()
    ret = library.viBufRead(session, buffer, count, byref(return_count))
    return buffer.raw[: return_count.value], ret


def buffer_write(library, session, data):
    """Writes data to a formatted I/O write buffer synchronously.

    Corresponds to viBufWrite function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    data : bytes
        Data to be written.

    Returns
    -------
    int
        number of written bytes
    constants.StatusCode
        return value of the library call.

    """
    return_count = ViUInt32()
    # [ViSession, ViBuf, ViUInt32, ViPUInt32]
    ret = library.viBufWrite(session, data, len(data), byref(return_count))
    return return_count.value, ret


def clear(library, session):
    """Clears a device.

    Corresponds to viClear function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viClear(session)


def close(library, session):
    """Closes the specified session, event, or find list.

    Corresponds to viClose function of the VISA library.

    Parameters
    ---------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : Union[VISASession, VISAEventContext, VISARMSession]
        Unique logical identifier to a session, event, resource manager.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viClose(session)


def disable_event(library, session, event_type, mechanism):
    """Disable notification for an event type(s) via the specified mechanism(s).

    Corresponds to viDisableEvent function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    event_type : constants.EventType
        Event type.
    mechanism : constants.EventMechanism
        Event handling mechanisms to be disabled.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viDisableEvent(session, event_type, mechanism)


def discard_events(library, session, event_type, mechanism):
    """Discard event occurrences for a given type and mechanisms in a session.

    Corresponds to viDiscardEvents function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    event_type : constans.EventType
        Logical event identifier.
    mechanism : constants.EventMechanism
        Specifies event handling mechanisms to be discarded.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viDiscardEvents(session, event_type, mechanism)


def enable_event(library, session, event_type, mechanism, context=None):
    """Enable event occurrences for specified event types and mechanisms in a session.

    Corresponds to viEnableEvent function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    if context is None:
        context = constants.VI_NULL
    elif context != constants.VI_NULL:
        warnings.warn("In enable_event, context will be set VI_NULL.")
        context = constants.VI_NULL  # according to spec VPP-4.3, section 3.7.3.1
    return library.viEnableEvent(session, event_type, mechanism, context)


def _find_next(library, find_list: ViFindList) -> Tuple[str, constants.StatusCode]:
    """Get next resource from the list of resources.

    The list of resources should be obtained from a previous call to find_resources().

    Corresponds to viFindNext function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    find_list :
        Describes a find list. This parameter must be created by find_resources().

    Returns
    -------
    str
        String identifying the location of a device
    constants.StatusCode
        Return value of the library call.

    """
    instrument_description = create_string_buffer(constants.VI_FIND_BUFLEN)
    ret = library.viFindNext(find_list, instrument_description)
    return buffer_to_text(instrument_description), ret


def _find_resources(library, session: typing.VISARMSession, query: str):
    """Queries VISA to locate the resources associated with a specified interface.

    Corresponds to viFindRsrc function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : typing.VISARMSession
        Unique logical identifier to the ResourceManger session
        (unused, just to uniform signatures).
    query : str
        A regular expression followed by an optional logical expression.
        Use '?*' for all.

    Returns
    -------
    ViFindList
        Opaque object to pass to `_find_next` to access the other devices
        resource name.
    int
        Number of identified devices.
    str
        Resource name of the first identified device
    constants.StatusCode
        Return value of the library call.

    """
    find_list = ViFindList()
    return_counter = ViUInt32()
    instrument_description = create_string_buffer(constants.VI_FIND_BUFLEN)

    ret = library.viFindRsrc(
        session, query, byref(find_list), byref(return_counter), instrument_description
    )
    return find_list, return_counter.value, buffer_to_text(instrument_description), ret


def flush(library, session, mask):
    """Retrieves the state of an attribute.

    Corresponds to viGetAttribute function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : Union[VISASession, VISAEventContext]
        Unique logical identifier to a session, event, or find list.
    attribute : Union[constants.ResourceAttribute, constants.EventAttribute]
        Resource or event attribute for which the state query is made.

    Returns
    -------
    Any
        State of the queried attribute for a specified resource
    constants.StatusCode
        Return value of the library call.

    """
    return library.viFlush(session, mask)


def get_attribute(library, session, attribute):
    """Retrieves the state of an attribute.

    Corresponds to viGetAttribute function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : Union[VISASession, VISAEventContext]
        Unique logical identifier to a session, event, or find list.
    attribute : Union[constants.ResourceAttribute, constants.EventAttribute]
        Resource or event attribute for which the state query is made.

    Returns
    -------
    Any
        State of the queried attribute for a specified resource
    constants.StatusCode
        Return value of the library call.

    """
    attr = attributes.AttributesByID[attribute]
    datatype = getattr(types, attr.visa_type)
    if datatype == ViString:
        attribute_state = create_string_buffer(256)
        ret = library.viGetAttribute(session, attribute, attribute_state)
        return buffer_to_text(attribute_state), ret
    # There are only 2 buffer attribute, the one we do not handle if the one
    # to async read that is handled at a higher since we pass the buffer ourself
    elif datatype == ViBuf:
        if attr.visa_name == "VI_ATTR_USB_RECV_INTR_DATA":
            length = get_attribute(
                library, session, constants.VI_ATTR_USB_RECV_INTR_SIZE
            )
            attribute_state = (ViUInt8 * length)()
            ret = library.viGetAttribute(session, attribute, byref(attribute_state))
            return list(attribute_state), ret
        else:
            raise AttributeError("%s cannot be accessed directly" % attr.visa_name)
    else:
        attribute_state = datatype()
        ret = library.viGetAttribute(session, attribute, byref(attribute_state))
        return attribute_state.value, ret


def gpib_command(library, session, data):
    """Write GPIB command bytes on the bus.

    Corresponds to viGpibCommand function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    data : bytes
        Data to write.

    Returns
    -------
    int
        Number of written bytes
    constants.StatusCode
        Return value of the library call.

    """
    return_count = ViUInt32()

    # [ViSession, ViBuf, ViUInt32, ViPUInt32]
    ret = library.viGpibCommand(session, data, len(data), byref(return_count))
    return return_count.value, ret


def gpib_control_atn(library, session, mode):
    """Specifies the state of the ATN line and the local active controller state.

    Corresponds to viGpibControlATN function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    mode : constants.ATNLineOperation
        State of the ATN line and optionally the local active controller state.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viGpibControlATN(session, mode)


def gpib_control_ren(library, session, mode):
    """Controls the state of the GPIB Remote Enable (REN) interface line.

    Optionally the remote/local state of the device can also be set.

    Corresponds to viGpibControlREN function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    mode : constants.RENLineOperation
        State of the REN line and optionally the device remote/local state.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viGpibControlREN(session, mode)


def gpib_pass_control(library, session, primary_address, secondary_address):
    """Tell a GPIB device to become controller in charge (CIC).

    Corresponds to viGpibPassControl function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    return library.viGpibPassControl(session, primary_address, secondary_address)


def gpib_send_ifc(library, session):
    """Pulse the interface clear line (IFC) for at least 100 microseconds.

    Corresponds to viGpibSendIFC function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viGpibSendIFC(session)


def in_8(library, session, space, offset, extended=False):
    """Reads in an 8-bit value from the specified memory space and offset.

    Corresponds to viIn8* function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    value_8 = ViUInt8()
    if extended:
        ret = library.viIn8Ex(session, space, offset, byref(value_8))
    else:
        ret = library.viIn8(session, space, offset, byref(value_8))
    return value_8.value, ret


def in_16(library, session, space, offset, extended=False):
    """Reads in an 16-bit value from the specified memory space and offset.

    Corresponds to viIn16* function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    value_16 = ViUInt16()
    if extended:
        ret = library.viIn16Ex(session, space, offset, byref(value_16))
    else:
        ret = library.viIn16(session, space, offset, byref(value_16))
    return value_16.value, ret


def in_32(library, session, space, offset, extended=False):
    """Reads in an 32-bit value from the specified memory space and offset.

    Corresponds to viIn32* function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    value_32 = ViUInt32()
    if extended:
        ret = library.viIn32Ex(session, space, offset, byref(value_32))
    else:
        ret = library.viIn32(session, space, offset, byref(value_32))
    return value_32.value, ret


def in_64(library, session, space, offset, extended=False):
    """Reads in an 64-bit value from the specified memory space and offset.

    Corresponds to viIn64* function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    value_64 = ViUInt64()
    if extended:
        ret = library.viIn64Ex(session, space, offset, byref(value_64))
    else:
        ret = library.viIn64(session, space, offset, byref(value_64))
    return value_64.value, ret


def install_handler(
    library, session, event_type, handler, user_handle: Any
) -> Tuple[typing.VISAHandler, Any, Any, constants.StatusCode]:
    """Install handlers for event callbacks.

    Corresponds to viInstallHandler function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    status_code : constants.StatusCode
        Return value of the library call

    """
    # Should be Optional[_CData] but that type cannot be imported
    converted_user_handle: object = None
    if user_handle is not None:
        if isinstance(user_handle, int):
            converted_user_handle = c_long(user_handle)
        elif isinstance(user_handle, float):
            converted_user_handle = c_double(user_handle)
        elif isinstance(user_handle, str):
            converted_user_handle = c_wchar_p(user_handle)
        elif isinstance(user_handle, list):
            for element in user_handle:
                if not isinstance(element, int):
                    # Mypy cannot track the fact that the list has to contain float
                    converted_user_handle = (c_double * len(user_handle))(  # type: ignore
                        *tuple(user_handle)
                    )
                    break
            else:
                converted_user_handle = (c_long * len(user_handle))(*tuple(user_handle))
        else:
            try:
                # check if it is already a ctypes
                byref(user_handle)
                converted_user_handle = user_handle
            except TypeError:
                raise TypeError(
                    "Type not allowed as user handle: %s" % type(user_handle)
                )

    with set_user_handle_type(library, converted_user_handle):
        if ctwrapper.WRAP_HANDLER:
            # Wrap the handler to provide a non-wrapper specific interface
            def handler_wrapper(
                ctype_session, ctype_event_type, ctype_event_context, ctype_user_handle
            ):
                handler(
                    ctype_session.value,
                    ctype_event_type,
                    ctype_event_context.value,
                    (
                        ctype_user_handle.contents
                        if ctype_user_handle
                        else ctype_user_handle
                    ),
                )
                return 0

            update_wrapper(handler_wrapper, handler)
        else:
            handler_wrapper = handler

        converted_handler = ViHndlr(handler_wrapper)
        if user_handle is None:
            ret = library.viInstallHandler(session, event_type, converted_handler, None)
        else:
            ret = library.viInstallHandler(
                session,
                event_type,
                converted_handler,
                byref(converted_user_handle),  # type: ignore
            )

    return handler, converted_user_handle, converted_handler, ret


def lock(library, session, lock_type, timeout, requested_key=None):
    """Establishes an access mode to the specified resources.

    Corresponds to viLock function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    if lock_type == constants.AccessModes.exclusive_lock:
        requested_key = None
        access_key = None
    else:
        access_key = create_string_buffer(256)
    ret = library.viLock(session, lock_type, timeout, requested_key, access_key)
    if access_key is None:
        return None, ret
    else:
        return access_key.value, ret


def map_address(
    library, session, map_space, map_base, map_size, access=False, suggested=None
):
    """Maps the specified memory space into the process's address space.

    Corresponds to viMapAddress function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    if access is False:
        access = constants.VI_FALSE
    elif access != constants.VI_FALSE:
        warnings.warn("In enable_event, context will be set VI_NULL.")
        access = constants.VI_FALSE
    address = ViAddr()
    ret = library.viMapAddress(
        session, map_space, map_base, map_size, access, suggested, byref(address)
    )
    return address, ret


def map_trigger(library, session, trigger_source, trigger_destination, mode):
    """Map the specified trigger source line to the specified destination line.

    Corresponds to viMapTrigger function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    return library.viMapTrigger(session, trigger_source, trigger_destination, mode)


def memory_allocation(library, session, size, extended=False):
    """Allocate memory from a resource's memory region.

    Corresponds to viMemAlloc* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    offset = ViBusAddress()
    if extended:
        ret = library.viMemAllocEx(session, size, byref(offset))
    else:
        ret = library.viMemAlloc(session, size, byref(offset))
    return offset, ret


def memory_free(library, session, offset, extended=False):
    """Frees memory previously allocated using the memory_allocation() operation.

    Corresponds to viMemFree* function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    offset : int
        Offset of the memory to free.
    extended : bool, optional
        Use 64 bits offset independent of the platform.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    if extended:
        return library.viMemFreeEx(session, offset)
    else:
        return library.viMemFree(session, offset)


def move(
    library,
    session,
    source_space,
    source_offset,
    source_width,
    destination_space,
    destination_offset,
    destination_width,
    length,
):
    """Moves a block of data.

    Corresponds to viMove function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    return library.viMove(
        session,
        source_space,
        source_offset,
        source_width,
        destination_space,
        destination_offset,
        destination_width,
        length,
    )


def move_asynchronously(
    library,
    session,
    source_space,
    source_offset,
    source_width,
    destination_space,
    destination_offset,
    destination_width,
    length,
):
    """Moves a block of data asynchronously.

    Corresponds to viMoveAsync function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    job_id = ViJobId()
    ret = library.viMoveAsync(
        session,
        source_space,
        source_offset,
        source_width,
        destination_space,
        destination_offset,
        destination_width,
        length,
        byref(job_id),
    )
    return job_id, ret


def move_in_8(library, session, space, offset, length, extended=False):
    """Moves an 8-bit block of data to local memory.

    Corresponds to viMoveIn8* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    status_code : constants.StatusCode
        Return value of the library call.

    """
    buffer_8 = (ViUInt8 * length)()
    if extended:
        ret = library.viMoveIn8Ex(session, space, offset, length, buffer_8)
    else:
        ret = library.viMoveIn8(session, space, offset, length, buffer_8)
    return list(buffer_8), ret


def move_in_16(library, session, space, offset, length, extended=False):
    """Moves an 16-bit block of data to local memory.

    Corresponds to viMoveIn816 functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    status_code : constants.StatusCode
        Return value of the library call.

    """
    buffer_16 = (ViUInt16 * length)()
    if extended:
        ret = library.viMoveIn16Ex(session, space, offset, length, buffer_16)
    else:
        ret = library.viMoveIn16(session, space, offset, length, buffer_16)

    return list(buffer_16), ret


def move_in_32(library, session, space, offset, length, extended=False):
    """Moves an 32-bit block of data to local memory.

    Corresponds to viMoveIn32* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    status_code : constants.StatusCode
        Return value of the library call.

    """
    buffer_32 = (ViUInt32 * length)()
    if extended:
        ret = library.viMoveIn32Ex(session, space, offset, length, buffer_32)
    else:
        ret = library.viMoveIn32(session, space, offset, length, buffer_32)

    return list(buffer_32), ret


def move_in_64(library, session, space, offset, length, extended=False):
    """Moves an 64-bit block of data to local memory.

    Corresponds to viMoveIn8* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    status_code : constants.StatusCode
        Return value of the library call.

    """
    buffer_64 = (ViUInt64 * length)()
    if extended:
        ret = library.viMoveIn64Ex(session, space, offset, length, buffer_64)
    else:
        ret = library.viMoveIn64(session, space, offset, length, buffer_64)

    return list(buffer_64), ret


def move_out_8(library, session, space, offset, length, data, extended=False):
    """Moves an 8-bit block of data from local memory.

    Corresponds to viMoveOut8* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    converted_buffer = (ViUInt8 * length)(*tuple(data))
    if extended:
        return library.viMoveOut8Ex(session, space, offset, length, converted_buffer)
    else:
        return library.viMoveOut8(session, space, offset, length, converted_buffer)


def move_out_16(library, session, space, offset, length, data, extended=False):
    """Moves an 16-bit block of data from local memory.

    Corresponds to viMoveOut16* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    converted_buffer = (ViUInt16 * length)(*tuple(data))
    if extended:
        return library.viMoveOut16Ex(session, space, offset, length, converted_buffer)
    else:
        return library.viMoveOut16(session, space, offset, length, converted_buffer)


def move_out_32(library, session, space, offset, length, data, extended=False):
    """Moves an 32-bit block of data from local memory.

    Corresponds to viMoveOut32* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.


    """
    converted_buffer = (ViUInt32 * length)(*tuple(data))
    if extended:
        return library.viMoveOut32Ex(session, space, offset, length, converted_buffer)
    else:
        return library.viMoveOut32(session, space, offset, length, converted_buffer)


def move_out_64(library, session, space, offset, length, data, extended=False):
    """Moves an 64-bit block of data from local memory.

    Corresponds to viMoveOut64* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    converted_buffer = (ViUInt64 * length)(*tuple(data))
    if extended:
        return library.viMoveOut64Ex(session, space, offset, length, converted_buffer)
    else:
        return library.viMoveOut64(session, space, offset, length, converted_buffer)


# noinspection PyShadowingBuiltins
def open(
    library,
    session,
    resource_name,
    access_mode=constants.AccessModes.no_lock,
    open_timeout=constants.VI_TMO_IMMEDIATE,
):
    """Opens a session to the specified resource.

    Corresponds to viOpen function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    try:
        open_timeout = int(open_timeout)
    except ValueError:
        raise ValueError(
            "open_timeout (%r) must be an integer (or compatible type)" % open_timeout
        )
    out_session = ViSession()

    # [ViSession, ViRsrc, ViAccessMode, ViUInt32, ViPSession]
    # ViRsrc converts from (str, unicode, bytes) to bytes
    ret = library.viOpen(
        session, resource_name, access_mode, open_timeout, byref(out_session)
    )
    return out_session.value, ret


def open_default_resource_manager(library):
    """This function returns a session to the Default Resource Manager resource.

    Corresponds to viOpenDefaultRM function of the VISA library.

    Returns
    -------
    VISARMSession
        Unique logical identifier to a Default Resource Manager session
    constants.StatusCode
        Return value of the library call.

    """
    session = ViSession()
    ret = library.viOpenDefaultRM(byref(session))
    return session.value, ret


def out_8(library, session, space, offset, data, extended=False):
    """Write an 8-bit value to the specified memory space and offset.

    Corresponds to viOut8* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    if extended:
        return library.viOut8Ex(session, space, offset, data)
    else:
        return library.viOut8(session, space, offset, data)


def out_16(library, session, space, offset, data, extended=False):
    """Write a 16-bit value to the specified memory space and offset.

    Corresponds to viOut16* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    if extended:
        return library.viOut16Ex(session, space, offset, data, extended=False)
    else:
        return library.viOut16(session, space, offset, data, extended=False)


def out_32(library, session, space, offset, data, extended=False):
    """Write a 32-bit value to the specified memory space and offset.

    Corresponds to viOut32* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    if extended:
        return library.viOut32Ex(session, space, offset, data)
    else:
        return library.viOut32(session, space, offset, data)


def out_64(library, session, space, offset, data, extended=False):
    """Write a 64-bit value to the specified memory space and offset.

    Corresponds to viOut64* functions of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    if extended:
        return library.viOut64Ex(session, space, offset, data)
    else:
        return library.viOut64(session, space, offset, data)


def parse_resource(library, session, resource_name):
    """Parse a resource string to get the interface information.

    Corresponds to viParseRsrc function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISARMSession
        Resource Manager session (should always be the Default Resource
        Manager for VISA returned from open_default_resource_manager()).
    resource_name : str
            Unique symbolic name of a resource.

    Returns
    -------
    ResourceInfo
        Resource information with interface type and board number
    constants.StatusCode
        Return value of the library call.

    """
    interface_type = ViUInt16()
    interface_board_number = ViUInt16()

    # [ViSession, ViRsrc, ViPUInt16, ViPUInt16]
    # ViRsrc converts from (str, unicode, bytes) to bytes
    ret = library.viParseRsrc(
        session, resource_name, byref(interface_type), byref(interface_board_number)
    )
    return (
        ResourceInfo(
            constants.InterfaceType(interface_type.value),
            interface_board_number.value,
            None,
            None,
            None,
        ),
        ret,
    )


def parse_resource_extended(library, session, resource_name):
    """Parse a resource string to get extended interface information.

    Corresponds to viParseRsrcEx function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISARMSession
        Resource Manager session (should always be the Default Resource
        Manager for VISA returned from open_default_resource_manager()).
    resource_name : str
            Unique symbolic name of a resource.

    Returns
    -------
    ResourceInfo
        Resource information with interface type and board number
    constants.StatusCode
        Return value of the library call.

    """
    interface_type = ViUInt16()
    interface_board_number = ViUInt16()
    resource_class = create_string_buffer(constants.VI_FIND_BUFLEN)
    unaliased_expanded_resource_name = create_string_buffer(constants.VI_FIND_BUFLEN)
    alias_if_exists = create_string_buffer(constants.VI_FIND_BUFLEN)

    # [ViSession, ViRsrc, ViPUInt16, ViPUInt16, ViAChar, ViAChar, ViAChar]
    # ViRsrc converts from (str, unicode, bytes) to bytes
    ret = library.viParseRsrcEx(
        session,
        resource_name,
        byref(interface_type),
        byref(interface_board_number),
        resource_class,
        unaliased_expanded_resource_name,
        alias_if_exists,
    )

    res = [
        buffer_to_text(val)
        for val in (resource_class, unaliased_expanded_resource_name, alias_if_exists)
    ]

    if res[-1] == "":
        res[-1] = None

    return (
        ResourceInfo(
            constants.InterfaceType(interface_type.value),
            interface_board_number.value,
            *res,
        ),
        ret,
    )


def peek_8(library, session, address):
    """Read an 8-bit value from the specified address.

    Corresponds to viPeek8 function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    address : VISAMemoryAddress
        Source address to read the value.

    Returns
    -------
    int
        Data read from bus
    constants.StatusCode
        Return value of the library call.

    """
    value_8 = ViUInt8()
    ret = library.viPeek8(session, address, byref(value_8))
    return value_8.value, ret


def peek_16(library, session, address):
    """Read an 16-bit value from the specified address.

    Corresponds to viPeek16 function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    address : VISAMemoryAddress
        Source address to read the value.

    Returns
    -------
    int
        Data read from bus
    constants.StatusCode
        Return value of the library call.

    """
    value_16 = ViUInt16()
    ret = library.viPeek16(session, address, byref(value_16))
    return value_16.value, ret


def peek_32(library, session, address):
    """Read an 32-bit value from the specified address.

    Corresponds to viPeek32 function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    address : VISAMemoryAddress
        Source address to read the value.

    Returns
    -------
    int
        Data read from bus
    constants.StatusCode
        Return value of the library call.

    """
    value_32 = ViUInt32()
    ret = library.viPeek32(session, address, byref(value_32))
    return value_32.value, ret


def peek_64(library, session, address):
    """Read an 64-bit value from the specified address.

    Corresponds to viPeek64 function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    address : VISAMemoryAddress
        Source address to read the value.

    Returns
    -------
    int
        Data read from bus
    constants.StatusCode
        Return value of the library call.

    """
    value_64 = ViUInt64()
    ret = library.viPeek64(session, address, byref(value_64))
    return value_64.value, ret


def poke_8(library, session, address, data):
    """Write an 8-bit value to the specified address.

    Corresponds to viPoke8 function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    address : VISAMemoryAddress
        Source address to read the value.
    data : int
        Data to write.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viPoke8(session, address, data)


def poke_16(library, session, address, data):
    """Write an 16-bit value to the specified address.

    Corresponds to viPoke16 function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    address : VISAMemoryAddress
        Source address to read the value.
    data : int
        Data to write.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viPoke16(session, address, data)


def poke_32(library, session, address, data):
    """Write an 32-bit value to the specified address.

    Corresponds to viPoke32 function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    address : VISAMemoryAddress
        Source address to read the value.
    data : int
        Data to write.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viPoke32(session, address, data)


def poke_64(library, session, address, data):
    """Write an 64-bit value to the specified address.

    Corresponds to viPoke64 function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    address : VISAMemoryAddress
        Source address to read the value.
    data : int
        Data to write.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viPoke64(session, address, data)


def read(library, session, count):
    """Reads data from device or interface synchronously.

    Corresponds to viRead function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    count : int
        Number of bytes to be read.

    Returns
    -------
    bytes
        Date read
    constants.StatusCode
        Return value of the library call.

    """
    buffer = create_string_buffer(count)
    return_count = ViUInt32()
    ret = library.viRead(session, buffer, count, byref(return_count))
    return buffer.raw[: return_count.value], ret


def read_stb(library, session):
    """Reads a status byte of the service request.

    Corresponds to viReadSTB function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.

    Returns
    -------
    int
        Service request status byte
    constants.StatusCode
        Return value of the library call.

    """
    status = ViUInt16()
    ret = library.viReadSTB(session, byref(status))
    return status.value, ret


def read_to_file(library, session, filename, count):
    """Read data synchronously, and store the transferred data in a file.

    Corresponds to viReadToFile function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    return_count = ViUInt32()
    ret = library.viReadToFile(session, filename, count, return_count)
    return return_count, ret


def set_attribute(library, session, attribute, attribute_state):
    """Set the state of an attribute.

    Corresponds to viSetAttribute function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    attribute : constants.ResourceAttribute
        Attribute for which the state is to be modified.
    attribute_state : Any
        The state of the attribute to be set for the specified object.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viSetAttribute(session, attribute, attribute_state)


def set_buffer(library, session, mask, size):
    """Set the size for the formatted I/O and/or low-level I/O communication buffer(s).

    Corresponds to viSetBuf function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    mask : constants.BufferType
        Specifies the type of buffer.
    size : int
        The size to be set for the specified buffer(s).

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viSetBuf(session, mask, size)


def status_description(library, session, status):
    """Return a user-readable description of the status code passed to the operation.

    Corresponds to viStatusDesc function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    status : constants.StatusCode
        Status code to interpret.

    Returns
    -------
    str
        User-readable string interpretation of the status code.
    constants.StatusCode
        Return value of the library call.

    """
    description = create_string_buffer(256)
    ret = library.viStatusDesc(session, status, description)
    return buffer_to_text(description), ret


def terminate(library, session, degree, job_id):
    """Request a VISA session to terminate normal execution of an operation.

    Corresponds to viTerminate function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    return library.viTerminate(session, degree, job_id)


def uninstall_handler(library, session, event_type, handler, user_handle=None):
    """Uninstall handlers for events.

    Corresponds to viUninstallHandler function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    with set_user_handle_type(library, user_handle):
        if user_handle is not None:
            user_handle = byref(user_handle)
        return library.viUninstallHandler(session, event_type, handler, user_handle)


def unlock(library, session):
    """Relinquish a lock for the specified resource.

    Corresponds to viUnlock function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viUnlock(session)


def unmap_address(library, session):
    """Unmap memory space previously mapped by map_address().

    Corresponds to viUnmapAddress function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viUnmapAddress(session)


def unmap_trigger(library, session, trigger_source, trigger_destination):
    """Undo a previous map between a trigger source line and a destination line.

    Corresponds to viUnmapTrigger function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    trigger_source : constants.InputTriggerLine
        Source line used in previous map.
    trigger_destination : constants.OutputTriggerLine
        Destination line used in previous map.

    Returns
    -------
    constants.StatusCode
        Return value of the library call.

    """
    return library.viUnmapTrigger(session, trigger_source, trigger_destination)


def usb_control_in(
    library,
    session,
    request_type_bitmap_field,
    request_id,
    request_value,
    index,
    length=0,
):
    """Perform a USB control pipe transfer from the device.

    Corresponds to viUsbControlIn function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    buffer = create_string_buffer(length)
    return_count = ViUInt16()
    ret = library.viUsbControlIn(
        session,
        request_type_bitmap_field,
        request_id,
        request_value,
        index,
        length,
        buffer,
        byref(return_count),
    )
    return buffer.raw[: return_count.value], ret


def usb_control_out(
    library,
    session,
    request_type_bitmap_field,
    request_id,
    request_value,
    index,
    data="",
):
    """Perform a USB control pipe transfer to the device.

    Corresponds to viUsbControlOut function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    length = len(data)
    return library.viUsbControlOut(
        session,
        request_type_bitmap_field,
        request_id,
        request_value,
        index,
        length,
        data,
    )


def vxi_command_query(library, session, mode, command):
    """Send the device a miscellaneous command or query and/or retrieves the response to a previous query.

    Corresponds to viVxiCommandQuery function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    response = ViUInt32()
    ret = library.viVxiCommandQuery(session, mode, command, byref(response))
    return response.value, ret


def wait_on_event(library, session, in_event_type, timeout):
    """Wait for an occurrence of the specified event for a given session.

    Corresponds to viWaitOnEvent function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    out_event_type = ViEventType()
    out_context = ViEvent()
    ret = library.viWaitOnEvent(
        session, in_event_type, timeout, byref(out_event_type), byref(out_context)
    )
    return out_event_type.value, out_context, ret


def write(library, session, data):
    """Write data to device or interface synchronously.

    Corresponds to viWrite function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    data : bytes
        Data to be written.

    Returns
    -------
    int
        Number of bytes actually transferred
    constants.StatusCode
        Return value of the library call.

    """
    return_count = ViUInt32()
    # [ViSession, ViBuf, ViUInt32, ViPUInt32]
    ret = library.viWrite(session, data, len(data), byref(return_count))
    return return_count.value, ret


def write_asynchronously(library, session, data):
    """Write data to device or interface asynchronously.

    Corresponds to viWriteAsync function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
    session : VISASession
        Unique logical identifier to a session.
    data : bytes
        Data to be written.

    Returns
    -------
    VISAJobID
        Job ID of this asynchronous write operation
    constants.StatusCode
        Return value of the library call.

    """
    job_id = ViJobId()
    # [ViSession, ViBuf, ViUInt32, ViPJobId]
    ret = library.viWriteAsync(session, data, len(data), byref(job_id))
    return job_id, ret


def write_from_file(library, session, filename, count):
    """Take data from a file and write it out synchronously.

    Corresponds to viWriteFromFile function of the VISA library.

    Parameters
    ----------
    library : ctypes.WinDLL or ctypes.CDLL
        ctypes wrapped library.
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
    constants.StatusCode
        Return value of the library call.

    """
    return_count = ViUInt32()
    ret = library.viWriteFromFile(session, filename, count, return_count)
    return return_count, ret
