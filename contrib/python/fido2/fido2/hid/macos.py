# Original work Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modified work Copyright 2020 Yubico AB. All Rights Reserved.
# This file, with modifications, is licensed under the above Apache License.

from __future__ import annotations

from .base import HidDescriptor, CtapHidConnection, FIDO_USAGE_PAGE, FIDO_USAGE

import ctypes
import ctypes.util
import threading
from queue import Queue, Empty

import logging

logger = logging.getLogger(__name__)

# Constants

HID_DEVICE_PROPERTY_VENDOR_ID = b"VendorID"
HID_DEVICE_PROPERTY_PRODUCT_ID = b"ProductID"
HID_DEVICE_PROPERTY_PRODUCT = b"Product"
HID_DEVICE_PROPERTY_SERIAL_NUMBER = b"SerialNumber"
HID_DEVICE_PROPERTY_PRIMARY_USAGE = b"PrimaryUsage"
HID_DEVICE_PROPERTY_PRIMARY_USAGE_PAGE = b"PrimaryUsagePage"
HID_DEVICE_PROPERTY_MAX_INPUT_REPORT_SIZE = b"MaxInputReportSize"
HID_DEVICE_PROPERTY_MAX_OUTPUT_REPORT_SIZE = b"MaxOutputReportSize"
HID_DEVICE_PROPERTY_REPORT_ID = b"ReportID"


# Declare C types
class _CFType(ctypes.Structure):
    pass


class _CFString(_CFType):
    pass


class _CFSet(_CFType):
    pass


class _IOHIDManager(_CFType):
    pass


class _IOHIDDevice(_CFType):
    pass


class _CFRunLoop(_CFType):
    pass


class _CFAllocator(_CFType):
    pass


CF_SET_REF = ctypes.POINTER(_CFSet)
CF_STRING_REF = ctypes.POINTER(_CFString)
CF_TYPE_REF = ctypes.POINTER(_CFType)
CF_RUN_LOOP_REF = ctypes.POINTER(_CFRunLoop)
CF_RUN_LOOP_RUN_RESULT = ctypes.c_int32
CF_ALLOCATOR_REF = ctypes.POINTER(_CFAllocator)
CF_DICTIONARY_REF = ctypes.c_void_p
CF_MUTABLE_DICTIONARY_REF = ctypes.c_void_p
CF_TYPE_ID = ctypes.c_ulong
CF_INDEX = ctypes.c_long
CF_TIME_INTERVAL = ctypes.c_double
CF_STRING_ENCODING = ctypes.c_uint32
CF_STRING_BUILTIN_ENCODINGS_UTF8 = 134217984
IO_RETURN = ctypes.c_uint
IO_HID_REPORT_TYPE = ctypes.c_uint
IO_OPTION_BITS = ctypes.c_uint
IO_OBJECT_T = ctypes.c_uint
MACH_PORT_T = ctypes.c_uint
IO_SERVICE_T = IO_OBJECT_T
IO_REGISTRY_ENTRY_T = IO_OBJECT_T

IO_HID_MANAGER_REF = ctypes.POINTER(_IOHIDManager)
IO_HID_DEVICE_REF = ctypes.POINTER(_IOHIDDevice)

IO_HID_REPORT_CALLBACK = ctypes.CFUNCTYPE(
    None,
    ctypes.py_object,
    IO_RETURN,
    ctypes.c_void_p,
    IO_HID_REPORT_TYPE,
    ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_uint8),
    CF_INDEX,
)
IO_HID_CALLBACK = ctypes.CFUNCTYPE(None, ctypes.py_object, IO_RETURN, ctypes.c_void_p)

# Define C constants
K_CF_NUMBER_SINT32_TYPE = 3
K_CF_ALLOCATOR_DEFAULT = None

K_IO_MASTER_PORT_DEFAULT = 0
K_IO_HID_REPORT_TYPE_OUTPUT = 1
K_IO_RETURN_SUCCESS = 0

K_CF_RUN_LOOP_RUN_STOPPED = 2
K_CF_RUN_LOOP_RUN_TIMED_OUT = 3
K_CF_RUN_LOOP_RUN_HANDLED_SOURCE = 4

# Load relevant libraries
# NOTE: find_library doesn't currently work on Big Sur, requiring the hardcoded paths
iokit = ctypes.cdll.LoadLibrary(
    ctypes.util.find_library("IOKit")
    or "/System/Library/Frameworks/IOKit.framework/IOKit"
)
cf = ctypes.cdll.LoadLibrary(
    ctypes.util.find_library("CoreFoundation")
    or "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation"
)

# Exported constants
K_CF_RUNLOOP_DEFAULT_MODE = CF_STRING_REF.in_dll(cf, "kCFRunLoopDefaultMode")

# Declare C function prototypes
cf.CFSetGetValues.restype = None
cf.CFSetGetValues.argtypes = [CF_SET_REF, ctypes.POINTER(ctypes.c_void_p)]
cf.CFStringCreateWithCString.restype = CF_STRING_REF
cf.CFStringCreateWithCString.argtypes = [
    ctypes.c_void_p,
    ctypes.c_char_p,
    ctypes.c_uint32,
]
cf.CFStringGetCString.restype = ctypes.c_bool
cf.CFStringGetCString.argtypes = [
    CF_TYPE_REF,
    ctypes.c_char_p,
    CF_INDEX,
    CF_STRING_ENCODING,
]
cf.CFGetTypeID.restype = CF_TYPE_ID
cf.CFGetTypeID.argtypes = [CF_TYPE_REF]
cf.CFNumberGetTypeID.restype = CF_TYPE_ID
cf.CFStringGetTypeID.restype = CF_TYPE_ID
cf.CFNumberGetValue.restype = ctypes.c_int
cf.CFRunLoopGetCurrent.restype = CF_RUN_LOOP_REF
cf.CFRunLoopGetCurrent.argtypes = []
cf.CFRunLoopRunInMode.restype = CF_RUN_LOOP_RUN_RESULT
cf.CFRunLoopRunInMode.argtypes = [CF_STRING_REF, CF_TIME_INTERVAL, ctypes.c_bool]
cf.CFRelease.restype = IO_RETURN
cf.CFRelease.argtypes = [CF_TYPE_REF]

iokit.IOObjectRelease.argtypes = [IO_OBJECT_T]

iokit.IOHIDManagerCreate.restype = IO_HID_MANAGER_REF
iokit.IOHIDManagerCreate.argtypes = [CF_ALLOCATOR_REF, IO_OPTION_BITS]
iokit.IOHIDManagerCopyDevices.restype = CF_SET_REF
iokit.IOHIDManagerCopyDevices.argtypes = [IO_HID_MANAGER_REF]
iokit.IOHIDManagerSetDeviceMatching.restype = None
iokit.IOHIDManagerSetDeviceMatching.argtypes = [IO_HID_MANAGER_REF, CF_TYPE_REF]

iokit.IORegistryEntryIDMatching.restype = CF_MUTABLE_DICTIONARY_REF
iokit.IORegistryEntryIDMatching.argtypes = [ctypes.c_uint64]
iokit.IORegistryEntryGetRegistryEntryID.restype = IO_RETURN
iokit.IORegistryEntryGetRegistryEntryID.argtypes = [
    IO_REGISTRY_ENTRY_T,
    ctypes.POINTER(ctypes.c_uint64),
]

iokit.IOHIDDeviceCreate.restype = IO_HID_DEVICE_REF
iokit.IOHIDDeviceCreate.argtypes = [CF_ALLOCATOR_REF, IO_SERVICE_T]
iokit.IOHIDDeviceClose.restype = IO_RETURN
iokit.IOHIDDeviceClose.argtypes = [IO_HID_DEVICE_REF, ctypes.c_uint32]
iokit.IOHIDDeviceScheduleWithRunLoop.restype = None
iokit.IOHIDDeviceScheduleWithRunLoop.argtypes = [
    IO_HID_DEVICE_REF,
    CF_RUN_LOOP_REF,
    CF_STRING_REF,
]
iokit.IOHIDDeviceUnscheduleFromRunLoop.restype = None
iokit.IOHIDDeviceUnscheduleFromRunLoop.argtypes = [
    IO_HID_DEVICE_REF,
    CF_RUN_LOOP_REF,
    CF_STRING_REF,
]
iokit.IOHIDDeviceGetProperty.restype = CF_TYPE_REF
iokit.IOHIDDeviceGetProperty.argtypes = [IO_HID_DEVICE_REF, CF_STRING_REF]
iokit.IOHIDDeviceSetReport.restype = IO_RETURN
iokit.IOHIDDeviceSetReport.argtypes = [
    IO_HID_DEVICE_REF,
    IO_HID_REPORT_TYPE,
    CF_INDEX,
    ctypes.c_void_p,
    CF_INDEX,
]
iokit.IOServiceGetMatchingService.restype = IO_SERVICE_T
iokit.IOServiceGetMatchingService.argtypes = [MACH_PORT_T, CF_DICTIONARY_REF]


def _hid_read_callback(
    read_queue, result, sender, report_type, report_id, report, report_length
):
    """Handles incoming IN report from HID device."""
    del result, sender, report_type, report_id  # Unused by the callback function

    read_queue.put(ctypes.string_at(report, report_length))


# C wrapper around ReadCallback()
# Declared in this scope so it doesn't get GC-ed
REGISTERED_READ_CALLBACK = IO_HID_REPORT_CALLBACK(_hid_read_callback)


def _hid_removal_callback(hid_device, result, sender):
    del result, sender
    cf.CFRunLoopStop(hid_device.run_loop_ref)


REMOVAL_CALLBACK = IO_HID_CALLBACK(_hid_removal_callback)


def _dev_read_thread(hid_device):
    """Binds a device to the thread's run loop, then starts the run loop.

    Args:
    hid_device: The MacOsHidDevice object

    The HID manager requires a run loop to handle Report reads. This thread
    function serves that purpose.
    """

    # Schedule device events with run loop
    hid_device.run_loop_ref = cf.CFRunLoopGetCurrent()
    if not hid_device.run_loop_ref:
        logger.error("Failed to get current run loop")
        return

    iokit.IOHIDDeviceScheduleWithRunLoop(
        hid_device.handle, hid_device.run_loop_ref, K_CF_RUNLOOP_DEFAULT_MODE
    )

    iokit.IOHIDDeviceRegisterRemovalCallback(
        hid_device.handle, REMOVAL_CALLBACK, ctypes.py_object(hid_device)
    )

    # Run the run loop
    run_loop_run_result = cf.CFRunLoopRunInMode(
        K_CF_RUNLOOP_DEFAULT_MODE, 4, True  # Timeout in seconds
    )  # Return after source handled

    # log any unexpected run loop exit
    if run_loop_run_result != K_CF_RUN_LOOP_RUN_HANDLED_SOURCE:
        logger.error("Unexpected run loop exit code: %d", run_loop_run_result)

    # Unschedule from run loop
    iokit.IOHIDDeviceUnscheduleFromRunLoop(
        hid_device.handle, hid_device.run_loop_ref, K_CF_RUNLOOP_DEFAULT_MODE
    )


class MacCtapHidConnection(CtapHidConnection):
    def __init__(self, descriptor):
        self.descriptor = descriptor
        self.handle = _handle_from_path(descriptor.path)

        # Open device
        result = iokit.IOHIDDeviceOpen(self.handle, 0)
        if result != K_IO_RETURN_SUCCESS:
            raise OSError(f"Failed to open device for communication: {result}")

        # Create read queue
        self.read_queue: Queue = Queue()

        # Create and start read thread
        self.run_loop_ref = None

        # Register read callback
        self.in_report_buffer = (ctypes.c_uint8 * descriptor.report_size_in)()
        iokit.IOHIDDeviceRegisterInputReportCallback(
            self.handle,
            self.in_report_buffer,
            self.descriptor.report_size_in,
            REGISTERED_READ_CALLBACK,
            ctypes.py_object(self.read_queue),
        )

    def close(self):
        iokit.IOHIDDeviceRegisterInputReportCallback(
            self.handle,
            self.in_report_buffer,
            self.descriptor.report_size_in,
            ctypes.cast(0, IO_HID_REPORT_CALLBACK),
            None,
        )

    def write_packet(self, packet):
        result = iokit.IOHIDDeviceSetReport(
            self.handle,
            K_IO_HID_REPORT_TYPE_OUTPUT,
            0,
            packet,
            len(packet),
        )

        # Non-zero status indicates failure
        if result != K_IO_RETURN_SUCCESS:
            raise OSError(f"Failed to write report to device: {result}")

    def read_packet(self):
        try:
            return self.read_queue.get(False)
        except Empty:
            read_thread = threading.Thread(target=_dev_read_thread, args=(self,))
            read_thread.start()
            read_thread.join()
            try:
                return self.read_queue.get(False)
            except Empty:
                raise OSError("Failed reading a response")


def get_int_property(dev, key):
    """Reads int property from the HID device."""
    cf_key = cf.CFStringCreateWithCString(None, key, 0)
    type_ref = iokit.IOHIDDeviceGetProperty(dev, cf_key)
    cf.CFRelease(cf_key)
    if not type_ref:
        return None

    if cf.CFGetTypeID(type_ref) != cf.CFNumberGetTypeID():
        raise OSError(f"Expected number type, got {cf.CFGetTypeID(type_ref)}")

    out = ctypes.c_int32()
    ret = cf.CFNumberGetValue(type_ref, K_CF_NUMBER_SINT32_TYPE, ctypes.byref(out))
    if not ret:
        return None

    return out.value


def get_string_property(dev, key):
    """Reads string property from the HID device."""
    cf_key = cf.CFStringCreateWithCString(None, key, 0)
    type_ref = iokit.IOHIDDeviceGetProperty(dev, cf_key)
    cf.CFRelease(cf_key)
    if not type_ref:
        return None

    if cf.CFGetTypeID(type_ref) != cf.CFStringGetTypeID():
        raise OSError(f"Expected string type, got {cf.CFGetTypeID(type_ref)}")

    out = ctypes.create_string_buffer(128)
    ret = cf.CFStringGetCString(
        type_ref, out, ctypes.sizeof(out), CF_STRING_BUILTIN_ENCODINGS_UTF8
    )
    if not ret:
        return None

    try:
        return out.value.decode("utf-8") or None
    except UnicodeDecodeError:
        return None


def get_device_id(handle):
    """Obtains the unique IORegistry entry ID for the device.

    Args:
    handle: reference to the device

    Returns:
    A unique ID for the device, obtained from the IO Registry
    """
    # Obtain device entry ID from IO Registry
    io_service_obj = iokit.IOHIDDeviceGetService(handle)
    entry_id = ctypes.c_uint64()
    result = iokit.IORegistryEntryGetRegistryEntryID(
        io_service_obj, ctypes.byref(entry_id)
    )
    if result != K_IO_RETURN_SUCCESS:
        raise OSError(f"Failed to obtain IORegistry entry ID: {result}")

    return entry_id.value


def _handle_from_path(path):
    # Resolve the path to device handle
    entry_id = ctypes.c_uint64(int(path))
    matching_dict = iokit.IORegistryEntryIDMatching(entry_id)
    device_entry = iokit.IOServiceGetMatchingService(
        K_IO_MASTER_PORT_DEFAULT, matching_dict
    )
    if not device_entry:
        raise OSError(f"Device ID {path} does not match any HID device on the system")

    return iokit.IOHIDDeviceCreate(K_CF_ALLOCATOR_DEFAULT, device_entry)


def open_connection(descriptor):
    return MacCtapHidConnection(descriptor)


def _get_descriptor_from_handle(handle):
    usage_page = get_int_property(handle, HID_DEVICE_PROPERTY_PRIMARY_USAGE_PAGE)
    usage = get_int_property(handle, HID_DEVICE_PROPERTY_PRIMARY_USAGE)
    if usage_page == FIDO_USAGE_PAGE and usage == FIDO_USAGE:
        device_id = get_device_id(handle)
        vid = get_int_property(handle, HID_DEVICE_PROPERTY_VENDOR_ID)
        pid = get_int_property(handle, HID_DEVICE_PROPERTY_PRODUCT_ID)
        product = get_string_property(handle, HID_DEVICE_PROPERTY_PRODUCT)
        serial = get_string_property(handle, HID_DEVICE_PROPERTY_SERIAL_NUMBER)
        size_in = get_int_property(handle, HID_DEVICE_PROPERTY_MAX_INPUT_REPORT_SIZE)
        size_out = get_int_property(handle, HID_DEVICE_PROPERTY_MAX_OUTPUT_REPORT_SIZE)
        return HidDescriptor(
            str(device_id), vid, pid, size_in, size_out, product, serial
        )
    raise ValueError("Not a CTAP device")


def get_descriptor(path):
    return _get_descriptor_from_handle(_handle_from_path(path))


def list_descriptors():
    # Init a HID manager
    hid_mgr = iokit.IOHIDManagerCreate(None, 0)
    if not hid_mgr:
        raise OSError("Unable to obtain HID manager reference")
    try:
        iokit.IOHIDManagerSetDeviceMatching(hid_mgr, None)

        # Get devices from HID manager
        device_set_ref = iokit.IOHIDManagerCopyDevices(hid_mgr)
        if not device_set_ref:
            raise OSError("Failed to obtain devices from HID manager")
        try:
            num = iokit.CFSetGetCount(device_set_ref)
            devices = (IO_HID_DEVICE_REF * num)()
            iokit.CFSetGetValues(device_set_ref, devices)

            # Retrieve and build descriptor dictionaries for each device
            descriptors = []
            for handle in devices:
                try:
                    descriptor = _get_descriptor_from_handle(handle)
                    descriptors.append(descriptor)
                except ValueError:
                    continue  # Not a CTAP device, ignore it
            return descriptors
        finally:
            cf.CFRelease(device_set_ref)
    finally:
        cf.CFRelease(hid_mgr)
