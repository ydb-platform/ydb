# -*- coding: utf-8 -*-
"""Serial Session implementation using PyUSB.

See the following link for more information about USB.

http://www.beyondlogic.org/usbnutshell/usb5.shtml

This file is an offspring of the Lantz Project.

:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from fnmatch import fnmatch

import usb
from usb.util import find_descriptor as usb_find_desc

ClassCodes = {
    0x00: ("Device", "Use class information in the Interface Descriptors"),
    0x01: ("Interface", "Audio"),
    0x02: ("Both", "Communications and CDC Control"),
    0x03: ("Interface", "HID (Human Interface Device)"),
    0x05: ("Interface", "Physical"),
    0x06: ("Interface", "Image"),
    0x07: ("Interface", "Printer"),
    0x08: ("Interface", "Mass Storage"),
    0x09: ("Device", "Hub"),
    0x0A: ("Interface", "CDC-Data"),
    0x0B: ("Interface", "Smart Card"),
    0x0D: ("Interface", "Content Security"),
    0x0E: ("Interface", "Video"),
    0x0F: ("Interface", "Personal Healthcare"),
    0x10: ("Interface", "Audio/Video Devices"),
    0xDC: ("Both", "Diagnostic Device"),
    0xE0: ("Interface", "Wireless Controller"),
    0xEF: ("Both", "Miscellaneous"),
    0xFE: ("Interface", "Application Specific"),
    0xFF: ("Both", "Vendor Specific"),
}

# None is 0xxx
AllCodes = {
    (0x00, 0x00, 0x00): "Use class code info from Interface Descriptors",
    (0x01, None, None): "Audio device",
    (0x02, None, None): "Communication device class",
    (0x03, None, None): "HID device class",
    (0x05, None, None): "Physical device class",
    (0x06, 0x01, 0x01): "Still Imaging device",
    (0x07, None, None): "Printer device",
    (0x08, None, None): "Mass Storage device",
    (0x09, 0x00, 0x00): "Full speed Hub",
    (0x09, 0x00, 0x01): "Hi-speed hub with single TT",
    (0x09, 0x00, 0x02): "Hi-speed hub with multiple TTs",
    (0x0A, None, None): "CDC data device",
    (0x0B, None, None): "Smart Card device",
    (0x0D, 0x00, 0x00): "Content Security device",
    (0x0E, None, None): "Video device",
    (0x0F, None, None): "Personal Healthcare device",
    (0x10, 0x01, 0x00): "Control Interface",
    (0x10, 0x02, 0x00): "Data Video Streaming Interface",
    (0x10, 0x03, 0x00): "VData Audio Streaming Interface",
    (0xDC, 0x01, 0x01): "USB2 Compliance Device",
    (0xE0, 0x01, 0x01): "Bluetooth Programming Interface.",
    (0xE0, 0x01, 0x02): "UWB Radio Control Interface.",
    (0xE0, 0x01, 0x03): "Remote NDIS",
    (0xE0, 0x01, 0x04): "Bluetooth AMP Controller.",
    (0xE0, 0x2, 0x01): "Host Wire Adapter Control/Data interface.",
    (0xE0, 0x2, 0x02): "Device Wire Adapter Control/Data interface.",
    (0xE0, 0x2, 0x03): "Device Wire Adapter Isochronous interface.",
    (0xEF, 0x01, 0x01): "Active Sync device.",
    (0xEF, 0x01, 0x02): "Palm Sync. This class code can be used in either "
    "Device or Interface Descriptors.",
    (0xEF, 0x02, 0x01): "Interface Association Descriptor.",
    (0xEF, 0x02, 0x02): "Wire Adapter Multifunction Peripheral programming interface.",
    (0xEF, 0x03, 0x01): "Cable Based Association Framework.",
    (0xEF, 0x04, 0x01): "RNDIS over Ethernet. Connecting a host to the Internet via "
    "Ethernet mobile device. The device appears to the host as an"
    "Ethernet gateway device. This class code may only be used in "
    "Interface Descriptors.",
    (0xEF, 0x04, 0x02): "RNDIS over WiFi. Connecting a host to the Internet via WiFi "
    "enabled mobile device.  The device represents itself to the host"
    "as an 802.11 compliant network device. This class code may only"
    "be used in Interface Descriptors.",
    (0xEF, 0x04, 0x03): "RNDIS over WiMAX. Connecting a host to the Internet via WiMAX "
    "enabled mobile device.  The device is represented to the host "
    "as an 802.16 network device. This class code may only be used "
    "in Interface Descriptors.",
    (
        0xEF,
        0x04,
        0x04,
    ): "RNDIS over WWAN. Connecting a host to the Internet via a device "
    "using mobile broadband, i.e. WWAN (GSM/CDMA). This class code may "
    "only be used in Interface Descriptors.",
    (
        0xEF,
        0x04,
        0x05,
    ): "RNDIS for Raw IPv4. Connecting a host to the Internet using raw "
    "IPv4 via non-Ethernet mobile device.  Devices that provide raw "
    "IPv4, not in an Ethernet packet, may use this form to in lieu of "
    "other stock types. "
    "This class code may only be used in Interface Descriptors.",
    (
        0xEF,
        0x04,
        0x06,
    ): "RNDIS for Raw IPv6. Connecting a host to the Internet using raw "
    "IPv6 via non-Ethernet mobile device.  Devices that provide raw "
    "IPv6, not in an Ethernet packet, may use this form to in lieu of "
    "other stock types. "
    "This class code may only be used in Interface Descriptors.",
    (
        0xEF,
        0x04,
        0x07,
    ): "RNDIS for GPRS. Connecting a host to the Internet over GPRS mobile "
    "device using the device,Äôs cellular radio.",
    (0xEF, 0x05, 0x00): "USB3 Vision Control Interface",
    (0xEF, 0x05, 0x01): "USB3 Vision Event Interface",
    (0xEF, 0x05, 0x02): "USB3 Vision Streaming Interface",
    (0xFE, 0x01, 0x01): "Device Firmware Upgrade.",
    (0xFE, 0x02, 0x00): "IRDA Bridge device.",
    (0xFE, 0x03, 0x00): "USB Test and Measurement Device.",
    (
        0xFE,
        0x03,
        0x01,
    ): "USB Test and Measurement Device conforming to the USBTMC USB488 Subclass",
    (0xFF, None, None): "Vendor specific",
}


def ep_attributes(ep):
    c = ep.bmAttributes
    attrs = []
    tp = c & usb.ENDPOINT_TYPE_MASK
    if tp == usb.ENDPOINT_TYPE_CONTROL:
        attrs.append("Control")
    elif tp == usb.ENDPOINT_TYPE_ISOCHRONOUS:
        attrs.append("Isochronous")
    elif tp == usb.ENDPOINT_TYPE_BULK:
        attrs.append("Bulk")
    elif tp == usb.ENDPOINT_TYPE_INTERRUPT:
        attrs.append("Interrupt")

    sync = (c & 12) >> 2
    if sync == 0:
        attrs.append("No sync")
    elif sync == 1:
        attrs.append("Async")
    elif sync == 2:
        attrs.append("Adaptive")
    elif sync == 3:
        attrs.append("Sync")
    usage = (c & 48) >> 4
    if usage == 0:
        attrs.append("Data endpoint")
    elif usage == 1:
        attrs.append("Feedback endpoint")
    elif usage == 2:
        attrs.append("Subordinate Feedback endpoint")
    elif usage == 3:
        attrs.append("Reserved")

    return ", ".join(attrs)


def find_devices(
    vendor=None, product=None, serial_number=None, custom_match=None, **kwargs
):
    """Find connected USB devices matching certain keywords.

    Wildcards can be used for vendor, product and serial_number.

    :param vendor: name or id of the vendor (manufacturer)
    :param product: name or id of the product
    :param serial_number: serial number.
    :param custom_match: callable returning True or False that takes a device as only input.
    :param kwargs: other properties to match. See usb.core.find
    :return:
    """
    kwargs = kwargs or {}
    attrs = {}
    if isinstance(vendor, str):
        attrs["manufacturer"] = vendor
    elif vendor is not None:
        kwargs["idVendor"] = vendor

    if isinstance(product, str):
        attrs["product"] = product
    elif product is not None:
        kwargs["idProduct"] = product

    if serial_number:
        attrs["serial_number"] = str(serial_number)

    if attrs:

        def cm(dev):
            if custom_match is not None and not custom_match(dev):
                return False
            for attr, pattern in attrs.items():
                try:
                    value = getattr(dev, attr)
                except (NotImplementedError, ValueError):
                    return False
                if not fnmatch(value.lower(), pattern.lower()):
                    return False
            return True

    else:
        cm = custom_match

    try:
        devices = usb.core.find(find_all=True, custom_match=cm, **kwargs)
    except usb.core.NoBackendError as e1:
        try:
            import libusb_package

            devices = libusb_package.find(find_all=True, custom_match=cm, **kwargs)
        except ImportError as e2:
            raise e1 from e2

    return devices


def find_interfaces(device, **kwargs):
    """
    :param device:
    :return:
    """
    interfaces = []
    try:
        for cfg in device:
            try:
                interfaces.extend(usb_find_desc(cfg, find_all=True, **kwargs))
            except Exception:
                pass
    except Exception:
        pass
    return interfaces


def find_endpoint(interface, direction, type):
    ep = usb_find_desc(
        interface,
        custom_match=lambda e: usb.util.endpoint_direction(e.bEndpointAddress)
        == direction
        and usb.util.endpoint_type(e.bmAttributes) == type,
    )
    return ep


def _patch_endpoint(ep, log_func=print):
    _read = ep.read
    _write = ep.write

    def new_read(*args, **kwargs):
        log_func("---")
        log_func("reading from {}".format(ep.bEndpointAddress))
        log_func("args: {}".format(args))
        log_func("kwargs: {}".format(kwargs))
        ret = _read(*args, **kwargs)
        log_func("returned", ret)
        log_func("---")
        return ret

    def new_write(*args, **kwargs):
        log_func("---")
        log_func("writing to {}".format(ep.bEndpointAddress))
        log_func("args: {}".format(args))
        log_func("kwargs: {}".format(kwargs))
        ret = _write(*args, **kwargs)
        log_func("returned", ret)
        log_func("---")
        return ret

    ep.read = new_read
    ep.write = new_write
