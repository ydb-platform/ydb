# -*- coding: utf-8 -*-
"""Serial Session implementation using PyUSB.


:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import errno
import logging
import os
import sys
import traceback
from typing import Any, List, Tuple, Type, Union

from pyvisa import attributes, constants
from pyvisa.constants import ResourceAttribute, StatusCode
from pyvisa.rname import USBInstr, USBRaw

from .common import LOGGER
from .sessions import Session, UnknownAttribute

try:
    import usb

    from .protocols import usbraw, usbtmc, usbutil
except ImportError as e:
    msg = "Please install PyUSB to use this resource type.\n%s"
    Session.register_unavailable(constants.InterfaceType.usb, "INSTR", msg % e)
    Session.register_unavailable(constants.InterfaceType.usb, "RAW", msg % e)
    raise

try:
    _ = usb.core.find()
except Exception as e1:
    try:
        import libusb_package

        _ = libusb_package.find()
    except Exception as e2:
        msg = (
            "PyUSB does not seem to be properly installed.\n"
            "Please refer to the PyUSB documentation and \n"
            "install a suitable backend like \n"
            "libusb 0.1, libusb 1.0, libusbx, \n"
            "libusb-win32 or OpenUSB. If you do not have \n"
            "administrator/root privileges, you may try \n"
            'installing the "libusb-package" Python \n'
            "package to provide the necessary backend.\n%s\n%s" % (e2, e1)
        )
        Session.register_unavailable(constants.InterfaceType.usb, "INSTR", msg)
        Session.register_unavailable(constants.InterfaceType.usb, "RAW", msg)
        raise


class USBTimeoutException(Exception):
    """Exception used internally to indicate USB timeout."""

    pass


class USBSession(Session):
    """Base class for drivers working with usb devices via usb port using pyUSB."""

    # Override parsed to take into account the fact that this class is only used
    # for a specific kind of resource
    parsed: Union[USBInstr, USBRaw]

    #: Class to use when instantiating the interface
    _intf_cls: Union[Type[usbraw.USBRawDevice], Type[usbtmc.USBTMC]]

    @staticmethod
    def list_resources() -> List[str]:
        """Return list of resources for this type of USB device."""
        raise NotImplementedError

    @classmethod
    def get_low_level_info(cls) -> str:
        try:
            ver = usb.__version__
        except AttributeError:
            ver = "N/A"

        try:
            # noinspection PyProtectedMember
            backend = usb.core.find()._ctx.backend.__class__.__module__.split(".")[-1]
        except Exception:
            try:
                backend = libusb_package.find()._ctx.backend.__class__.__module__.split(
                    "."
                )[-1]
            except Exception:
                backend = "N/A"

        return "via PyUSB (%s). Backend: %s" % (ver, backend)

    def after_parsing(self) -> None:
        self.interface = self._intf_cls(
            int(self.parsed.manufacturer_id, 0),
            int(self.parsed.model_code, 0),
            self.parsed.serial_number,
        )

        self.attrs.update(
            {
                ResourceAttribute.manufacturer_id: int(self.parsed.manufacturer_id, 0),
                ResourceAttribute.model_code: int(self.parsed.model_code, 0),
                ResourceAttribute.usb_serial_number: self.parsed.serial_number,
                ResourceAttribute.usb_interface_number: int(
                    self.parsed.usb_interface_number
                ),
            }
        )

        for name, attr in (
            ("SEND_END_EN", ResourceAttribute.send_end_enabled),
            ("SUPPRESS_END_EN", ResourceAttribute.suppress_end_enabled),
            ("TERMCHAR", ResourceAttribute.termchar),
            ("TERMCHAR_EN", ResourceAttribute.termchar_enabled),
        ):
            attribute = getattr(constants, "VI_ATTR_" + name)
            self.attrs[attr] = attributes.AttributesByID[attribute].default

        # Force setting the timeout to get the proper value
        self.set_attribute(
            ResourceAttribute.timeout_value,
            attributes.AttributesByID[attribute].default,
        )

    def _get_timeout(self, attribute: ResourceAttribute) -> Tuple[int, StatusCode]:
        if self.interface:
            if self.interface.timeout == 2**32 - 1:
                self.timeout = None
            else:
                self.timeout = self.interface.timeout / 1000
        return super(USBSession, self)._get_timeout(attribute)

    def _set_timeout(self, attribute: ResourceAttribute, value: int) -> StatusCode:
        status = super(USBSession, self)._set_timeout(attribute, value)
        timeout = int(self.timeout * 1000) if self.timeout else 2**32 - 1
        timeout = min(timeout, 2**32 - 1)
        if self.interface:
            self.interface.timeout = timeout
        return status

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

        def _usb_reader():
            """Data reader identifying usb timeout exception."""
            try:
                return self.interface.read(count)
            except usb.USBError as exc:
                if exc.errno in (errno.ETIMEDOUT, -errno.ETIMEDOUT):
                    raise USBTimeoutException()
                raise

        supress_end_en, _ = self.get_attribute(ResourceAttribute.suppress_end_enabled)

        term_char, _ = self.get_attribute(ResourceAttribute.termchar)
        term_char_en, _ = self.get_attribute(ResourceAttribute.termchar_enabled)

        return self._read(
            _usb_reader,
            count,
            lambda current: True,  # USB always returns a complete message
            supress_end_en,
            term_char,
            term_char_en,
            USBTimeoutException,
        )

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
        send_end, _ = self.get_attribute(ResourceAttribute.send_end_enabled)

        count = self.interface.write(data)

        return count, StatusCode.success

    def close(self):
        self.interface.close()
        return StatusCode.success

    def _get_attribute(
        self, attribute: constants.ResourceAttribute
    ) -> Tuple[Any, StatusCode]:
        """Get the value for a given VISA attribute for this session.

        Use to implement custom logic for attributes.

        Parameters
        ----------
        attribute : ResourceAttribute
            Attribute for which the state query is made

        Returns
        -------
        Any
            State of the queried attribute for a specified resource
        StatusCode
            Return value of the library call.

        """
        raise UnknownAttribute(attribute)

    def _set_attribute(
        self, attribute: constants.ResourceAttribute, attribute_state: Any
    ) -> StatusCode:
        """Sets the state of an attribute.

        Corresponds to viSetAttribute function of the VISA library.

        Parameters
        ----------
        attribute : constants.ResourceAttribute
            Attribute for which the state is to be modified. (Attributes.*)
        attribute_state : Any
            The state of the attribute to be set for the specified object.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        raise UnknownAttribute(attribute)


@Session.register(constants.InterfaceType.usb, "INSTR")
class USBInstrSession(USBSession):
    """Class for USBTMC devices."""

    # Override parsed to take into account the fact that this class is only used
    # for a specific kind of resource
    parsed: USBInstr

    #: Class to use when instantiating the interface
    _intf_cls = usbtmc.USBTMC

    @staticmethod
    def list_resources() -> List[str]:
        out = []
        fmt = (
            "USB%(board)s::%(manufacturer_id)s::%(model_code)s::"
            "%(serial_number)s::%(usb_interface_number)s::INSTR"
        )
        for dev in usbtmc.find_tmc_devices():
            intfc = usbutil.find_interfaces(
                dev, bInterfaceClass=0xFE, bInterfaceSubClass=3
            )
            try:
                intfc = intfc[0].index
            except (IndexError, AttributeError):
                intfc = 0

            try:
                serial = dev.serial_number
            except (NotImplementedError, ValueError) as err:
                msg = (
                    "Found a USB INSTR device whose serial number cannot be read."
                    " The partial VISA resource name is: " + fmt
                )
                LOGGER.warning(
                    msg,
                    {
                        "board": 0,
                        "manufacturer_id": dev.idVendor,
                        "model_code": dev.idProduct,
                        "serial_number": "???",
                        "usb_interface_number": intfc,
                    },
                )
                logging_level = LOGGER.getEffectiveLevel()
                if logging_level <= logging.DEBUG:
                    LOGGER.debug("Error while reading serial number", exc_info=err)
                elif logging_level <= logging.INFO:
                    if exc_strs := traceback.format_exception_only(err):
                        LOGGER.info(
                            "Error raised from underlying module (pyusb): %s",
                            exc_strs[0].strip(),
                        )

                # Check permissions on Linux
                if sys.platform.startswith("linux"):
                    dev_path = f"/dev/bus/usb/{dev.bus:03d}/{dev.address:03d}"
                    if os.path.exists(dev_path) and not os.access(dev_path, os.O_RDWR):
                        missing_perms = []
                        if not os.access(dev_path, os.O_RDONLY):
                            missing_perms.append("read from")
                        if not os.access(dev_path, os.O_WRONLY):
                            missing_perms.append("write to")
                        missing_perms_str = " or ".join(missing_perms)
                        LOGGER.warning(
                            "User does not have permission to %s %s, so the above "
                            "USB INSTR device cannot be used by pyvisa; see"
                            " https://pyvisa.readthedocs.io/projects/pyvisa-py/en/latest/installation.html"
                            " for more info.",
                            missing_perms_str,
                            dev_path,
                        )

                continue

            out.append(
                fmt
                % {
                    "board": 0,
                    "manufacturer_id": dev.idVendor,
                    "model_code": dev.idProduct,
                    "serial_number": serial,
                    "usb_interface_number": intfc,
                }
            )
        return out


@Session.register(constants.InterfaceType.usb, "RAW")
class USBRawSession(USBSession):
    """Class for RAW devices."""

    # Override parsed to take into account the fact that this class is only used
    # for a specific kind of resource
    parsed: USBRaw

    #: Class to use when instantiating the interface
    _intf_cls = usbraw.USBRawDevice

    @staticmethod
    def list_resources() -> List[str]:
        out = []
        fmt = (
            "USB%(board)s::%(manufacturer_id)s::%(model_code)s::"
            "%(serial_number)s::%(usb_interface_number)s::RAW"
        )
        for dev in usbraw.find_raw_devices():
            intfc = usbutil.find_interfaces(dev, bInterfaceClass=0xFF)
            try:
                intfc = intfc[0].index
            except (IndexError, AttributeError):
                intfc = 0

            try:
                serial = dev.serial_number
            except (NotImplementedError, ValueError, usb.USBError):
                msg = (
                    "Found a USB RAW device whose serial number cannot be read."
                    " The partial VISA resource name is: " + fmt
                )
                LOGGER.warning(
                    msg,
                    {
                        "board": 0,
                        "manufacturer_id": dev.idVendor,
                        "model_code": dev.idProduct,
                        "serial_number": "???",
                        "usb_interface_number": intfc,
                    },
                )
                continue

            out.append(
                fmt
                % {
                    "board": 0,
                    "manufacturer_id": dev.idVendor,
                    "model_code": dev.idProduct,
                    "serial_number": serial,
                    "usb_interface_number": intfc,
                }
            )
        return out
