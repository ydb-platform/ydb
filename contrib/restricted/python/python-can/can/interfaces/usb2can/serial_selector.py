""" """

import logging

log = logging.getLogger("can.usb2can")

try:
    import pythoncom
    import win32com.client
except ImportError:
    log.warning(
        "win32com.client module required for usb2can. Install the 'pywin32' package."
    )
    raise


def WMIDateStringToDate(dtmDate) -> str:
    if dtmDate[4] == 0:
        strDateTime = dtmDate[5] + "/"
    else:
        strDateTime = dtmDate[4] + dtmDate[5] + "/"

    if dtmDate[6] == 0:
        strDateTime = strDateTime + dtmDate[7] + "/"
    else:
        strDateTime = strDateTime + dtmDate[6] + dtmDate[7] + "/"
        strDateTime = (
            strDateTime
            + dtmDate[0]
            + dtmDate[1]
            + dtmDate[2]
            + dtmDate[3]
            + " "
            + dtmDate[8]
            + dtmDate[9]
            + ":"
            + dtmDate[10]
            + dtmDate[11]
            + ":"
            + dtmDate[12]
            + dtmDate[13]
        )
    return strDateTime


def find_serial_devices(serial_matcher: str = "") -> list[str]:
    """
    Finds a list of USB devices where the serial number (partially) matches the given string.

    :param serial_matcher:
        only device IDs starting with this string are returned
    """
    serial_numbers = []
    pythoncom.CoInitialize()
    wmi = win32com.client.GetObject("winmgmts:")
    for usb_controller in wmi.InstancesOf("Win32_USBControllerDevice"):
        usb_device = wmi.Get(usb_controller.Dependent)
        if "USB2CAN" in usb_device.Name:
            serial_numbers.append(usb_device.DeviceID.split("\\")[-1])

    if serial_matcher:
        return [sn for sn in serial_numbers if serial_matcher in sn]
    return serial_numbers
