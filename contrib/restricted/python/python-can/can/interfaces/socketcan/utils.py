"""
Defines common socketcan functions.
"""

import errno
import json
import logging
import os
import struct
import subprocess
import sys
from typing import Optional

from can import typechecking
from can.interfaces.socketcan.constants import CAN_EFF_FLAG

log = logging.getLogger(__name__)


def pack_filters(can_filters: Optional[typechecking.CanFilters] = None) -> bytes:
    if can_filters is None:
        # Pass all messages
        can_filters = [{"can_id": 0, "can_mask": 0}]

    can_filter_fmt = f"={2 * len(can_filters)}I"
    filter_data = []
    for can_filter in can_filters:
        can_id = can_filter["can_id"]
        can_mask = can_filter["can_mask"]
        if "extended" in can_filter:
            # Match on either 11-bit OR 29-bit messages instead of both
            can_mask |= CAN_EFF_FLAG
            if can_filter["extended"]:
                can_id |= CAN_EFF_FLAG
        filter_data.append(can_id)
        filter_data.append(can_mask)

    return struct.pack(can_filter_fmt, *filter_data)


def find_available_interfaces() -> list[str]:
    """Returns the names of all open can/vcan interfaces

    The function calls the ``ip link list`` command. If the lookup fails, an error
    is logged to the console and an empty list is returned.

    :return: The list of available and active CAN interfaces or an empty list of the command failed
    """
    if sys.platform != "linux":
        return []

    try:
        command = ["ip", "-json", "link", "list", "up"]
        output_str = subprocess.check_output(command, text=True)
    except Exception:  # pylint: disable=broad-except
        # subprocess.CalledProcessError is too specific
        log.exception("failed to fetch opened can devices from ip link")
        return []

    try:
        output_json = json.loads(output_str)
    except json.JSONDecodeError:
        log.exception("Failed to parse ip link JSON output: %s", output_str)
        return []

    log.debug(
        "find_available_interfaces(): detected these interfaces (before filtering): %s",
        output_json,
    )

    interfaces = [i["ifname"] for i in output_json if i.get("link_type") == "can"]
    return interfaces


def error_code_to_str(code: Optional[int]) -> str:
    """
    Converts a given error code (errno) to a useful and human readable string.

    :param code: a possibly invalid/unknown error code
    :returns: a string explaining and containing the given error code, or a string
              explaining that the errorcode is unknown if that is the case
    """
    name = errno.errorcode.get(code, "UNKNOWN")  # type: ignore
    description = os.strerror(code) if code is not None else "NO DESCRIPTION AVAILABLE"

    return f"{name} (errno {code}): {description}"
