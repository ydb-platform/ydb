"""
This module contains the implementation of :class:`can.Message`.

.. note::
    Could use `@dataclass <https://docs.python.org/3.7/library/dataclasses.html>`__
    starting with Python 3.7.
"""

from copy import deepcopy
from math import isinf, isnan
from typing import Any, Optional

from . import typechecking


class Message:  # pylint: disable=too-many-instance-attributes; OK for a dataclass
    """
    The :class:`~can.Message` object is used to represent CAN messages for
    sending, receiving and other purposes like converting between different
    logging formats.

    Messages can use extended identifiers, be remote or error frames, contain
    data and may be associated to a channel.

    Messages are always compared by identity and never by value, because that
    may introduce unexpected behaviour. See also :meth:`~can.Message.equals`.

    :func:`~copy.copy`/:func:`~copy.deepcopy` is supported as well.

    Messages do not support "dynamic" attributes, meaning any others than the
    documented ones, since it uses :obj:`~object.__slots__`.
    """

    __slots__ = (
        "__weakref__",  # support weak references to messages
        "arbitration_id",
        "bitrate_switch",
        "channel",
        "data",
        "dlc",
        "error_state_indicator",
        "is_error_frame",
        "is_extended_id",
        "is_fd",
        "is_remote_frame",
        "is_rx",
        "timestamp",
    )

    def __init__(  # pylint: disable=too-many-locals, too-many-arguments
        self,
        timestamp: float = 0.0,
        arbitration_id: int = 0,
        is_extended_id: bool = True,
        is_remote_frame: bool = False,
        is_error_frame: bool = False,
        channel: Optional[typechecking.Channel] = None,
        dlc: Optional[int] = None,
        data: Optional[typechecking.CanData] = None,
        is_fd: bool = False,
        is_rx: bool = True,
        bitrate_switch: bool = False,
        error_state_indicator: bool = False,
        check: bool = False,
    ):
        """
        To create a message object, simply provide any of the below attributes
        together with additional parameters as keyword arguments to the constructor.

        :param check: By default, the constructor of this class does not strictly check the input.
                      Thus, the caller must prevent the creation of invalid messages or
                      set this parameter to `True`, to raise an Error on invalid inputs.
                      Possible problems include the `dlc` field not matching the length of `data`
                      or creating a message with both `is_remote_frame` and `is_error_frame` set
                      to `True`.

        :raises ValueError:
            If and only if `check` is set to `True` and one or more arguments were invalid
        """
        self.timestamp = timestamp
        self.arbitration_id = arbitration_id
        self.is_extended_id = is_extended_id
        self.is_remote_frame = is_remote_frame
        self.is_error_frame = is_error_frame
        self.channel = channel
        self.is_fd = is_fd
        self.is_rx = is_rx
        self.bitrate_switch = bitrate_switch
        self.error_state_indicator = error_state_indicator

        if data is None or is_remote_frame:
            self.data = bytearray()
        elif isinstance(data, bytearray):
            self.data = data
        else:
            try:
                self.data = bytearray(data)
            except TypeError as error:
                err = f"Couldn't create message from {data} ({type(data)})"
                raise TypeError(err) from error

        if dlc is None:
            self.dlc = len(self.data)
        else:
            self.dlc = dlc

        if check:
            self._check()

    def __str__(self) -> str:
        field_strings = [f"Timestamp: {self.timestamp:>15.6f}"]
        if self.is_extended_id:
            arbitration_id_string = f"{self.arbitration_id:08x}"
        else:
            arbitration_id_string = f"{self.arbitration_id:03x}"
        field_strings.append(f"ID: {arbitration_id_string:>8}")

        flag_string = " ".join(
            [
                "X" if self.is_extended_id else "S",
                "Rx" if self.is_rx else "Tx",
                "E" if self.is_error_frame else " ",
                "R" if self.is_remote_frame else " ",
                "F" if self.is_fd else " ",
                "BS" if self.bitrate_switch else "  ",
                "EI" if self.error_state_indicator else "  ",
            ]
        )

        field_strings.append(flag_string)

        field_strings.append(f"DL: {self.dlc:2d}")
        data_strings = ""
        if self.data is not None:
            data_strings = self.data[: min(self.dlc, len(self.data))].hex(" ")
        if data_strings:  # if not empty
            field_strings.append(data_strings.ljust(24, " "))
        else:
            field_strings.append(" " * 24)

        if (self.data is not None) and (self.data.isalnum()):
            field_strings.append(f"'{self.data.decode('utf-8', 'replace')}'")

        if self.channel is not None:
            try:
                field_strings.append(f"Channel: {self.channel}")
            except UnicodeEncodeError:
                pass

        return "    ".join(field_strings).strip()

    def __len__(self) -> int:
        # return the dlc such that it also works on remote frames
        return self.dlc

    def __bool__(self) -> bool:
        return True

    def __repr__(self) -> str:
        args = [
            f"timestamp={self.timestamp}",
            f"arbitration_id={self.arbitration_id:#x}",
            f"is_extended_id={self.is_extended_id}",
        ]

        if not self.is_rx:
            args.append("is_rx=False")

        if self.is_remote_frame:
            args.append(f"is_remote_frame={self.is_remote_frame}")

        if self.is_error_frame:
            args.append(f"is_error_frame={self.is_error_frame}")

        if self.channel is not None:
            args.append(f"channel={self.channel!r}")

        data = [f"{byte:#02x}" for byte in self.data]
        args += [f"dlc={self.dlc}", f"data=[{', '.join(data)}]"]

        if self.is_fd:
            args.append("is_fd=True")
            args.append(f"bitrate_switch={self.bitrate_switch}")
            args.append(f"error_state_indicator={self.error_state_indicator}")

        return f"can.Message({', '.join(args)})"

    def __format__(self, format_spec: Optional[str]) -> str:
        if not format_spec:
            return self.__str__()
        else:
            raise ValueError("non-empty format_specs are not supported")

    def __bytes__(self) -> bytes:
        return bytes(self.data)

    def __copy__(self) -> "Message":
        return Message(
            timestamp=self.timestamp,
            arbitration_id=self.arbitration_id,
            is_extended_id=self.is_extended_id,
            is_remote_frame=self.is_remote_frame,
            is_error_frame=self.is_error_frame,
            channel=self.channel,
            dlc=self.dlc,
            data=self.data,
            is_fd=self.is_fd,
            is_rx=self.is_rx,
            bitrate_switch=self.bitrate_switch,
            error_state_indicator=self.error_state_indicator,
        )

    def __deepcopy__(self, memo: Optional[dict[int, Any]]) -> "Message":
        return Message(
            timestamp=self.timestamp,
            arbitration_id=self.arbitration_id,
            is_extended_id=self.is_extended_id,
            is_remote_frame=self.is_remote_frame,
            is_error_frame=self.is_error_frame,
            channel=deepcopy(self.channel, memo),
            dlc=self.dlc,
            data=deepcopy(self.data, memo),
            is_fd=self.is_fd,
            is_rx=self.is_rx,
            bitrate_switch=self.bitrate_switch,
            error_state_indicator=self.error_state_indicator,
        )

    def _check(self) -> None:
        """Checks if the message parameters are valid.

        Assumes that the attribute types are already correct.

        :raises ValueError: If and only if one or more attributes are invalid
        """

        if self.timestamp < 0.0:
            raise ValueError("the timestamp may not be negative")
        if isinf(self.timestamp):
            raise ValueError("the timestamp may not be infinite")
        if isnan(self.timestamp):
            raise ValueError("the timestamp may not be NaN")

        if self.is_remote_frame:
            if self.is_error_frame:
                raise ValueError(
                    "a message cannot be a remote and an error frame at the same time"
                )
            if self.is_fd:
                raise ValueError("CAN FD does not support remote frames")

        if self.arbitration_id < 0:
            raise ValueError("arbitration IDs may not be negative")

        if self.is_extended_id:
            if self.arbitration_id >= 0x20000000:
                raise ValueError("Extended arbitration IDs must be less than 2^29")
        elif self.arbitration_id >= 0x800:
            raise ValueError("Normal arbitration IDs must be less than 2^11")

        if self.dlc < 0:
            raise ValueError("DLC may not be negative")
        if self.is_fd:
            if self.dlc > 64:
                raise ValueError(
                    f"DLC was {self.dlc} but it should be <= 64 for CAN FD frames"
                )
        elif self.dlc > 8:
            raise ValueError(
                f"DLC was {self.dlc} but it should be <= 8 for normal CAN frames"
            )

        if self.is_remote_frame:
            if self.data:
                raise ValueError("remote frames may not carry any data")
        elif self.dlc != len(self.data):
            raise ValueError(
                "the DLC and the length of the data must match up for non remote frames"
            )

        if not self.is_fd:
            if self.bitrate_switch:
                raise ValueError("bitrate switch is only allowed for CAN FD frames")
            if self.error_state_indicator:
                raise ValueError(
                    "error state indicator is only allowed for CAN FD frames"
                )

    def equals(
        self,
        other: "Message",
        timestamp_delta: Optional[float] = 1.0e-6,
        check_channel: bool = True,
        check_direction: bool = True,
    ) -> bool:
        """
        Compares a given message with this one.

        :param other: the message to compare with
        :param timestamp_delta: the maximum difference in seconds at which two timestamps are
                                still considered equal or `None` to not compare timestamps
        :param check_channel: whether to compare the message channel
        :param check_direction: whether to compare the messages' directions (Tx/Rx)

        :return: True if and only if the given message equals this one
        """
        # see https://github.com/hardbyte/python-can/pull/413 for a discussion
        # on why a delta of 1.0e-6 was chosen
        return (
            # check for identity first and finish fast
            self is other
            or
            # then check for equality by value
            (
                (
                    timestamp_delta is None
                    or abs(self.timestamp - other.timestamp) <= timestamp_delta
                )
                and (self.is_rx == other.is_rx or not check_direction)
                and self.arbitration_id == other.arbitration_id
                and self.is_extended_id == other.is_extended_id
                and self.dlc == other.dlc
                and self.data == other.data
                and self.is_remote_frame == other.is_remote_frame
                and self.is_error_frame == other.is_error_frame
                and (self.channel == other.channel or not check_channel)
                and self.is_fd == other.is_fd
                and self.bitrate_switch == other.bitrate_switch
                and self.error_state_indicator == other.error_state_indicator
            )
        )
