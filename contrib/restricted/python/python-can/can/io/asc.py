"""
Contains handling of ASC logging files.

Example .asc files:
    - https://bitbucket.org/tobylorenz/vector_asc/src/master/src/Vector/ASC/tests/unittests/data/
    - under `test/data/logfile.asc`
"""

import logging
import re
from collections.abc import Generator
from datetime import datetime
from typing import Any, Final, Optional, TextIO, Union

from ..message import Message
from ..typechecking import StringPathLike
from ..util import channel2int, dlc2len, len2dlc
from .generic import TextIOMessageReader, TextIOMessageWriter

CAN_MSG_EXT = 0x80000000
CAN_ID_MASK = 0x1FFFFFFF
BASE_HEX = 16
BASE_DEC = 10
ASC_TRIGGER_REGEX: Final = re.compile(
    r"begin\s+triggerblock\s+\w+\s+(?P<datetime_string>.+)", re.IGNORECASE
)
ASC_MESSAGE_REGEX: Final = re.compile(
    r"\d+\.\d+\s+(\d+\s+(\w+\s+(Tx|Rx)|ErrorFrame)|CANFD)",
    re.ASCII | re.IGNORECASE,
)


logger = logging.getLogger("can.io.asc")


class ASCReader(TextIOMessageReader):
    """
    Iterator of CAN messages from a ASC logging file. Meta data (comments,
    bus statistics, J1939 Transport Protocol messages) is ignored.
    """

    def __init__(
        self,
        file: Union[StringPathLike, TextIO],
        base: str = "hex",
        relative_timestamp: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        :param file: a path-like object or as file-like object to read from
                     If this is a file-like object, is has to opened in text
                     read mode, not binary read mode.
        :param base: Select the base(hex or dec) of id and data.
                     If the header of the asc file contains base information,
                     this value will be overwritten. Default "hex".
        :param relative_timestamp: Select whether the timestamps are
                     `relative` (starting at 0.0) or `absolute` (starting at
                     the system time). Default `True = relative`.
        """
        super().__init__(file, mode="r")

        if not self.file:
            raise ValueError("The given file cannot be None")
        self.base = base
        self._converted_base = self._check_base(base)
        self.relative_timestamp = relative_timestamp
        self.date: Optional[str] = None
        self.start_time = 0.0
        # TODO - what is this used for? The ASC Writer only prints `absolute`
        self.timestamps_format: Optional[str] = None
        self.internal_events_logged = False

    def _extract_header(self) -> None:
        for _line in self.file:
            line = _line.strip()

            datetime_match = re.match(
                r"date\s+\w+\s+(?P<datetime_string>.+)", line, re.IGNORECASE
            )
            base_match = re.match(
                r"base\s+(?P<base>hex|dec)(?:\s+timestamps\s+"
                r"(?P<timestamp_format>absolute|relative))?",
                line,
                re.IGNORECASE,
            )
            comment_match = re.match(r"//.*", line)
            events_match = re.match(
                r"(?P<no_events>no)?\s*internal\s+events\s+logged", line, re.IGNORECASE
            )

            if datetime_match:
                self.date = datetime_match.group("datetime_string")
                self.start_time = (
                    0.0
                    if self.relative_timestamp
                    else self._datetime_to_timestamp(self.date)
                )
                continue

            if base_match:
                base = base_match.group("base")
                timestamp_format = base_match.group("timestamp_format")
                self.base = base
                self._converted_base = self._check_base(self.base)
                self.timestamps_format = timestamp_format or "absolute"
                continue

            if comment_match:
                continue

            if events_match:
                self.internal_events_logged = events_match.group("no_events") is None
                break

            break

    @staticmethod
    def _datetime_to_timestamp(datetime_string: str) -> float:
        # ugly locale independent solution
        month_map = {
            "Jan": 1,
            "Feb": 2,
            "Mar": 3,
            "Apr": 4,
            "May": 5,
            "Jun": 6,
            "Jul": 7,
            "Aug": 8,
            "Sep": 9,
            "Oct": 10,
            "Nov": 11,
            "Dec": 12,
            "Mär": 3,
            "Mai": 5,
            "Okt": 10,
            "Dez": 12,
        }
        for name, number in month_map.items():
            datetime_string = datetime_string.replace(name, str(number).zfill(2))

        datetime_formats = (
            "%m %d %I:%M:%S.%f %p %Y",
            "%m %d %I:%M:%S %p %Y",
            "%m %d %H:%M:%S.%f %Y",
            "%m %d %H:%M:%S %Y",
        )
        for format_str in datetime_formats:
            try:
                return datetime.strptime(datetime_string, format_str).timestamp()
            except ValueError:
                continue

        raise ValueError(f"Incompatible datetime string {datetime_string}")

    def _extract_can_id(self, str_can_id: str, msg_kwargs: dict[str, Any]) -> None:
        if str_can_id[-1:].lower() == "x":
            msg_kwargs["is_extended_id"] = True
            can_id = int(str_can_id[0:-1], self._converted_base)
        else:
            msg_kwargs["is_extended_id"] = False
            can_id = int(str_can_id, self._converted_base)
        msg_kwargs["arbitration_id"] = can_id

    @staticmethod
    def _check_base(base: str) -> int:
        if base not in ["hex", "dec"]:
            raise ValueError('base should be either "hex" or "dec"')
        return BASE_DEC if base == "dec" else BASE_HEX

    def _process_data_string(
        self, data_str: str, data_length: int, msg_kwargs: dict[str, Any]
    ) -> None:
        frame = bytearray()
        data = data_str.split()
        for byte in data[:data_length]:
            frame.append(int(byte, self._converted_base))
        msg_kwargs["data"] = frame

    def _process_classic_can_frame(
        self, line: str, msg_kwargs: dict[str, Any]
    ) -> Message:
        # CAN error frame
        if line.strip()[0:10].lower() == "errorframe":
            # Error Frame
            msg_kwargs["is_error_frame"] = True
        else:
            abr_id_str, direction, rest_of_message = line.split(None, 2)
            msg_kwargs["is_rx"] = direction == "Rx"
            self._extract_can_id(abr_id_str, msg_kwargs)

            if rest_of_message[0].lower() == "r":
                # CAN Remote Frame
                msg_kwargs["is_remote_frame"] = True
                remote_data = rest_of_message.split()
                if len(remote_data) > 1:
                    dlc_str = remote_data[1]
                    if dlc_str.isdigit():
                        msg_kwargs["dlc"] = int(dlc_str, self._converted_base)
            else:
                # Classic CAN Message
                try:
                    # There is data after DLC
                    _, dlc_str, data = rest_of_message.split(None, 2)
                except ValueError:
                    # No data after DLC
                    _, dlc_str = rest_of_message.split(None, 1)
                    data = ""

                dlc = dlc2len(int(dlc_str, self._converted_base))
                msg_kwargs["dlc"] = dlc
                self._process_data_string(data, min(8, dlc), msg_kwargs)

        return Message(**msg_kwargs)

    def _process_fd_can_frame(self, line: str, msg_kwargs: dict[str, Any]) -> Message:
        channel, direction, rest_of_message = line.split(None, 2)
        # See ASCWriter
        msg_kwargs["channel"] = int(channel) - 1
        msg_kwargs["is_rx"] = direction == "Rx"

        # CAN FD error frame
        if rest_of_message.strip()[:10].lower() == "errorframe":
            # Error Frame
            # TODO: maybe use regex to parse BRS, ESI, etc?
            msg_kwargs["is_error_frame"] = True
        else:
            can_id_str, frame_name_or_brs, rest_of_message = rest_of_message.split(
                None, 2
            )

            if frame_name_or_brs.isdigit():
                brs = frame_name_or_brs
                esi, dlc_str, data_length_str, data = rest_of_message.split(None, 3)
            else:
                brs, esi, dlc_str, data_length_str, data = rest_of_message.split(
                    None, 4
                )

            self._extract_can_id(can_id_str, msg_kwargs)
            msg_kwargs["bitrate_switch"] = brs == "1"
            msg_kwargs["error_state_indicator"] = esi == "1"
            dlc = int(dlc_str, self._converted_base)
            data_length = int(data_length_str)
            if data_length == 0:
                # CAN remote Frame
                msg_kwargs["is_remote_frame"] = True
                msg_kwargs["dlc"] = dlc
            else:
                if dlc2len(dlc) != data_length:
                    logger.warning(
                        "DLC vs Data Length mismatch %d[%d] != %d",
                        dlc,
                        dlc2len(dlc),
                        data_length,
                    )
                msg_kwargs["dlc"] = data_length

            self._process_data_string(data, data_length, msg_kwargs)

        return Message(**msg_kwargs)

    def __iter__(self) -> Generator[Message, None, None]:
        self._extract_header()

        for _line in self.file:
            line = _line.strip()

            if trigger_match := ASC_TRIGGER_REGEX.match(line):
                datetime_str = trigger_match.group("datetime_string")
                self.start_time = (
                    0.0
                    if self.relative_timestamp
                    else self._datetime_to_timestamp(datetime_str)
                )
                continue

            # Handle the "Start of measurement" line
            if re.match(r"^\d+\.\d+\s+Start of measurement", line):
                # Skip this line as it's just an indicator
                continue

            if not ASC_MESSAGE_REGEX.match(line):
                # line might be a comment, chip status,
                # J1939 message or some other unsupported event
                continue

            msg_kwargs: dict[str, Union[float, bool, int]] = {}
            try:
                _timestamp, channel, rest_of_message = line.split(None, 2)
                timestamp = float(_timestamp) + self.start_time
                msg_kwargs["timestamp"] = timestamp
                if channel == "CANFD":
                    msg_kwargs["is_fd"] = True
                elif channel.isdigit():
                    # See ASCWriter
                    msg_kwargs["channel"] = int(channel) - 1
                else:
                    # Not a CAN message. Possible values include "statistic", J1939TP
                    continue
            except ValueError:
                # Some other unprocessed or unknown format
                continue

            if "is_fd" not in msg_kwargs:
                msg = self._process_classic_can_frame(rest_of_message, msg_kwargs)
            else:
                msg = self._process_fd_can_frame(rest_of_message, msg_kwargs)
            if msg is not None:
                yield msg

        self.stop()


class ASCWriter(TextIOMessageWriter):
    """Logs CAN data to an ASCII log file (.asc).

    The measurement starts with the timestamp of the first registered message.
    If a message has a timestamp smaller than the previous one or None,
    it gets assigned the timestamp that was written for the last message.
    It the first message does not have a timestamp, it is set to zero.
    """

    FORMAT_MESSAGE = "{channel}  {id:<15} {dir:<4} {dtype} {data}"
    FORMAT_MESSAGE_FD = " ".join(
        [
            "CANFD",
            "{channel:>3}",
            "{dir:<4}",
            "{id:>8}  {symbolic_name:>32}",
            "{brs}",
            "{esi}",
            "{dlc:x}",
            "{data_length:>2}",
            "{data}",
            "{message_duration:>8}",
            "{message_length:>4}",
            "{flags:>8X}",
            "{crc:>8}",
            "{bit_timing_conf_arb:>8}",
            "{bit_timing_conf_data:>8}",
            "{bit_timing_conf_ext_arb:>8}",
            "{bit_timing_conf_ext_data:>8}",
        ]
    )
    FORMAT_DATE = "%a %b %d %H:%M:%S.{} %Y"
    FORMAT_EVENT = "{timestamp: 9.6f} {message}\n"

    def __init__(
        self,
        file: Union[StringPathLike, TextIO],
        channel: int = 1,
        **kwargs: Any,
    ) -> None:
        """
        :param file: a path-like object or as file-like object to write to
                     If this is a file-like object, is has to opened in text
                     write mode, not binary write mode.
        :param channel: a default channel to use when the message does not
                        have a channel set
        """
        if kwargs.get("append", False):
            raise ValueError(
                f"{self.__class__.__name__} is currently not equipped to "
                f"append messages to an existing file."
            )
        super().__init__(file, mode="w")

        self.channel = channel

        # write start of file header
        start_time = self._format_header_datetime(datetime.now())
        self.file.write(f"date {start_time}\n")
        self.file.write("base hex  timestamps absolute\n")
        self.file.write("internal events logged\n")

        # the last part is written with the timestamp of the first message
        self.header_written = False
        self.last_timestamp = 0.0
        self.started = 0.0

    def _format_header_datetime(self, dt: datetime) -> str:
        # Note: CANoe requires that the microsecond field only have 3 digits
        # Since Python strftime only supports microsecond formatters, we must
        # manually include the millisecond portion before passing the format
        # to strftime
        msec = dt.microsecond // 1000 % 1000
        format_w_msec = self.FORMAT_DATE.format(msec)
        return dt.strftime(format_w_msec)

    def stop(self) -> None:
        # This is guaranteed to not be None since we raise ValueError in __init__
        if not self.file.closed:
            self.file.write("End TriggerBlock\n")
        super().stop()

    def log_event(self, message: str, timestamp: Optional[float] = None) -> None:
        """Add a message to the log file.

        :param message: an arbitrary message
        :param timestamp: the absolute timestamp of the event
        """

        if not message:  # if empty or None
            logger.debug("ASCWriter: ignoring empty message")
            return

        # this is the case for the very first message:
        if not self.header_written:
            self.started = self.last_timestamp = timestamp or 0.0

            start_time = datetime.fromtimestamp(self.last_timestamp)
            formatted_date = self._format_header_datetime(start_time)

            self.file.write(f"Begin Triggerblock {formatted_date}\n")
            self.header_written = True
            self.log_event("Start of measurement")  # caution: this is a recursive call!
        # Use last known timestamp if unknown
        if timestamp is None:
            timestamp = self.last_timestamp
        # turn into relative timestamps if necessary
        if timestamp >= self.started:
            timestamp -= self.started
        line = self.FORMAT_EVENT.format(timestamp=timestamp, message=message)
        self.file.write(line)

    def on_message_received(self, msg: Message) -> None:
        channel = channel2int(msg.channel)
        if channel is None:
            channel = self.channel
        else:
            # Many interfaces start channel numbering at 0 which is invalid
            channel += 1

        if msg.is_error_frame:
            self.log_event(f"{channel}  ErrorFrame", msg.timestamp)
            return
        if msg.is_remote_frame:
            dtype = f"r {msg.dlc:x}"  # New after v8.5
            data: str = ""
        else:
            dtype = f"d {msg.dlc:x}"
            data = msg.data.hex(" ").upper()
        arb_id = f"{msg.arbitration_id:X}"
        if msg.is_extended_id:
            arb_id += "x"
        if msg.is_fd:
            flags = 0
            flags |= 1 << 12
            if msg.bitrate_switch:
                flags |= 1 << 13
            if msg.error_state_indicator:
                flags |= 1 << 14
            serialized = self.FORMAT_MESSAGE_FD.format(
                channel=channel,
                id=arb_id,
                dir="Rx" if msg.is_rx else "Tx",
                symbolic_name="",
                brs=1 if msg.bitrate_switch else 0,
                esi=1 if msg.error_state_indicator else 0,
                dlc=len2dlc(msg.dlc),
                data_length=len(msg.data),
                data=data,
                message_duration=0,
                message_length=0,
                flags=flags,
                crc=0,
                bit_timing_conf_arb=0,
                bit_timing_conf_data=0,
                bit_timing_conf_ext_arb=0,
                bit_timing_conf_ext_data=0,
            )
        else:
            serialized = self.FORMAT_MESSAGE.format(
                channel=channel,
                id=arb_id,
                dir="Rx" if msg.is_rx else "Tx",
                dtype=dtype,
                data=data,
            )
        self.log_event(serialized, msg.timestamp)
