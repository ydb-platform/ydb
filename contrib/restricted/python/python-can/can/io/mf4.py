"""
Contains handling of MF4 logging files.

MF4 files represent Measurement Data Format (MDF) version 4 as specified by
the ASAM MDF standard (see https://www.asam.net/standards/detail/mdf/)
"""

import abc
import heapq
import logging
from collections.abc import Generator, Iterator
from datetime import datetime
from hashlib import md5
from io import BufferedIOBase, BytesIO
from pathlib import Path
from typing import Any, BinaryIO, Optional, Union, cast

from ..message import Message
from ..typechecking import StringPathLike
from ..util import channel2int, len2dlc
from .generic import BinaryIOMessageReader, BinaryIOMessageWriter

logger = logging.getLogger("can.io.mf4")

try:
    import asammdf
    import numpy as np
    from asammdf import Signal, Source
    from asammdf.blocks.mdf_v4 import MDF4
    from asammdf.blocks.v4_blocks import ChannelGroup, SourceInformation
    from asammdf.blocks.v4_constants import BUS_TYPE_CAN, FLAG_CG_BUS_EVENT, SOURCE_BUS
    from asammdf.mdf import MDF

    STD_DTYPE = np.dtype(
        [
            ("CAN_DataFrame.BusChannel", "<u1"),
            ("CAN_DataFrame.ID", "<u4"),
            ("CAN_DataFrame.IDE", "<u1"),
            ("CAN_DataFrame.DLC", "<u1"),
            ("CAN_DataFrame.DataLength", "<u1"),
            ("CAN_DataFrame.DataBytes", "(64,)u1"),
            ("CAN_DataFrame.Dir", "<u1"),
            ("CAN_DataFrame.EDL", "<u1"),
            ("CAN_DataFrame.BRS", "<u1"),
            ("CAN_DataFrame.ESI", "<u1"),
        ]
    )

    ERR_DTYPE = np.dtype(
        [
            ("CAN_ErrorFrame.BusChannel", "<u1"),
            ("CAN_ErrorFrame.ID", "<u4"),
            ("CAN_ErrorFrame.IDE", "<u1"),
            ("CAN_ErrorFrame.DLC", "<u1"),
            ("CAN_ErrorFrame.DataLength", "<u1"),
            ("CAN_ErrorFrame.DataBytes", "(64,)u1"),
            ("CAN_ErrorFrame.Dir", "<u1"),
            ("CAN_ErrorFrame.EDL", "<u1"),
            ("CAN_ErrorFrame.BRS", "<u1"),
            ("CAN_ErrorFrame.ESI", "<u1"),
        ]
    )

    RTR_DTYPE = np.dtype(
        [
            ("CAN_RemoteFrame.BusChannel", "<u1"),
            ("CAN_RemoteFrame.ID", "<u4"),
            ("CAN_RemoteFrame.IDE", "<u1"),
            ("CAN_RemoteFrame.DLC", "<u1"),
            ("CAN_RemoteFrame.DataLength", "<u1"),
            ("CAN_RemoteFrame.Dir", "<u1"),
        ]
    )
except ImportError:
    asammdf = None
    MDF4 = None
    Signal = None


CAN_MSG_EXT = 0x80000000
CAN_ID_MASK = 0x1FFFFFFF


class MF4Writer(BinaryIOMessageWriter):
    """Logs CAN data to an ASAM Measurement Data File v4 (.mf4).

    MF4Writer does not support append mode.

    If a message has a timestamp smaller than the previous one or None,
    it gets assigned the timestamp that was written for the last message.
    It the first message does not have a timestamp, it is set to zero.
    """

    def __init__(
        self,
        file: Union[StringPathLike, BinaryIO],
        database: Optional[StringPathLike] = None,
        compression_level: int = 2,
        **kwargs: Any,
    ) -> None:
        """
        :param file:
            A path-like object or as file-like object to write to.
            If this is a file-like object, is has to be opened in
            binary write mode, not text write mode.
        :param database:
            optional path to a DBC or ARXML file that contains message description.
        :param compression_level:
            compression option as integer (default 2)
            * 0 - no compression
            * 1 - deflate (slower, but produces smaller files)
            * 2 - transposition + deflate (slowest, but produces the smallest files)
        """
        if asammdf is None:
            raise NotImplementedError(
                "The asammdf package was not found. Install python-can with "
                "the optional dependency [mf4] to use the MF4Writer."
            )

        if kwargs.get("append", False):
            raise ValueError(
                f"{self.__class__.__name__} is currently not equipped to "
                f"append messages to an existing file."
            )

        super().__init__(file, mode="w+b")
        now = datetime.now()
        self._mdf = cast("MDF4", MDF(version="4.10"))
        self._mdf.header.start_time = now
        self.last_timestamp = self._start_time = now.timestamp()

        self._compression_level = compression_level

        if database:
            database = Path(database).resolve()
            if database.exists():
                data = database.read_bytes()
                attachment = data, database.name, md5(data).digest()
            else:
                attachment = None
        else:
            attachment = None

        acquisition_source = SourceInformation(
            source_type=SOURCE_BUS, bus_type=BUS_TYPE_CAN
        )

        # standard frames group
        self._mdf.append(
            Signal(
                name="CAN_DataFrame",
                samples=np.array([], dtype=STD_DTYPE),
                timestamps=np.array([], dtype="<f8"),
                attachment=attachment,
                source=acquisition_source,
            )
        )

        # error frames group
        self._mdf.append(
            Signal(
                name="CAN_ErrorFrame",
                samples=np.array([], dtype=ERR_DTYPE),
                timestamps=np.array([], dtype="<f8"),
                attachment=attachment,
                source=acquisition_source,
            )
        )

        # remote frames group
        self._mdf.append(
            Signal(
                name="CAN_RemoteFrame",
                samples=np.array([], dtype=RTR_DTYPE),
                timestamps=np.array([], dtype="<f8"),
                attachment=attachment,
                source=acquisition_source,
            )
        )

        self._std_buffer = np.zeros(1, dtype=STD_DTYPE)
        self._err_buffer = np.zeros(1, dtype=ERR_DTYPE)
        self._rtr_buffer = np.zeros(1, dtype=RTR_DTYPE)

    def file_size(self) -> int:
        """Return an estimate of the current file size in bytes."""
        # TODO: find solution without accessing private attributes of asammdf
        return cast(
            "int",
            self._mdf._tempfile.tell(),  # pylint: disable=protected-access,no-member
        )

    def stop(self) -> None:
        self._mdf.save(self.file, compression=self._compression_level)
        self._mdf.close()
        super().stop()

    def on_message_received(self, msg: Message) -> None:
        channel = channel2int(msg.channel)

        timestamp = msg.timestamp
        if timestamp is None:
            timestamp = self.last_timestamp
        else:
            self.last_timestamp = max(self.last_timestamp, timestamp)

        timestamp -= self._start_time

        if msg.is_remote_frame:
            if channel is not None:
                self._rtr_buffer["CAN_RemoteFrame.BusChannel"] = channel

            self._rtr_buffer["CAN_RemoteFrame.ID"] = msg.arbitration_id
            self._rtr_buffer["CAN_RemoteFrame.IDE"] = int(msg.is_extended_id)
            self._rtr_buffer["CAN_RemoteFrame.Dir"] = 0 if msg.is_rx else 1
            self._rtr_buffer["CAN_RemoteFrame.DLC"] = msg.dlc

            sigs = [(np.array([timestamp]), None), (self._rtr_buffer, None)]
            self._mdf.extend(2, sigs)

        elif msg.is_error_frame:
            if channel is not None:
                self._err_buffer["CAN_ErrorFrame.BusChannel"] = channel

            self._err_buffer["CAN_ErrorFrame.ID"] = msg.arbitration_id
            self._err_buffer["CAN_ErrorFrame.IDE"] = int(msg.is_extended_id)
            self._err_buffer["CAN_ErrorFrame.Dir"] = 0 if msg.is_rx else 1
            data = msg.data
            size = len(data)
            self._err_buffer["CAN_ErrorFrame.DataLength"] = size
            self._err_buffer["CAN_ErrorFrame.DataBytes"][0, :size] = data
            if msg.is_fd:
                self._err_buffer["CAN_ErrorFrame.DLC"] = len2dlc(msg.dlc)
                self._err_buffer["CAN_ErrorFrame.ESI"] = int(msg.error_state_indicator)
                self._err_buffer["CAN_ErrorFrame.BRS"] = int(msg.bitrate_switch)
                self._err_buffer["CAN_ErrorFrame.EDL"] = 1
            else:
                self._err_buffer["CAN_ErrorFrame.DLC"] = msg.dlc
                self._err_buffer["CAN_ErrorFrame.ESI"] = 0
                self._err_buffer["CAN_ErrorFrame.BRS"] = 0
                self._err_buffer["CAN_ErrorFrame.EDL"] = 0

            sigs = [(np.array([timestamp]), None), (self._err_buffer, None)]
            self._mdf.extend(1, sigs)

        else:
            if channel is not None:
                self._std_buffer["CAN_DataFrame.BusChannel"] = channel

            self._std_buffer["CAN_DataFrame.ID"] = msg.arbitration_id
            self._std_buffer["CAN_DataFrame.IDE"] = int(msg.is_extended_id)
            self._std_buffer["CAN_DataFrame.Dir"] = 0 if msg.is_rx else 1
            data = msg.data
            size = len(data)
            self._std_buffer["CAN_DataFrame.DataLength"] = size
            self._std_buffer["CAN_DataFrame.DataBytes"][0, :size] = data
            if msg.is_fd:
                self._std_buffer["CAN_DataFrame.DLC"] = len2dlc(msg.dlc)
                self._std_buffer["CAN_DataFrame.ESI"] = int(msg.error_state_indicator)
                self._std_buffer["CAN_DataFrame.BRS"] = int(msg.bitrate_switch)
                self._std_buffer["CAN_DataFrame.EDL"] = 1
            else:
                self._std_buffer["CAN_DataFrame.DLC"] = msg.dlc
                self._std_buffer["CAN_DataFrame.ESI"] = 0
                self._std_buffer["CAN_DataFrame.BRS"] = 0
                self._std_buffer["CAN_DataFrame.EDL"] = 0

            sigs = [(np.array([timestamp]), None), (self._std_buffer, None)]
            self._mdf.extend(0, sigs)

        # reset buffer structure
        self._std_buffer = np.zeros(1, dtype=STD_DTYPE)
        self._err_buffer = np.zeros(1, dtype=ERR_DTYPE)
        self._rtr_buffer = np.zeros(1, dtype=RTR_DTYPE)


class FrameIterator(abc.ABC):
    """
    Iterator helper class for common handling among CAN DataFrames, ErrorFrames and RemoteFrames.
    """

    # Number of records to request for each asammdf call
    _chunk_size = 1000

    def __init__(self, mdf: MDF4, group_index: int, start_timestamp: float, name: str):
        self._mdf = mdf
        self._group_index = group_index
        self._start_timestamp = start_timestamp
        self._name = name

        # Extract names
        channel_group: ChannelGroup = self._mdf.groups[self._group_index]

        self._channel_names = []

        for channel in channel_group.channels:
            if str(channel.name).startswith(f"{self._name}."):
                self._channel_names.append(channel.name)

    def _get_data(self, current_offset: int) -> Signal:
        # NOTE: asammdf suggests using select instead of get. Select seem to miss converting some
        #       channels which get does convert as expected.
        data_raw = self._mdf.get(
            self._name,
            self._group_index,
            record_offset=current_offset,
            record_count=self._chunk_size,
            raw=False,
        )

        return data_raw

    @abc.abstractmethod
    def __iter__(self) -> Generator[Message, None, None]:
        pass


class MF4Reader(BinaryIOMessageReader):
    """
    Iterator of CAN messages from a MF4 logging file.

    The MF4Reader only supports MF4 files with CAN bus logging.
    """

    # NOTE: Readout based on the bus logging code from asammdf GUI

    class _CANDataFrameIterator(FrameIterator):

        def __init__(self, mdf: MDF4, group_index: int, start_timestamp: float):
            super().__init__(mdf, group_index, start_timestamp, "CAN_DataFrame")

        def __iter__(self) -> Generator[Message, None, None]:
            for current_offset in range(
                0,
                self._mdf.groups[self._group_index].channel_group.cycles_nr,
                self._chunk_size,
            ):
                data = self._get_data(current_offset)
                names = data.samples[0].dtype.names

                for i in range(len(data)):
                    data_length = int(data["CAN_DataFrame.DataLength"][i])

                    kv: dict[str, Any] = {
                        "timestamp": float(data.timestamps[i]) + self._start_timestamp,
                        "arbitration_id": int(data["CAN_DataFrame.ID"][i]) & 0x1FFFFFFF,
                        "data": data["CAN_DataFrame.DataBytes"][i][
                            :data_length
                        ].tobytes(),
                    }

                    if "CAN_DataFrame.BusChannel" in names:
                        kv["channel"] = int(data["CAN_DataFrame.BusChannel"][i])
                    if "CAN_DataFrame.Dir" in names:
                        if data["CAN_DataFrame.Dir"][i].dtype.kind == "S":
                            kv["is_rx"] = data["CAN_DataFrame.Dir"][i] == b"Rx"
                        else:
                            kv["is_rx"] = int(data["CAN_DataFrame.Dir"][i]) == 0
                    if "CAN_DataFrame.IDE" in names:
                        kv["is_extended_id"] = bool(data["CAN_DataFrame.IDE"][i])
                    if "CAN_DataFrame.EDL" in names:
                        kv["is_fd"] = bool(data["CAN_DataFrame.EDL"][i])
                    if "CAN_DataFrame.BRS" in names:
                        kv["bitrate_switch"] = bool(data["CAN_DataFrame.BRS"][i])
                    if "CAN_DataFrame.ESI" in names:
                        kv["error_state_indicator"] = bool(data["CAN_DataFrame.ESI"][i])

                    yield Message(**kv)

    class _CANErrorFrameIterator(FrameIterator):

        def __init__(self, mdf: MDF4, group_index: int, start_timestamp: float):
            super().__init__(mdf, group_index, start_timestamp, "CAN_ErrorFrame")

        def __iter__(self) -> Generator[Message, None, None]:
            for current_offset in range(
                0,
                self._mdf.groups[self._group_index].channel_group.cycles_nr,
                self._chunk_size,
            ):
                data = self._get_data(current_offset)
                names = data.samples[0].dtype.names

                for i in range(len(data)):
                    kv: dict[str, Any] = {
                        "timestamp": float(data.timestamps[i]) + self._start_timestamp,
                        "is_error_frame": True,
                    }

                    if "CAN_ErrorFrame.BusChannel" in names:
                        kv["channel"] = int(data["CAN_ErrorFrame.BusChannel"][i])
                    if "CAN_ErrorFrame.Dir" in names:
                        if data["CAN_ErrorFrame.Dir"][i].dtype.kind == "S":
                            kv["is_rx"] = data["CAN_ErrorFrame.Dir"][i] == b"Rx"
                        else:
                            kv["is_rx"] = int(data["CAN_ErrorFrame.Dir"][i]) == 0
                    if "CAN_ErrorFrame.ID" in names:
                        kv["arbitration_id"] = (
                            int(data["CAN_ErrorFrame.ID"][i]) & 0x1FFFFFFF
                        )
                    if "CAN_ErrorFrame.IDE" in names:
                        kv["is_extended_id"] = bool(data["CAN_ErrorFrame.IDE"][i])
                    if "CAN_ErrorFrame.EDL" in names:
                        kv["is_fd"] = bool(data["CAN_ErrorFrame.EDL"][i])
                    if "CAN_ErrorFrame.BRS" in names:
                        kv["bitrate_switch"] = bool(data["CAN_ErrorFrame.BRS"][i])
                    if "CAN_ErrorFrame.ESI" in names:
                        kv["error_state_indicator"] = bool(
                            data["CAN_ErrorFrame.ESI"][i]
                        )
                    if "CAN_ErrorFrame.RTR" in names:
                        kv["is_remote_frame"] = bool(data["CAN_ErrorFrame.RTR"][i])
                    if (
                        "CAN_ErrorFrame.DataLength" in names
                        and "CAN_ErrorFrame.DataBytes" in names
                    ):
                        data_length = int(data["CAN_ErrorFrame.DataLength"][i])
                        kv["data"] = data["CAN_ErrorFrame.DataBytes"][i][
                            :data_length
                        ].tobytes()

                    yield Message(**kv)

    class _CANRemoteFrameIterator(FrameIterator):

        def __init__(self, mdf: MDF4, group_index: int, start_timestamp: float):
            super().__init__(mdf, group_index, start_timestamp, "CAN_RemoteFrame")

        def __iter__(self) -> Generator[Message, None, None]:
            for current_offset in range(
                0,
                self._mdf.groups[self._group_index].channel_group.cycles_nr,
                self._chunk_size,
            ):
                data = self._get_data(current_offset)
                names = data.samples[0].dtype.names

                for i in range(len(data)):
                    kv: dict[str, Any] = {
                        "timestamp": float(data.timestamps[i]) + self._start_timestamp,
                        "arbitration_id": int(data["CAN_RemoteFrame.ID"][i])
                        & 0x1FFFFFFF,
                        "dlc": int(data["CAN_RemoteFrame.DLC"][i]),
                        "is_remote_frame": True,
                    }

                    if "CAN_RemoteFrame.BusChannel" in names:
                        kv["channel"] = int(data["CAN_RemoteFrame.BusChannel"][i])
                    if "CAN_RemoteFrame.Dir" in names:
                        if data["CAN_RemoteFrame.Dir"][i].dtype.kind == "S":
                            kv["is_rx"] = data["CAN_RemoteFrame.Dir"][i] == b"Rx"
                        else:
                            kv["is_rx"] = int(data["CAN_RemoteFrame.Dir"][i]) == 0
                    if "CAN_RemoteFrame.IDE" in names:
                        kv["is_extended_id"] = bool(data["CAN_RemoteFrame.IDE"][i])

                    yield Message(**kv)

    def __init__(
        self,
        file: Union[StringPathLike, BinaryIO],
        **kwargs: Any,
    ) -> None:
        """
        :param file: a path-like object or as file-like object to read from
                        If this is a file-like object, is has to be opened in
                        binary read mode, not text read mode.
        """
        if asammdf is None:
            raise NotImplementedError(
                "The asammdf package was not found. Install python-can with "
                "the optional dependency [mf4] to use the MF4Reader."
            )

        super().__init__(file, mode="rb")

        self._mdf: MDF4
        if isinstance(file, BufferedIOBase):
            self._mdf = cast("MDF4", MDF(BytesIO(file.read())))
        else:
            self._mdf = cast("MDF4", MDF(file))

        self._start_timestamp = self._mdf.header.start_time.timestamp()

    def __iter__(self) -> Iterator[Message]:
        # To handle messages split over multiple channel groups, create a single iterator per
        # channel group and merge these iterators into a single iterator using heapq.
        iterators: list[FrameIterator] = []
        for group_index, group in enumerate(self._mdf.groups):
            channel_group: ChannelGroup = group.channel_group

            if not channel_group.flags & FLAG_CG_BUS_EVENT:
                # Not a bus event, skip
                continue

            if channel_group.cycles_nr == 0:
                # No data, skip
                continue

            acquisition_source: Optional[Source] = channel_group.acq_source

            if acquisition_source is None:
                # No source information, skip
                continue
            if not acquisition_source.source_type & Source.SOURCE_BUS:
                # Not a bus type (likely already covered by the channel group flag), skip
                continue

            channel_names = [channel.name for channel in group.channels]

            if acquisition_source.bus_type == Source.BUS_TYPE_CAN:
                if "CAN_DataFrame" in channel_names:
                    iterators.append(
                        self._CANDataFrameIterator(
                            self._mdf, group_index, self._start_timestamp
                        )
                    )
                elif "CAN_ErrorFrame" in channel_names:
                    iterators.append(
                        self._CANErrorFrameIterator(
                            self._mdf, group_index, self._start_timestamp
                        )
                    )
                elif "CAN_RemoteFrame" in channel_names:
                    iterators.append(
                        self._CANRemoteFrameIterator(
                            self._mdf, group_index, self._start_timestamp
                        )
                    )
            else:
                # Unknown bus type, skip
                continue

        # Create merged iterator over all the groups, using the timestamps as comparison key
        return iter(heapq.merge(*iterators, key=lambda x: x.timestamp))

    def stop(self) -> None:
        self._mdf.close()
        self._mdf = None
        super().stop()
