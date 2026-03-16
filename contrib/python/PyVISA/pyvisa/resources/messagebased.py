# -*- coding: utf-8 -*-
"""High level wrapper for MessageBased Instruments.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import contextlib
import struct
import time
import warnings
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    Optional,
    Protocol,
    Sequence,
    Type,
    Union,
)

from .. import attributes, constants, errors, logger, util
from ..attributes import Attribute
from ..highlevel import VisaLibraryBase
from .resource import Resource


class SupportsUpdate(Protocol):
    """Type hint for a progress bar object"""

    def update(self, size: int) -> None: ...


class ControlRenMixin(object):
    """Common control_ren method of some messaged based resources."""

    #: Will be present when used as a mixin with Resource
    visalib: VisaLibraryBase

    #: Will be present when used as a mixin with Resource
    session: Any

    # It should work for GPIB, USB and some TCPIP
    # For TCPIP I found some (all?) NI's VISA library do not handle
    # control_ren, but it works for Agilent's VISA library (at least some of
    # them)
    def control_ren(self, mode: constants.RENLineOperation) -> constants.StatusCode:
        """Controls the state of the GPIB Remote Enable (REN) interface line.

        The remote/local state of the device can also be controlled optionally.

        Corresponds to viGpibControlREN function of the VISA library.

        Parameters
        ----------
        mode : constants.RENLineOperation
            Specifies the state of the REN line and optionally the device
            remote/local state.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        return self.visalib.gpib_control_ren(self.session, mode)


class MessageBasedResource(Resource):
    """Base class for resources that use message based communication."""

    CR: str = "\r"
    LF: str = "\n"

    #: Number of bytes to read at a time. Some resources (serial) may not support
    #: large chunk sizes.
    chunk_size: int = 20 * 1024

    #: Delay in s to sleep between the write and read occuring in a query
    query_delay: float = 0.0

    #: Internal storage for the read_termination character
    _read_termination: Optional[str] = None

    #: Internal storage for the write_termination character
    _write_termination: str = CR + LF

    #: Internal storage for the encoding
    _encoding: str = "ascii"

    @property
    def encoding(self) -> str:
        """Encoding used for read and write operations."""
        return self._encoding

    @encoding.setter
    def encoding(self, encoding: str) -> None:
        # Test that the encoding specified makes sense.
        "test encoding".encode(encoding).decode(encoding)
        self._encoding = encoding

    @property
    def read_termination(self) -> Optional[str]:
        """Read termination character."""
        return self._read_termination

    @read_termination.setter
    def read_termination(self, value: Optional[str]) -> None:
        if value:
            # termination character, the rest is just used for verification
            # after each read operation.
            last_char = value[-1:]
            # Consequently, it's illogical to have the real termination
            # character twice in the sequence (otherwise reading would stop
            # prematurely).

            if last_char in value[:-1]:
                raise ValueError("ambiguous ending in termination characters")

            self.set_visa_attribute(
                constants.ResourceAttribute.termchar, ord(last_char)
            )
            self.set_visa_attribute(
                constants.ResourceAttribute.termchar_enabled, constants.VI_TRUE
            )
        else:
            # The termchar is also used in VI_ATTR_ASRL_END_IN (for serial
            # termination) so return it to its default.
            self.set_visa_attribute(constants.ResourceAttribute.termchar, ord(self.LF))
            self.set_visa_attribute(
                constants.ResourceAttribute.termchar_enabled, constants.VI_FALSE
            )

        self._read_termination = value

    @property
    def write_termination(self) -> str:
        """Write termination character."""
        return self._write_termination

    @write_termination.setter
    def write_termination(self, value: str) -> None:
        self._write_termination = value

    #: Should END be asserted during the transfer of the last byte of the buffer.
    send_end: Attribute[bool] = attributes.AttrVI_ATTR_SEND_END_EN()

    #: IO protocol to use. See the attribute definition for more details.
    io_protocol: Attribute[constants.IOProtocol] = attributes.AttrVI_ATTR_IO_PROT()

    #: Should I/O accesses use DMA (True) or Programmed I/O (False).
    allow_dma: Attribute[bool] = attributes.AttrVI_ATTR_DMA_ALLOW_EN()

    def write_raw(self, message: bytes) -> int:
        """Write a byte message to the device.

        Parameters
        ----------
        message : bytes
            The message to be sent.

        Returns
        -------
        int
            Number of bytes written

        """
        return self.visalib.write(self.session, message)[0]

    def write(
        self,
        message: str,
        termination: Optional[str] = None,
        encoding: Optional[str] = None,
    ) -> int:
        """Write a string message to the device.

        The write_termination is always appended to it.

        Parameters
        ----------
        message : str
            The message to be sent.
        termination : Optional[str], optional
            Alternative character termination to use. If None, the value of
            write_termination is used. Defaults to None.
        encoding : Optional[str], optional
            Alternative encoding to use to turn str into bytes. If None, the
            value of encoding is used. Defaults to None.

        Returns
        -------
        int
            Number of bytes written.

        """
        term = self._write_termination if termination is None else termination
        enco = self._encoding if encoding is None else encoding

        if term:
            if message.endswith(term):
                warnings.warn(
                    "write message already ends with termination characters",
                    stacklevel=2,
                )
            message += term

        count = self.write_raw(message.encode(enco))

        return count

    def write_ascii_values(
        self,
        message: str,
        values: Iterable[Any],
        converter: util.ASCII_CONVERTER = "f",
        separator: Union[str, Callable[[Iterable[str]], str]] = ",",
        termination: Optional[str] = None,
        encoding: Optional[str] = None,
    ):
        """Write a string message to the device followed by values in ascii format.

        The write_termination is always appended to it.

        Parameters
        ----------
        message : str
            Header of the message to be sent.
        values : Sequence[Any]
            Data to be written to the device.
        converter : Union[str, Callable[[Any], str]], optional
            Str formatting codes or function used to convert each value.
            Defaults to "f".
        separator : Union[str, Callable[[Iterable[str]], str]], optional
            Str or callable that join the values in a single str.
            If a str is given, separator.join(values) is used. Defaults to ','
        termination : Optional[str], optional
            Alternative character termination to use. If None, the value of
            write_termination is used. Defaults to None.
        encoding : Optional[str], optional
            Alternative encoding to use to turn str into bytes. If None, the
            value of encoding is used. Defaults to None.

        Returns
        -------
        int
            Number of bytes written.

        """
        term = self._write_termination if termination is None else termination
        enco = self._encoding if encoding is None else encoding

        if term and message.endswith(term):
            warnings.warn(
                "write message already ends with termination characters",
                stacklevel=2,
            )

        block = util.to_ascii_block(values, converter, separator)

        msg = message.encode(enco) + block.encode(enco)

        if term:
            msg += term.encode(enco)

        count = self.write_raw(msg)

        return count

    def write_binary_values(
        self,
        message: str,
        values: Sequence[Any],
        datatype: util.BINARY_DATATYPES = "f",
        is_big_endian: bool = False,
        termination: Optional[str] = None,
        encoding: Optional[str] = None,
        header_fmt: util.BINARY_HEADERS = "ieee",
    ):
        """Write a string message to the device followed by values in binary format.

        The write_termination is always appended to it.

        Parameters
        ----------
        message : str
            The header of the message to be sent.
        values : Sequence[Any]
            Data to be written to the device.
        datatype : util.BINARY_DATATYPES, optional
            The format string for a single element. See struct module.
        is_big_endian : bool, optional
            Are the data in big or little endian order.
        termination : Optional[str], optional
            Alternative character termination to use. If None, the value of
            write_termination is used. Defaults to None.
        encoding : Optional[str], optional
            Alternative encoding to use to turn str into bytes. If None, the
            value of encoding is used. Defaults to None.
        header_fmt : util.BINARY_HEADERS
            Format of the header prefixing the data.

        Returns
        -------
        int
            Number of bytes written.

        """
        term = self._write_termination if termination is None else termination
        enco = self._encoding if encoding is None else encoding

        if term and message.endswith(term):
            warnings.warn(
                "write message already ends with termination characters",
                stacklevel=2,
            )

        if header_fmt == "ieee":
            block = util.to_ieee_block(values, datatype, is_big_endian)
        elif header_fmt == "rs":
            block = util.to_rs_block(values, datatype, is_big_endian)
        elif header_fmt == "hp":
            block = util.to_hp_block(values, datatype, is_big_endian)
        elif header_fmt == "empty":
            block = util.to_binary_block(values, b"", datatype, is_big_endian)
        else:
            raise ValueError("Unsupported header_fmt: %s" % header_fmt)

        msg = message.encode(enco) + block

        if term:
            msg += term.encode(enco)

        count = self.write_raw(msg)

        return count

    def read_bytes(
        self,
        count: int,
        chunk_size: Optional[int] = None,
        break_on_termchar: bool = False,
        monitoring_interface: Optional[SupportsUpdate] = None,
    ) -> bytes:
        """Read a certain number of bytes from the instrument.

        Parameters
        ----------
        count : int
            The number of bytes to read from the instrument.
        chunk_size : Optional[int], optional
            The chunk size to use to perform the reading. If count > chunk_size
            multiple low level operations will be performed. Defaults to None,
            meaning the resource wide set value is set.
        break_on_termchar : bool, optional
            Should the reading stop when a termination character is encountered
            or when the message ends. Defaults to False.
        monitoring_interface : SupportsUpdate Protocol, optional
            Progress monitoring object with update() method that accepts the number
            of bytes read. See the tqdm documentation (a progress bar package) for
            more information.

        Returns
        -------
        bytes
            Bytes read from the instrument.

        """
        chunk_size = chunk_size or self.chunk_size
        ret = bytearray()
        left_to_read = count
        success = constants.StatusCode.success
        termchar_read = constants.StatusCode.success_termination_character_read

        with self.ignore_warning(
            constants.StatusCode.success_device_not_present,
            constants.StatusCode.success_max_count_read,
        ):
            try:
                status = None
                while len(ret) < count:
                    size = min(chunk_size, left_to_read)
                    logger.debug(
                        "%s - reading %d bytes (last status %r)",
                        self._resource_name,
                        size,
                        status,
                    )
                    chunk, status = self.visalib.read(self.session, size)
                    if monitoring_interface:
                        monitoring_interface.update(len(chunk))
                    ret.extend(chunk)
                    left_to_read -= len(chunk)
                    if break_on_termchar and (
                        status == success or status == termchar_read
                    ):
                        break
            except errors.VisaIOError as e:
                logger.debug(
                    "%s - exception while reading: %s\nBuffer content: %r",
                    self._resource_name,
                    e,
                    ret,
                )
                raise
        return bytes(ret)

    def read_raw(self, size: Optional[int] = None) -> bytes:
        """Read the unmodified string sent from the instrument to the computer.

        In contrast to read(), no termination characters are stripped.

        Parameters
        ----------
        size : Optional[int], optional
            The chunk size to use to perform the reading. Defaults to None,
            meaning the resource wide set value is set.

        Returns
        -------
        bytes
            Bytes read from the instrument.

        """
        return bytes(self._read_raw(size))

    def _read_raw(
        self,
        size: Optional[int] = None,
        monitoring_interface: Optional[SupportsUpdate] = None,
    ):
        """Read the unmodified string sent from the instrument to the computer.

        In contrast to read(), no termination characters are stripped.

        Parameters
        ----------
        size : Optional[int], optional
            The chunk size to use to perform the reading. Defaults to None,
            meaning the resource wide set value is set.
        monitoring_interface : SupportsUpdate Protocol, optional
            Progress monitoring object with update() method that accepts the number
            of bytes read. See the tqdm documentation (a progress bar package) for
            more information.

        Returns
        -------
        bytearray
            Bytes read from the instrument.

        """
        size = self.chunk_size if size is None else size

        loop_status = constants.StatusCode.success_max_count_read

        ret = bytearray()
        with self.ignore_warning(
            constants.StatusCode.success_device_not_present,
            constants.StatusCode.success_max_count_read,
        ):
            try:
                status = loop_status
                while status == loop_status:
                    logger.debug(
                        "%s - reading %d bytes (last status %r)",
                        self._resource_name,
                        size,
                        status,
                    )
                    chunk, status = self.visalib.read(self.session, size)
                    if monitoring_interface:
                        monitoring_interface.update(len(chunk))
                    ret.extend(chunk)
            except errors.VisaIOError as e:
                logger.debug(
                    "%s - exception while reading: %s\nBuffer content: %r",
                    self._resource_name,
                    e,
                    ret,
                )
                raise

        return ret

    def read(
        self, termination: Optional[str] = None, encoding: Optional[str] = None
    ) -> str:
        """Read a string from the device.

        Reading stops when the device stops sending (e.g. by setting
        appropriate bus lines), or the termination characters sequence was
        detected.  Attention: Only the last character of the termination
        characters is really used to stop reading, however, the whole sequence
        is compared to the ending of the read string message.  If they don't
        match, a warning is issued.

        Parameters
        ----------
        termination : Optional[str], optional
            Alternative character termination to use. If None, the value of
            read_termination is used. Defaults to None.
        encoding : Optional[str], optional
            Alternative encoding to use to turn bytes into str. If None, the
            value of encoding is used. Defaults to None.

        Returns
        -------
        str
            Message read from the instrument and decoded.

        """
        enco = self._encoding if encoding is None else encoding

        if termination is None:
            termination = self._read_termination
            message = self._read_raw().decode(enco)
        else:
            with self.read_termination_context(termination):
                message = self._read_raw().decode(enco)

        if not termination:
            return message

        if not message.endswith(termination):
            warnings.warn(
                "read string doesn't end with termination characters", stacklevel=2
            )
            return message

        return message[: -len(termination)]

    def read_ascii_values(
        self,
        converter: util.ASCII_CONVERTER = "f",
        separator: Union[str, Callable[[str], Iterable[str]]] = ",",
        container: Union[Type, Callable[[Iterable], Sequence]] = list,
    ) -> Sequence:
        """Read values from the device in ascii format returning an iterable of
        values.

        Parameters
        ----------
         converter : ASCII_CONVERTER, optional
            Str format of function to convert each value. Default to "f".
        separator : Union[str, Callable[[str], Iterable[str]]]
            str or callable used to split the data into individual elements.
            If a str is given, data.split(separator) is used. Default to ",".
        container : Union[Type, Callable[[Iterable], Sequence]], optional
            Container type to use for the output data. Possible values are: list,
            tuple, np.ndarray, etc, Default to list.

        Returns
        -------
        Sequence
            Parsed data.

        """
        # Use read rather than _read_raw because we cannot handle a bytearray
        block = self.read()

        return util.from_ascii_block(block, converter, separator, container)

    def read_binary_values(
        self,
        datatype: util.BINARY_DATATYPES = "f",
        is_big_endian: bool = False,
        container: Union[Type, Callable[[Iterable], Sequence]] = list,
        header_fmt: util.BINARY_HEADERS = "ieee",
        expect_termination: bool = True,
        data_points: int = -1,
        chunk_size: Optional[int] = None,
        monitoring_interface: Optional[SupportsUpdate] = None,
        length_before_block: Optional[int] = None,
        raise_on_late_block: bool = False,
    ) -> Sequence[Union[int, float]]:
        """Read values from the device in binary format returning an iterable
        of values.

        Parameters
        ----------
        datatype : BINARY_DATATYPES, optional
            Format string for a single element. See struct module. 'f' by default.
        is_big_endian : bool, optional
            Are the data in big or little endian order. Defaults to False.
        container : Union[Type, Callable[[Iterable], Sequence]], optional
            Container type to use for the output data. Possible values are: list,
            tuple, np.ndarray, etc, Default to list.
        header_fmt : util.BINARY_HEADERS, optional
            Format of the header prefixing the data. Defaults to 'ieee'.
        expect_termination : bool, optional
            When set to False, the expected length of the binary values block
            does not account for the final termination character
            (the read termination). Defaults to True.
        data_points : int, optional
             Number of points expected in the block. This is used only if the
             instrument does not report it itself. This will be converted in a
             number of bytes based on the datatype. Defaults to 0.
        chunk_size : int, optional
            Size of the chunks to read from the device. Using larger chunks may
            be faster for large amount of data.
        monitoring_interface : SupportsUpdate Protocol, optional
            Progress monitoring object with update() method that accepts the number
            of bytes read. See the tqdm documentation (a progress bar package) for
            more information.

        Returns
        -------
        Sequence[Union[int, float]]
            Data read from the device.

        """
        block = self._read_raw(chunk_size, monitoring_interface=monitoring_interface)

        if header_fmt == "ieee":
            offset, data_length = util.parse_ieee_block_header(
                block, length_before_block, raise_on_late_block
            )

        elif header_fmt == "rs":
            offset, data_length = util.parse_ieee_or_rs_block_header(
                block, length_before_block, raise_on_late_block
            )
        elif header_fmt == "hp":
            offset, data_length = util.parse_hp_block_header(
                block, is_big_endian, length_before_block, raise_on_late_block
            )
        elif header_fmt == "empty":
            offset = 0
            data_length = -1
        else:
            raise ValueError(
                "Invalid header format. Valid options are 'ieee', 'empty', 'hp'"
            )

        # Allow to support instrument such as the Keithley 2000 that do not
        # report the length of the block
        data_length = (
            data_length if data_length >= 0 else data_points * struct.calcsize(datatype)
        )

        expected_length = offset + data_length

        if expect_termination and self._read_termination is not None:
            expected_length += len(self._read_termination)

        # Read all the data if we know what to expect.
        if data_length > 0:
            block.extend(
                self.read_bytes(
                    expected_length - len(block),
                    chunk_size=chunk_size,
                    monitoring_interface=monitoring_interface,
                )
            )
        elif data_length == 0:
            pass
        else:
            raise ValueError(
                "The length of the data to receive could not be "
                "determined. You should provide the number of "
                "points you expect using the data_points keyword "
                "argument."
            )

        try:
            # Do not reparse the headers since it was already done and since
            # this allows for custom data length
            return util.from_binary_block(
                block, offset, data_length, datatype, is_big_endian, container
            )
        except ValueError as e:
            raise errors.InvalidBinaryFormat(e.args[0])

    def query(self, message: str, delay: Optional[float] = None) -> str:
        """A combination of write(message) and read()

        Parameters
        ----------
        message : str
            The message to send.
        delay : Optional[float], optional
            Delay in seconds between write and read operations. If None,
            defaults to self.query_delay.

        Returns
        -------
        str
            Answer from the device.

        """
        self.write(message)

        delay = self.query_delay if delay is None else delay
        if delay > 0.0:
            time.sleep(delay)

        return self.read()

    def query_ascii_values(
        self,
        message: str,
        converter: util.ASCII_CONVERTER = "f",
        separator: Union[str, Callable[[str], Iterable[str]]] = ",",
        container: Union[Type, Callable[[Iterable], Sequence]] = list,
        delay: Optional[float] = None,
    ) -> Sequence[Any]:
        """Query the device for values in ascii format returning an iterable of
        values.

        Parameters
        ----------
        message : str
            The message to send.
        converter : ASCII_CONVERTER, optional
            Str format of function to convert each value. Default to "f".
        separator : Union[str, Callable[[str], Iterable[str]]]
            str or callable used to split the data into individual elements.
            If a str is given, data.split(separator) is used. Default to ",".
        container : Union[Type, Callable[[Iterable], Sequence]], optional
            Container type to use for the output data. Possible values are: list,
            tuple, np.ndarray, etc, Default to list.
        delay : Optional[float], optional
            Delay in seconds between write and read operations. If None,
            defaults to self.query_delay.

        Returns
        -------
        Sequence
            Parsed data.
        """

        self.write(message)
        if delay is None:
            delay = self.query_delay
        if delay > 0.0:
            time.sleep(delay)

        return self.read_ascii_values(converter, separator, container)

    def query_binary_values(
        self,
        message: str,
        datatype: util.BINARY_DATATYPES = "f",
        is_big_endian: bool = False,
        container: Union[Type, Callable[[Iterable], Sequence]] = list,
        delay: Optional[float] = None,
        header_fmt: util.BINARY_HEADERS = "ieee",
        expect_termination: bool = True,
        data_points: int = 0,
        chunk_size: Optional[int] = None,
        monitoring_interface: Optional[SupportsUpdate] = None,
        length_before_block: Optional[int] = None,
        raise_on_late_block: bool = False,
    ) -> Sequence[Union[int, float]]:
        """Query the device for values in binary format returning an iterable
        of values.

        Parameters
        ----------
        message : str
            The message to send.
        datatype : BINARY_DATATYPES, optional
            Format string for a single element. See struct module. 'f' by default.
        is_big_endian : bool, optional
            Are the data in big or little endian order. Defaults to False.
        container : Union[Type, Callable[[Iterable], Sequence]], optional
            Container type to use for the output data. Possible values are: list,
            tuple, np.ndarray, etc, Default to list.
        delay : Optional[float], optional
            Delay in seconds between write and read operations. If None,
            defaults to self.query_delay.
        header_fmt : util.BINARY_HEADERS, optional
            Format of the header prefixing the data. Defaults to 'ieee'.
        expect_termination : bool, optional
            When set to False, the expected length of the binary values block
            does not account for the final termination character
            (the read termination). Defaults to True.
        data_points : int, optional
             Number of points expected in the block. This is used only if the
             instrument does not report it itself. This will be converted in a
             number of bytes based on the datatype. Defaults to 0.
        chunk_size : int, optional
            Size of the chunks to read from the device. Using larger chunks may
            be faster for large amount of data.
        monitoring_interface : SupportsUpdate Protocol, optional
            Progress monitoring object with update() method that accepts the number
            of bytes read. See the tqdm documentation (a progress bar package) for
            more information.

        Returns
        -------
        Sequence[Union[int, float]]
            Data read from the device.

        """
        if header_fmt not in ("ieee", "hp", "rs", "empty"):
            raise ValueError(
                "Invalid header format. Valid options are 'ieee', 'hp', 'rs', and 'empty'"
            )

        self.write(message)
        if delay is None:
            delay = self.query_delay
        if delay > 0.0:
            time.sleep(delay)

        return self.read_binary_values(
            datatype,
            is_big_endian,
            container,
            header_fmt,
            expect_termination,
            data_points,
            chunk_size,
            monitoring_interface,
            length_before_block,
            raise_on_late_block,
        )

    def assert_trigger(self) -> None:
        """Sends a software trigger to the device."""
        self.visalib.assert_trigger(self.session, constants.TriggerProtocol.default)

    @property
    def stb(self) -> int:
        """Service request status register."""
        return self.read_stb()

    def read_stb(self) -> int:
        """Service request status register."""
        value, _retcode = self.visalib.read_stb(self.session)
        return value

    @contextlib.contextmanager
    def read_termination_context(self, new_termination: Optional[str]) -> Iterator:
        term = self.read_termination
        self.read_termination = new_termination
        yield
        self.read_termination = term

    def flush(self, mask: constants.BufferOperation) -> None:
        """Manually clears the specified buffers.

        Depending on the value of the mask this can cause the buffer data
        to be written to the device.

        Parameters
        ----------
        mask : constants.BufferOperation
            Specifies the action to be taken with flushing the buffer.
            See highlevel.VisaLibraryBase.flush for a detailed description.

        """
        self.visalib.flush(self.session, mask)


# Rohde and Schwarz Device via Passport. Not sure which Resource should be.
MessageBasedResource.register(constants.InterfaceType.rsnrp, "INSTR")(
    MessageBasedResource
)
