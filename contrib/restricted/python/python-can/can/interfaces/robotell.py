"""
Interface for Chinese Robotell compatible interfaces (win32/linux).
"""

import io
import logging
import time
from typing import Optional

from can import BusABC, CanProtocol, Message

from ..exceptions import CanInterfaceNotImplementedError, CanOperationError

logger = logging.getLogger(__name__)

try:
    import serial
except ImportError:
    logger.warning(
        "You won't be able to use the Robotell can backend without "
        "the serial module installed!"
    )
    serial = None


class robotellBus(BusABC):
    """
    robotell interface
    """

    _PACKET_HEAD = 0xAA  # Frame starts with 2x FRAME_HEAD bytes
    _PACKET_TAIL = 0x55  # Frame ends with 2x FRAME_END bytes
    _PACKET_ESC = (
        0xA5  # Escape char before any HEAD, TAIL or ESC chat (including in checksum)
    )

    _CAN_CONFIG_CHANNEL = 0xFF  # Configuration channel of CAN
    _CAN_SERIALBPS_ID = 0x01FFFE90  # USB Serial port speed
    _CAN_ART_ID = 0x01FFFEA0  # Automatic retransmission
    _CAN_ABOM_ID = 0x01FFFEB0  # Automatic bus management
    _CAN_RESET_ID = 0x01FFFEC0  # ID for initialization
    _CAN_BAUD_ID = 0x01FFFED0  # CAN baud rate
    _CAN_FILTER_BASE_ID = 0x01FFFEE0  # ID for first filter (filter0)
    _CAN_FILTER_MAX_ID = 0x01FFFEE0 + 13  # ID for the last filter (filter13)
    _CAN_INIT_FLASH_ID = 0x01FFFEFF  # Restore factory settings
    _CAN_READ_SERIAL1 = 0x01FFFFF0  # Read first part of device serial number
    _CAN_READ_SERIAL2 = 0x01FFFFF1  # Read first part of device serial number
    _MAX_CAN_BAUD = 1000000  # Maximum supported CAN baud rate
    _FILTER_ID_MASK = 0x0000000F  # Filter ID mask
    _CAN_FILTER_EXTENDED = 0x40000000  # Enable mask
    _CAN_FILTER_ENABLE = 0x80000000  # Enable filter

    _CAN_STANDARD_FMT = 0  # Standard message ID
    _CAN_EXTENDED_FMT = 1  # 29 Bit extended format ID
    _CAN_DATA_FRAME = 0  # Send data frame
    _CAN_REMOTE_FRAME = 1  # Request remote frame

    def __init__(
        self, channel, ttyBaudrate=115200, bitrate=None, rtscts=False, **kwargs
    ):
        """
        :param str channel:
            port of underlying serial or usb device (e.g. ``/dev/ttyUSB0``, ``COM8``, ...)
            Must not be empty. Can also end with ``@115200`` (or similarly) to specify the baudrate.
        :param int ttyBaudrate:
            baudrate of underlying serial or usb device (Ignored if set via the ``channel`` parameter)
        :param int bitrate:
            CAN Bitrate in bit/s. Value is stored in the adapter and will be used as default if no bitrate is specified
        :param bool rtscts:
            turn hardware handshake (RTS/CTS) on and off
        """
        if serial is None:
            raise CanInterfaceNotImplementedError("The serial module is not installed")

        if not channel:  # if None or empty
            raise TypeError("Must specify a serial port.")
        if "@" in channel:
            (channel, ttyBaudrate) = channel.split("@")
        self.serialPortOrig = serial.serial_for_url(
            channel, baudrate=ttyBaudrate, rtscts=rtscts
        )

        # Disable flushing queued config ACKs on lookup channel (for unit tests)
        self._loopback_test = channel == "loop://"

        self._rxbuffer = bytearray()  # raw bytes from the serial port
        self._rxmsg = []  # extracted CAN messages waiting to be read
        self._configmsg = []  # extracted config channel messages

        self._writeconfig(self._CAN_RESET_ID, 0)  # Not sure if this is really necessary

        if bitrate is not None:
            self.set_bitrate(bitrate)

        self._can_protocol = CanProtocol.CAN_20
        self.channel_info = (
            f"Robotell USB-CAN s/n {self.get_serial_number(1)} on {channel}"
        )
        logger.info("Using device: %s", self.channel_info)

        super().__init__(channel=channel, **kwargs)

    def set_bitrate(self, bitrate):
        """
        :raise ValueError: if *bitrate* is greater than 1000000
        :param int bitrate:
            Bitrate in bit/s
        """
        if bitrate <= self._MAX_CAN_BAUD:
            self._writeconfig(self._CAN_BAUD_ID, bitrate)
        else:
            raise ValueError(f"Invalid bitrate, must be less than {self._MAX_CAN_BAUD}")

    def set_auto_retransmit(self, retrans_flag):
        """
        :param bool retrans_flag:
            Enable/disable automatic retransmission of unacknowledged CAN frames
        """
        self._writeconfig(self._CAN_ART_ID, 1 if retrans_flag else 0)

    def set_auto_bus_management(self, auto_man):
        """
        :param bool auto_man:
            Enable/disable automatic bus management
        """
        # Not sure what "automatic bus management" does. Does not seem to control
        # automatic ACK of CAN frames (listen only mode)
        self._writeconfig(self._CAN_ABOM_ID, 1 if auto_man else 0)

    def set_serial_rate(self, serial_bps):
        """
        :param int serial_bps:
            Set the baud rate of the serial port (not CAN) interface
        """
        self._writeconfig(self._CAN_SERIALBPS_ID, serial_bps)

    def set_hw_filter(self, filterid, enabled, msgid_value, msgid_mask, extended_msg):
        """
        :raise ValueError: if *filterid* is not between 1 and 14
        :param int filterid:
            ID of filter (1-14)
        :param bool enabled:
            This filter is enabled
        :param int msgid_value:
            CAN message ID to filter on. The test unit does not accept an extented message ID unless bit 31 of the ID was set.
        :param int msgid_mask:
            Mask to apply to CAN messagge ID
        :param bool extended_msg:
            Filter operates on extended format messages
        """
        if filterid < 1 or filterid > 14:
            raise ValueError("Invalid filter ID. ID must be between 0 and 13")
        else:
            configid = self._CAN_FILTER_BASE_ID + (filterid - 1)
            msgid_value += self._CAN_FILTER_ENABLE if enabled else 0
            msgid_value += self._CAN_FILTER_EXTENDED if extended_msg else 0
            self._writeconfig(configid, msgid_value, msgid_mask)

    def _getconfigsize(self, configid):
        if configid == self._CAN_ART_ID or configid == self._CAN_ABOM_ID:
            return 1
        if configid == self._CAN_BAUD_ID or configid == self._CAN_INIT_FLASH_ID:
            return 4
        if configid == self._CAN_SERIALBPS_ID:
            return 4
        if configid == self._CAN_READ_SERIAL1 or configid <= self._CAN_READ_SERIAL2:
            return 8
        if self._CAN_FILTER_BASE_ID <= configid <= self._CAN_FILTER_MAX_ID:
            return 8
        return 0

    def _readconfig(self, configid, timeout):
        self._writemessage(
            msgid=configid,
            msgdata=bytearray(8),
            datalen=self._getconfigsize(configid),
            msgchan=self._CAN_CONFIG_CHANNEL,
            msgformat=self._CAN_EXTENDED_FMT,
            msgtype=self._CAN_REMOTE_FRAME,
        )
        # Read message from config channel with result. Flush any previously pending config messages
        newmsg = self._readmessage(not self._loopback_test, True, timeout)
        if newmsg is None:
            logger.warning(
                f"Timeout waiting for response when reading config value {configid:04X}."
            )
            return None
        return newmsg[4:12]

    def _writeconfig(self, configid, value, value2=0):
        configsize = self._getconfigsize(configid)
        configdata = bytearray(configsize)
        if configsize >= 1:
            configdata[0] = value & 0xFF
        if configsize >= 4:
            configdata[1] = (value >> 8) & 0xFF
            configdata[2] = (value >> 16) & 0xFF
            configdata[3] = (value >> 24) & 0xFF
        if configsize >= 8:
            configdata[4] = value2 & 0xFF
            configdata[5] = (value2 >> 8) & 0xFF
            configdata[6] = (value2 >> 16) & 0xFF
            configdata[7] = (value2 >> 24) & 0xFF
        self._writemessage(
            msgid=configid,
            msgdata=configdata,
            datalen=configsize,
            msgchan=self._CAN_CONFIG_CHANNEL,
            msgformat=self._CAN_EXTENDED_FMT,
            msgtype=self._CAN_DATA_FRAME,
        )
        # Read message from config channel to verify. Flush any previously pending config messages
        newmsg = self._readmessage(not self._loopback_test, True, 1)
        if newmsg is None:
            logger.warning(
                "Timeout waiting for response when writing config value %d", configid
            )

    def _readmessage(self, flushold, cfgchannel, timeout):
        header = bytearray([self._PACKET_HEAD, self._PACKET_HEAD])
        terminator = bytearray([self._PACKET_TAIL, self._PACKET_TAIL])

        msgqueue = self._configmsg if cfgchannel else self._rxmsg
        if flushold:
            del msgqueue[:]

        # read what is already in serial port receive buffer - unless we are doing loopback testing
        if not self._loopback_test:
            while self.serialPortOrig.in_waiting:
                self._rxbuffer += self.serialPortOrig.read()

        # loop until we have read an appropriate message
        start = time.time()
        time_left = timeout
        while True:
            # make sure first bytes in RX buffer is a new packet header
            headpos = self._rxbuffer.find(header)
            if headpos > 0:
                # data does not start with expected header bytes. Log error and ignore garbage
                logger.warning("Ignoring extra " + str(headpos) + " garbage bytes")
                del self._rxbuffer[:headpos]
                headpos = self._rxbuffer.find(header)  # should now be at index 0!

            # check to see if we have a complete packet in the RX buffer
            termpos = self._rxbuffer.find(terminator)
            if headpos == 0 and termpos > headpos:
                # copy packet into message structure and un-escape bytes
                newmsg = bytearray()
                idx = headpos + len(header)
                while idx < termpos:
                    if self._rxbuffer[idx] == self._PACKET_ESC:
                        idx += 1
                    newmsg.append(self._rxbuffer[idx])
                    idx += 1
                del self._rxbuffer[: termpos + len(terminator)]

                # Check one - make sure message structure is the correct length
                if len(newmsg) == 17:
                    # Check two - verify the checksum
                    cs = 0
                    for idx in range(16):
                        cs = (cs + newmsg[idx]) & 0xFF
                    if newmsg[16] == cs:
                        # OK, valid message - place it in the correct queue
                        if newmsg[13] == 0xFF:  # Check for config channel
                            self._configmsg.append(newmsg)
                        else:
                            self._rxmsg.append(newmsg)
                    else:
                        logger.warning("Incorrect message checksum, discarded message")
                else:
                    logger.warning(
                        "Invalid message structure length %d, ignoring message",
                        len(newmsg),
                    )

            # Check if we have a message in the desired queue - if so copy and return
            if len(msgqueue) > 0:
                newmsg = msgqueue[0]
                del msgqueue[:1]
                return newmsg

            # if we still don't have a complete message, do a blocking read
            self.serialPortOrig.timeout = time_left
            byte = self.serialPortOrig.read()
            if byte:
                self._rxbuffer += byte
            # If there is time left, try next one with reduced timeout
            if timeout is not None:
                time_left = timeout - (time.time() - start)
                if time_left <= 0:
                    return None

    def _writemessage(self, msgid, msgdata, datalen, msgchan, msgformat, msgtype):
        msgbuf = bytearray(17)  # Message structure plus checksum byte

        msgbuf[0] = msgid & 0xFF
        msgbuf[1] = (msgid >> 8) & 0xFF
        msgbuf[2] = (msgid >> 16) & 0xFF
        msgbuf[3] = (msgid >> 24) & 0xFF

        if msgtype == self._CAN_DATA_FRAME:
            for idx in range(datalen):
                msgbuf[idx + 4] = msgdata[idx]

        msgbuf[12] = datalen
        msgbuf[13] = msgchan
        msgbuf[14] = msgformat
        msgbuf[15] = msgtype

        cs = 0
        for idx in range(16):
            cs = (cs + msgbuf[idx]) & 0xFF
        msgbuf[16] = cs

        packet = bytearray()
        packet.append(self._PACKET_HEAD)
        packet.append(self._PACKET_HEAD)
        for msgbyte in msgbuf:
            if (
                msgbyte == self._PACKET_ESC
                or msgbyte == self._PACKET_HEAD
                or msgbyte == self._PACKET_TAIL
            ):
                packet.append(self._PACKET_ESC)
            packet.append(msgbyte)
        packet.append(self._PACKET_TAIL)
        packet.append(self._PACKET_TAIL)

        self.serialPortOrig.write(packet)
        self.serialPortOrig.flush()

    def flush(self):
        del self._rxbuffer[:]
        del self._rxmsg[:]
        del self._configmsg[:]
        while self.serialPortOrig.in_waiting:
            self.serialPortOrig.read()

    def _recv_internal(self, timeout):
        msgbuf = self._readmessage(False, False, timeout)
        if msgbuf is not None:
            msg = Message(
                arbitration_id=msgbuf[0]
                + (msgbuf[1] << 8)
                + (msgbuf[2] << 16)
                + (msgbuf[3] << 24),
                is_extended_id=(msgbuf[14] == self._CAN_EXTENDED_FMT),
                timestamp=time.time(),  # Better than nothing...
                is_remote_frame=(msgbuf[15] == self._CAN_REMOTE_FRAME),
                dlc=msgbuf[12],
                data=msgbuf[4 : 4 + msgbuf[12]],
            )
            return msg, False
        return None, False

    def send(self, msg, timeout=None):
        if timeout != self.serialPortOrig.write_timeout:
            self.serialPortOrig.write_timeout = timeout
        self._writemessage(
            msg.arbitration_id,
            msg.data,
            msg.dlc,
            0,
            self._CAN_EXTENDED_FMT if msg.is_extended_id else self._CAN_STANDARD_FMT,
            self._CAN_REMOTE_FRAME if msg.is_remote_frame else self._CAN_DATA_FRAME,
        )

    def shutdown(self):
        super().shutdown()
        self.serialPortOrig.close()

    def fileno(self):
        try:
            return self.serialPortOrig.fileno()
        except io.UnsupportedOperation:
            raise NotImplementedError(
                "fileno is not implemented using current CAN bus on this platform"
            ) from None
        except Exception as exception:
            raise CanOperationError("Cannot fetch fileno") from exception

    def get_serial_number(self, timeout: Optional[int]) -> Optional[str]:
        """Get serial number of the slcan interface.

        :param timeout:
            seconds to wait for serial number or None to wait indefinitely
        :return:
            None on timeout or a str object.
        """

        sn1 = self._readconfig(self._CAN_READ_SERIAL1, timeout)
        if sn1 is None:
            return None
        sn2 = self._readconfig(self._CAN_READ_SERIAL2, timeout)
        if sn2 is None:
            return None

        serial = ""
        for idx in range(0, 8, 2):
            serial += f"{sn1[idx]:02X}{sn1[idx + 1]:02X}-"
        for idx in range(0, 4, 2):
            serial += f"{sn2[idx]:02X}{sn2[idx + 1]:02X}-"
        return serial[:-1]
