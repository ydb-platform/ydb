"""
To Support the Seeed USB-Can analyzer interface. The device will appear
as a serial port, for example "/dev/ttyUSB0" on Linux machines
or "COM1" on Windows.
https://www.seeedstudio.com/USB-CAN-Analyzer-p-2888.html
SKU 114991193
"""

import io
import logging
import struct
from time import time

import can
from can import BusABC, CanProtocol, Message

logger = logging.getLogger("seeedbus")

try:
    import serial
except ImportError:
    logger.warning(
        "You won't be able to use the serial can backend without "
        "the serial module installed!"
    )
    serial = None


class SeeedBus(BusABC):
    """
    Enable basic can communication over a USB-CAN-Analyzer device.
    """

    BITRATE = {
        1000000: 0x01,
        800000: 0x02,
        500000: 0x03,
        400000: 0x04,
        250000: 0x05,
        200000: 0x06,
        125000: 0x07,
        100000: 0x08,
        50000: 0x09,
        20000: 0x0A,
        10000: 0x0B,
        5000: 0x0C,
    }

    FRAMETYPE = {"STD": 0x01, "EXT": 0x02}

    OPERATIONMODE = {
        "normal": 0x00,
        "loopback": 0x01,
        "silent": 0x02,
        "loopback_and_silent": 0x03,
    }

    def __init__(
        self,
        channel,
        baudrate=2000000,
        timeout=0.1,
        frame_type="STD",
        operation_mode="normal",
        bitrate=500000,
        **kwargs,
    ):
        """
        :param str channel:
            The serial device to open. For example "/dev/ttyS1" or
            "/dev/ttyUSB0" on Linux or "COM1" on Windows systems.

        :param baudrate:
            The default matches required baudrate

        :param float timeout:
            Timeout for the serial device in seconds (default 0.1).

        :param str frame_type:
            STD or EXT, to select standard or extended messages

        :param operation_mode
            normal, loopback, silent or loopback_and_silent.

        :param bitrate
            CAN bus bit rate, selected from available list.

        :raises can.CanInitializationError: If the given parameters are invalid.
        :raises can.CanInterfaceNotImplementedError: If the serial module is not installed.
        """

        if serial is None:
            raise can.CanInterfaceNotImplementedError(
                "the serial module is not installed"
            )

        self.bit_rate = bitrate
        self.frame_type = frame_type
        self.op_mode = operation_mode
        self.filter_id = bytearray([0x00, 0x00, 0x00, 0x00])
        self.mask_id = bytearray([0x00, 0x00, 0x00, 0x00])
        self._can_protocol = CanProtocol.CAN_20

        if not channel:
            raise can.CanInitializationError("Must specify a serial port.")

        self.channel_info = "Serial interface: " + channel
        try:
            self.ser = serial.Serial(
                channel, baudrate=baudrate, timeout=timeout, rtscts=False
            )
        except ValueError as error:
            raise can.CanInitializationError(
                "could not create the serial device"
            ) from error

        super().__init__(channel=channel, **kwargs)
        self.init_frame()

    def shutdown(self):
        """
        Close the serial interface.
        """
        super().shutdown()
        self.ser.close()

    def init_frame(self, timeout=None):
        """
        Send init message to setup the device for comms. this is called during
        interface creation.

        :param timeout:
            This parameter will be ignored. The timeout value of the channel is
            used instead.
        """
        byte_msg = bytearray()
        byte_msg.append(0xAA)  # Frame Start Byte 1
        byte_msg.append(0x55)  # Frame Start Byte 2
        byte_msg.append(0x12)  # Initialization Message ID
        byte_msg.append(SeeedBus.BITRATE[self.bit_rate])  # CAN Baud Rate
        byte_msg.append(SeeedBus.FRAMETYPE[self.frame_type])
        byte_msg.extend(self.filter_id)
        byte_msg.extend(self.mask_id)
        byte_msg.append(SeeedBus.OPERATIONMODE[self.op_mode])
        byte_msg.append(0x01)  # Follows 'Send once' in windows app.

        byte_msg.extend([0x00] * 4)  # Manual bitrate config, details unknown.

        crc = sum(byte_msg[2:]) & 0xFF
        byte_msg.append(crc)

        logger.debug("init_frm:\t%s", byte_msg.hex())
        try:
            self.ser.write(byte_msg)
        except Exception as error:
            raise can.CanInitializationError("could send init frame") from error

    def flush_buffer(self):
        self.ser.flushInput()

    def status_frame(self, timeout=None):
        """
        Send status request message over the serial device.  The device will
        respond but details of error codes are unknown but are logged - DEBUG.

        :param timeout:
            This parameter will be ignored. The timeout value of the channel is
            used instead.
        """
        byte_msg = bytearray()
        byte_msg.append(0xAA)  # Frame Start Byte 1
        byte_msg.append(0x55)  # Frame Start Byte 2
        byte_msg.append(0x04)  # Status Message ID
        byte_msg.append(0x00)  # In response packet - Rx error count
        byte_msg.append(0x00)  # In response packet - Tx error count

        byte_msg.extend([0x00] * 14)

        crc = sum(byte_msg[2:]) & 0xFF
        byte_msg.append(crc)

        logger.debug("status_frm:\t%s", byte_msg.hex())
        self._write(byte_msg)

    def send(self, msg, timeout=None):
        """
        Send a message over the serial device.

        :param can.Message msg:
            Message to send.

        :param timeout:
            This parameter will be ignored. The timeout value of the channel is
            used instead.
        """

        byte_msg = bytearray()
        byte_msg.append(0xAA)

        m_type = 0xC0
        if msg.is_extended_id:
            m_type += 1 << 5

        if msg.is_remote_frame:
            m_type += 1 << 4

        m_type += msg.dlc
        byte_msg.append(m_type)

        if msg.is_extended_id:
            a_id = struct.pack("<I", msg.arbitration_id)
        else:
            a_id = struct.pack("<H", msg.arbitration_id)

        byte_msg.extend(a_id)
        byte_msg.extend(msg.data)
        byte_msg.append(0x55)

        logger.debug("sending:\t%s", byte_msg.hex())
        self._write(byte_msg)

    def _write(self, byte_msg: bytearray) -> None:
        try:
            self.ser.write(byte_msg)
        except serial.PortNotOpenError as error:
            raise can.CanOperationError("writing to closed port") from error
        except serial.SerialTimeoutException as error:
            raise can.CanTimeoutError() from error

    def _recv_internal(self, timeout):
        """
        Read a message from the serial device.

        :param timeout:

            .. warning::
                This parameter will be ignored. The timeout value of the
                channel is used.

        :returns:
            Received message and False (because not filtering as taken place).

        :rtype:
            can.Message, bool
        """
        try:
            # ser.read can return an empty string
            # or raise a SerialException
            rx_byte_1 = self.ser.read()

        except serial.PortNotOpenError as error:
            raise can.CanOperationError("reading from closed port") from error
        except serial.SerialException:
            return None, False

        if rx_byte_1 and ord(rx_byte_1) == 0xAA:
            try:
                rx_byte_2 = ord(self.ser.read())

                time_stamp = time()
                if rx_byte_2 == 0x55:
                    status = bytearray([0xAA, 0x55])
                    status += bytearray(self.ser.read(18))
                    logger.debug("status resp:\t%s", status.hex())

                else:
                    length = int(rx_byte_2 & 0x0F)
                    is_extended = bool(rx_byte_2 & 0x20)
                    is_remote = bool(rx_byte_2 & 0x10)
                    if is_extended:
                        s_3_4_5_6 = bytearray(self.ser.read(4))
                        arb_id = (struct.unpack("<I", s_3_4_5_6))[0]

                    else:
                        s_3_4 = bytearray(self.ser.read(2))
                        arb_id = (struct.unpack("<H", s_3_4))[0]

                    data = bytearray(self.ser.read(length))
                    end_packet = ord(self.ser.read())
                    if end_packet == 0x55:
                        msg = Message(
                            timestamp=time_stamp,
                            arbitration_id=arb_id,
                            is_extended_id=is_extended,
                            is_remote_frame=is_remote,
                            dlc=length,
                            data=data,
                        )
                        logger.debug("recv message: %s", str(msg))
                        return msg, False

                    else:
                        return None, False

            except serial.PortNotOpenError as error:
                raise can.CanOperationError("reading from closed port") from error
            except serial.SerialException as error:
                raise can.CanOperationError(
                    "failed to read message information"
                ) from error

        return None, None

    def fileno(self):
        try:
            return self.ser.fileno()
        except io.UnsupportedOperation as excption:
            logger.warning(
                "fileno is not implemented using current CAN bus: %s", str(excption)
            )
            return -1
