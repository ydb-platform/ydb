
__all__ = ['CanMessage']

from binascii import hexlify


class CanMessage:
    """
    Represent a CAN message (ISO-11898)

    :param arbitration_id: The CAN arbitration ID. Must be a 11 bits value or a 29 bits value if ``extended_id`` is True
    :type arbitration_id: int

    :param dlc: The Data Length Code representing the number of bytes in the data field
    :type dlc: int

    :param data: The 8 bytes payload of the message
    :type data: bytearray

    :param extended_id: When True, the arbitration ID stands on 29 bits. 11 bits when False
    :type extended_id: bool

    :param is_fd: When True, message has to be transmitted or has been received in a CAN FD frame. CAN frame when set to False
    :type is_fd: bool

    :param bitrate_switch: When True, message has the Bit Rate Switch (BRS) bit set.
    :type bitrate_switch: bool
    """
    __slots__ = 'arbitration_id', 'dlc', 'data', 'is_extended_id', 'is_fd', 'bitrate_switch'

    arbitration_id: int
    dlc: int
    data: bytes
    extended_id: bool
    is_fd: bool
    bitrate_switch: bool

    def __init__(self,
                 arbitration_id: int = 0,
                 dlc: int = 0,
                 data: bytes = bytes(),
                 extended_id: bool = False,
                 is_fd: bool = False,
                 bitrate_switch: bool = False
                 ):
        self.arbitration_id = arbitration_id
        self.dlc = dlc
        self.data = data
        self.is_extended_id = extended_id
        self.is_fd = is_fd
        self.bitrate_switch = bitrate_switch

    def __repr__(self) -> str:
        data_str = hexlify(self.data[:64]).decode('ascii')
        if len(self.data) > 64:
            data_str += '...'
        if self.is_extended_id:
            id_str = "%08x" % self.arbitration_id
        else:
            id_str = "%03x" % self.arbitration_id

        addr = "0x%x" % id(self)
        flags = []
        if self.is_fd:
            flags.append('fd')
        if self.bitrate_switch:
            flags.append('bs')

        flag_str = ','.join(flags)
        if flag_str:
            flag_str = f'({flag_str})'

        return f'<{self.__class__.__name__} {id_str} [{self.dlc}] {flag_str} "{data_str}" at {addr}>'
