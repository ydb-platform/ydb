__all__ = ['CommunicationType']

# Communication type is a single byte value including message type and subnet.
# Used by CommunicationControl service and defined by ISO-14229:2006 Annex B, table B.1
import struct
from udsoncan import tools

from typing import Union, List


class CommunicationType:
    """
    This class represents a pair of subnet and message types. This value is mainly used by the :ref:`CommunicationControl<CommunicationControl>` service

    :param subnet: Represent the subnet number. Value ranges from 0 to 0xF 
    :type subnet: int

    :param normal_msg: Bit indicating that the `normal messages` are involved
    :type normal_msg: bool

    :param network_management_msg: Bit indicating that the `network management messages` are involved
    :type network_management_msg: bool

    """
    class Subnet:
        node = 0
        network = 0xF

        def __init__(self, subnet: int):
            tools.validate_int(subnet, min=0, max=0xF, name="subnet")
            self.subnet = subnet

        def value(self):
            return self.subnet

    subnet: Subnet
    normal_msg: bool
    network_management_msg: bool

    def __init__(self, subnet: Union[Subnet, int], normal_msg: bool = False, network_management_msg: bool = False):

        if not isinstance(subnet, self.Subnet):
            subnet = self.Subnet(subnet)

        if not isinstance(normal_msg, bool) or not isinstance(network_management_msg, bool):
            raise ValueError('message type (normal_msg, network_management_msg) must be valid boolean values')

        if normal_msg == False and network_management_msg == False:
            raise ValueError('At least one message type must be controlled')

        self.subnet = subnet
        self.normal_msg = normal_msg
        self.network_management_msg = network_management_msg

    def get_byte_as_int(self) -> int:
        message_type = 0
        if self.normal_msg:
            message_type |= 1
        if self.network_management_msg:
            message_type |= 2

        byte = (message_type & 0x3) | ((self.subnet.value() & 0xF) << 4)
        return byte

    def get_byte(self):
        return struct.pack('B', self.get_byte_as_int())

    @classmethod
    def from_byte(cls, val: Union[int, bytes]) -> "CommunicationType":
        if isinstance(val, bytes):
            val = struct.unpack('B', val)[0]
        val = int(val)
        subnet = (val & 0xF0) >> 4
        normal_msg = True if val & 1 > 0 else False
        network_management_msg = True if val & 2 > 0 else False
        return cls(subnet, normal_msg, network_management_msg)

    def __str__(self) -> str:
        flags: List[str] = []
        if self.normal_msg:
            flags.append('NormalMsg')

        if self.network_management_msg:
            flags.append('NetworkManagementMsg')

        return 'subnet=0x%x. Flags : [%s]' % (self.subnet.value(), ','.join(flags))

    def __repr__(self) -> str:
        return '<%s: %s at 0x%08x>' % (self.__class__.__name__, str(self), id(self))
