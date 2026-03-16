'''
'''
from pyroute2.netlink import nla
from pyroute2.netlink import genlmsg
from pyroute2.netlink.nlsocket import Marshal
from pyroute2.netlink.event import EventSocket

ACPI_GENL_CMD_UNSPEC = 0
ACPI_GENL_CMD_EVENT = 1


class acpimsg(genlmsg):
    nla_map = (('ACPI_GENL_ATTR_UNSPEC', 'none'),
               ('ACPI_GENL_ATTR_EVENT', 'acpiev'))

    class acpiev(nla):
        fields = (('device_class', '20s'),
                  ('bus_id', '15s'),
                  ('type', 'I'),
                  ('data', 'I'))

        def decode(self):
            nla.decode(self)
            dc = self['device_class']
            bi = self['bus_id']
            self['device_class'] = dc[:dc.find(b'\x00')]
            self['bus_id'] = bi[:bi.find(b'\x00')]


class MarshalAcpiEvent(Marshal):
    msg_map = {ACPI_GENL_CMD_UNSPEC: acpimsg,
               ACPI_GENL_CMD_EVENT: acpimsg}


class AcpiEventSocket(EventSocket):
    marshal_class = MarshalAcpiEvent
    genl_family = 'acpi_event'
