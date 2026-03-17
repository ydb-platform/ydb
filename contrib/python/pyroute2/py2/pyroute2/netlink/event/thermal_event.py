'''
TODO: add THERMAL_GENL_ATTR_EVENT structure
'''
from pyroute2.netlink import genlmsg
from pyroute2.netlink.nlsocket import Marshal
from pyroute2.netlink.event import EventSocket

THERMAL_GENL_CMD_UNSPEC = 0
THERMAL_GENL_CMD_EVENT = 1


class thermal_msg(genlmsg):
    nla_map = (('THERMAL_GENL_ATTR_UNSPEC', 'none'),
               ('THERMAL_GENL_ATTR_EVENT', 'hex'))  # to be done


class MarshalThermalEvent(Marshal):
    msg_map = {THERMAL_GENL_CMD_UNSPEC: thermal_msg,
               THERMAL_GENL_CMD_EVENT: thermal_msg}


class ThermalEventSocket(EventSocket):
    marshal_class = MarshalThermalEvent
    genl_family = 'thermal_event'
