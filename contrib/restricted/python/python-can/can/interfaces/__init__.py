"""
Interfaces contain low level implementations that interact with CAN hardware.
"""

from can._entry_points import read_entry_points

__all__ = [
    "BACKENDS",
    "VALID_INTERFACES",
    "canalystii",
    "cantact",
    "etas",
    "gs_usb",
    "ics_neovi",
    "iscan",
    "ixxat",
    "kvaser",
    "neousys",
    "nican",
    "nixnet",
    "pcan",
    "robotell",
    "seeedstudio",
    "serial",
    "slcan",
    "socketcan",
    "socketcand",
    "systec",
    "udp_multicast",
    "usb2can",
    "vector",
    "virtual",
]

# interface_name => (module, classname)
BACKENDS: dict[str, tuple[str, str]] = {
    "kvaser": ("can.interfaces.kvaser", "KvaserBus"),
    "socketcan": ("can.interfaces.socketcan", "SocketcanBus"),
    "serial": ("can.interfaces.serial.serial_can", "SerialBus"),
    "pcan": ("can.interfaces.pcan", "PcanBus"),
    "usb2can": ("can.interfaces.usb2can", "Usb2canBus"),
    "ixxat": ("can.interfaces.ixxat", "IXXATBus"),
    "nican": ("can.interfaces.nican", "NicanBus"),
    "iscan": ("can.interfaces.iscan", "IscanBus"),
    "virtual": ("can.interfaces.virtual", "VirtualBus"),
    "udp_multicast": ("can.interfaces.udp_multicast", "UdpMulticastBus"),
    "neovi": ("can.interfaces.ics_neovi", "NeoViBus"),
    "vector": ("can.interfaces.vector", "VectorBus"),
    "slcan": ("can.interfaces.slcan", "slcanBus"),
    "robotell": ("can.interfaces.robotell", "robotellBus"),
    "canalystii": ("can.interfaces.canalystii", "CANalystIIBus"),
    "systec": ("can.interfaces.systec", "UcanBus"),
    "seeedstudio": ("can.interfaces.seeedstudio", "SeeedBus"),
    "cantact": ("can.interfaces.cantact", "CantactBus"),
    "gs_usb": ("can.interfaces.gs_usb", "GsUsbBus"),
    "nixnet": ("can.interfaces.nixnet", "NiXNETcanBus"),
    "neousys": ("can.interfaces.neousys", "NeousysBus"),
    "etas": ("can.interfaces.etas", "EtasBus"),
    "socketcand": ("can.interfaces.socketcand", "SocketCanDaemonBus"),
}


BACKENDS.update(
    {
        interface.key: (interface.module_name, interface.class_name)
        for interface in read_entry_points(group="can.interface")
    }
)

VALID_INTERFACES = frozenset(sorted(BACKENDS.keys()))
