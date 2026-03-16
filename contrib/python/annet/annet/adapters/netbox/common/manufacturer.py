from logging import getLogger

from annet.annlib.netdev.views.hardware import HardwareView


logger = getLogger(__name__)


def get_hw(manufacturer: str, model: str, platform_name: str):
    # By some reason Netbox calls Mellanox SN as MSN, so we fix them here
    if manufacturer == "Mellanox" and model.startswith("MSN"):
        model = model.replace("MSN", "SN", 1)

    return HardwareView(manufacturer + " " + model, platform_name)


def get_breed(manufacturer: str, model: str):
    hw = get_hw(manufacturer, model, "")
    if hw.Huawei.CE:
        return "vrp85"
    elif hw.Huawei.NE:
        return "vrp85"
    elif hw.Huawei:
        return "vrp55"
    elif hw.H3C:
        return "h3c"
    elif hw.PC.NVIDIA or hw.PC.Mellanox:
        return "cuml2"
    elif hw.Juniper:
        return "jun10"
    elif hw.Cisco.NCS:
        return "ios12"
    elif hw.Cisco.Nexus:
        return "nxos"
    elif hw.Cisco:
        return "ios12"
    elif hw.Arista:
        return "eos4"
    elif hw.B4com:
        return "bcom-os"
    elif hw.RouterOS:
        return "routeros"
    elif hw.PC.Moxa or hw.PC.Nebius:
        return "moxa"
    elif hw.PC:
        return "pc"
    return ""
