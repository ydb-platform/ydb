# supported devices config, add new device (eg: 'device name':'device label').
supported_devices_cfg = {
    "alu": "Alcatel Lucent",
    "ciena": "Ciena",
    "csr": "Cisco CSR1000v",
    "h3c": "H3C",
    "hpcomware": "HP Comware",
    "huawei": "Huawei",
    "huaweiyang": "Huawei",
    "iosxe": "Cisco IOS XE",
    "iosxr": "Cisco IOS XR",
    "junos": "Juniper",
    "nexus": "Cisco Nexus",
    "sros": "Nokia SR OS",
    "default": "Server or anything not in above",
}


def get_supported_devices():
    return tuple(supported_devices_cfg.keys())


def get_supported_device_labels():
    return supported_devices_cfg
