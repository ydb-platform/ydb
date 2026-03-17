"""
Handler for Huawei device specific information.

Note that for proper import, the classname has to be:

    "<Devicename>DeviceHandler"

...where <Devicename> is something like "Default", "Huawei", etc.

All device-specific handlers derive from the DefaultDeviceHandler, which implements the
generic information needed for interaction with a Netconf server.

"""
from ncclient.operations.third_party.huawei.rpc import *

from ncclient.xml_ import BASE_NS_1_0

from .default import DefaultDeviceHandler

class HuaweiDeviceHandler(DefaultDeviceHandler):
    """
    Huawei handler for device specific information.

    In the device_params dictionary, which is passed to __init__, you can specify
    the parameter "ssh_subsystem_name". That allows you to configure the preferred
    SSH subsystem name that should be tried on your Huawei switch. If connecting with
    that name fails, or you didn't specify that name, the other known subsystem names
    will be tried. However, if you specify it then this name will be tried first.

    """
    _EXEMPT_ERRORS = []

    def __init__(self, device_params, ignore_errors=None):
        super(HuaweiDeviceHandler, self).__init__(device_params, ignore_errors)


    def add_additional_operations(self):
        dict = {}
        dict["cli"] = CLI
        dict["action"] = Action
        return dict

    def handle_raw_dispatch(self, raw):
        return raw.strip('\0')

    def get_capabilities(self):
        # Just need to replace a single value in the default capabilities
        c = super(HuaweiDeviceHandler, self).get_capabilities()
        c.append('http://www.huawei.com/netconf/capability/execute-cli/1.0')
        c.append('http://www.huawei.com/netconf/capability/action/1.0')
        c.append('http://www.huawei.com/netconf/capability/active/1.0')
        c.append('http://www.huawei.com/netconf/capability/discard-commit/1.0')
        c.append('http://www.huawei.com/netconf/capability/exchange/1.0')
        
        return c

    def get_xml_base_namespace_dict(self):
        return {None: BASE_NS_1_0}

    def get_xml_extra_prefix_kwargs(self):
        d = {}
        d.update(self.get_xml_base_namespace_dict())
        return {"nsmap": d}

    def perform_qualify_check(self):
        return False
