"""
Handler for Huawei device specific information through YANG.

Note that for proper import, the classname has to be:

    "<Devicename>DeviceHandler"

...where <Devicename> is something like "Default", "Huawei", etc.

All device-specific handlers derive from the DefaultDeviceHandler, which implements the
generic information needed for interaction with a Netconf server.

"""
from ncclient.xml_ import BASE_NS_1_0

from .default import DefaultDeviceHandler

class HuaweiyangDeviceHandler(DefaultDeviceHandler):
    """
    Huawei handler for device specific information .

    In the device_params dictionary, which is passed to __init__, you can specify
    the parameter "ssh_subsystem_name". That allows you to configure the preferred
    SSH subsystem name that should be tried on your Huawei switch. If connecting with
    that name fails, or you didn't specify that name, the other known subsystem names
    will be tried. However, if you specify it then this name will be tried first.

    """
    _EXEMPT_ERRORS = []

    def __init__(self, device_params, ignore_errors=None):
        super(HuaweiyangDeviceHandler, self).__init__(device_params, ignore_errors)

    def get_capabilities(self):
        # Just need to replace a single value in the default capabilities
        c = []
        c.append('urn:ietf:params:netconf:base:1.0')
        c.append('urn:ietf:params:netconf:base:1.1')
        
        return c

    def get_xml_base_namespace_dict(self):
        return {None: BASE_NS_1_0}

    def get_xml_extra_prefix_kwargs(self):
        d = {}
        d.update(self.get_xml_base_namespace_dict())
        return {"nsmap": d}
