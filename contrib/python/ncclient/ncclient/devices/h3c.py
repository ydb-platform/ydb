"""
Handler for H3c device specific information.

Note that for proper import, the classname has to be:

    "<Devicename>DeviceHandler"

...where <Devicename> is something like "Default", "H3c", etc.

All device-specific handlers derive from the DefaultDeviceHandler, which implements the
generic information needed for interaction with a Netconf server.

"""

from .default import DefaultDeviceHandler
from ncclient.operations.third_party.h3c.rpc import *


class H3cDeviceHandler(DefaultDeviceHandler):
    """
    H3C handler for device specific information.

    In the device_params dictionary, which is passed to __init__, you can specify
    the parameter "ssh_subsystem_name". That allows you to configure the preferred
    SSH subsystem name that should be tried on your H3C switch. If connecting with
    that name fails, or you didn't specify that name, the other known subsystem names
    will be tried. However, if you specify it then this name will be tried first.

    """
    _EXEMPT_ERRORS = []

    def __init__(self, device_params, ignore_errors=None):
        super(H3cDeviceHandler, self).__init__(device_params, ignore_errors)

    def add_additional_operations(self):
        dict = {}
        dict["get_bulk"] = GetBulk
        dict["get_bulk_config"] = GetBulkConfig
        dict["cli"] = CLI
        dict["action"] = Action
        dict["save"] = Save
        dict["load"] = Load
        dict["rollback"] = Rollback
        return dict

    def get_capabilities(self):
        # Just need to replace a single value in the default capabilities
        c = super(H3cDeviceHandler, self).get_capabilities()
        return c

    def get_xml_base_namespace_dict(self):
        return {None: BASE_NS_1_0}

    def get_xml_extra_prefix_kwargs(self):
        d = {}
        d.update(self.get_xml_base_namespace_dict())
        return {"nsmap": d}

    def perform_qualify_check(self):
        return False
