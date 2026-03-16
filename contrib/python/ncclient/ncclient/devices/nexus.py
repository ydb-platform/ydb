"""
Handler for Cisco Nexus device specific information.

Note that for proper import, the classname has to be:

    "<Devicename>DeviceHandler"

...where <Devicename> is something like "Default", "Nexus", etc.

All device-specific handlers derive from the DefaultDeviceHandler, which implements the
generic information needed for interaction with a Netconf server.

"""

from ncclient.xml_ import BASE_NS_1_0
from ncclient.operations.third_party.nexus.rpc import ExecCommand

from .default import DefaultDeviceHandler

class NexusDeviceHandler(DefaultDeviceHandler):
    """
    Cisco Nexus handler for device specific information.

    In the device_params dictionary, which is passed to __init__, you can specify
    the parameter "ssh_subsystem_name". That allows you to configure the preferred
    SSH subsystem name that should be tried on your Nexus switch. If connecting with
    that name fails, or you didn't specify that name, the other known subsystem names
    will be tried. However, if you specify it then this name will be tried first.

    """
    _EXEMPT_ERRORS = [
        "*VLAN with the same name exists*", # returned even if VLAN was created, but
                                            # name was already in use (switch will
                                            # automatically choose different, unique
                                            # name for VLAN)
    ]

    def __init__(self, device_params, ignore_errors=None):
        super(NexusDeviceHandler, self).__init__(device_params, ignore_errors)

    def add_additional_operations(self):
        dict = {}
        dict['exec_command'] = ExecCommand
        return dict

    def get_capabilities(self):
        # Just need to replace a single value in the default capabilities
        c = super(NexusDeviceHandler, self).get_capabilities()
        c[0] = "urn:ietf:params:xml:ns:netconf:base:1.0"
        return c

    def get_xml_base_namespace_dict(self):
        """
        Base namespace needs a None key.

        See 'nsmap' argument for lxml's Element().

        """
        return { None : BASE_NS_1_0 }

    def get_xml_extra_prefix_kwargs(self):
        """
        Return keyword arguments per request, which are applied to Element().

        Mostly, this is a dictionary containing the "nsmap" key.

        See 'nsmap' argument for lxml's Element().

        """
        d = {
                "nxos":"http://www.cisco.com/nxos:1.0",
                "if":"http://www.cisco.com/nxos:1.0:if_manager",
                "nfcli": "http://www.cisco.com/nxos:1.0:nfcli",
                "vlan_mgr_cli": "http://www.cisco.com/nxos:1.0:vlan_mgr_cli"
            }
        d.update(self.get_xml_base_namespace_dict())
        return { "nsmap" : d }

    def get_ssh_subsystem_names(self):
        """
        Return a list of possible SSH subsystem names.

        Different NXOS versions use different SSH subsystem names for netconf.
        Therefore, we return a list so that several can be tried, if necessary.

        The Nexus device handler also accepts

        """
        preferred_ssh_subsystem = self.device_params.get("ssh_subsystem_name")
        name_list = [ "netconf", "xmlagent" ]
        if preferred_ssh_subsystem:
            return [ preferred_ssh_subsystem ] + \
                        [ n for n in name_list if n != preferred_ssh_subsystem ]
        else:
            return name_list

