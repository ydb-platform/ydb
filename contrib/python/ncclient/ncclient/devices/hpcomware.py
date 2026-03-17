from ncclient.xml_ import BASE_NS_1_0
from .default import DefaultDeviceHandler
from ncclient.operations.third_party.hpcomware.rpc import DisplayCommand, ConfigCommand, Action, Rollback, Save

class HpcomwareDeviceHandler(DefaultDeviceHandler):

    def __init__(self, device_params, ignore_errors=None):
        super(HpcomwareDeviceHandler, self).__init__(device_params, ignore_errors)

    def get_xml_base_namespace_dict(self):
        """
        Base namespace needs a None key.

        See 'nsmap' argument for lxml's Element().

        """
        return {None: BASE_NS_1_0}

    def get_xml_extra_prefix_kwargs(self):
        """
        Return keyword arguments per request, which are applied to Element().

        Mostly, this is a dictionary containing the "nsmap" key.

        See 'nsmap' argument for lxml's Element().

        """
        d = {
                "data": "http://www.hp.com/netconf/data:1.0",
                "config": "http://www.hp.com/netconf/config:1.0",
            }
        d.update(self.get_xml_base_namespace_dict())
        return {"nsmap": d}

    def add_additional_operations(self):
        addtl = {}
        addtl['cli_display'] = DisplayCommand
        addtl['cli_config'] = ConfigCommand
        addtl['action'] = Action
        addtl['rollback'] = Rollback
        addtl['save'] = Save
        return addtl
