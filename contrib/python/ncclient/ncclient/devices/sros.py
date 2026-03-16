from .default import DefaultDeviceHandler
from ncclient.operations.third_party.sros.rpc import MdCliRawCommand, Commit
from ncclient.xml_ import BASE_NS_1_0


class ConfigMode:
    PRIVATE = 'private'

def passthrough(xml):
    return xml

class SrosDeviceHandler(DefaultDeviceHandler):
    """
    Nokia SR OS handler for device specific information.
    """

    def __init__(self, device_params, ignore_errors=None):
        super(SrosDeviceHandler, self).__init__(device_params, ignore_errors)

    def get_capabilities(self):
        """Set SR OS device handler client capabilities

        Set additional capabilities beyond the default device handler.

        Returns:
            A list of strings representing NETCONF capabilities to be
            sent to the server.
        """
        base = super(SrosDeviceHandler, self).get_capabilities()
        additional = [
            'urn:ietf:params:xml:ns:netconf:base:1.0',
            'urn:ietf:params:xml:ns:yang:1',
            'urn:ietf:params:netconf:capability:confirmed-commit:1.1',
            'urn:ietf:params:netconf:capability:validate:1.1',
        ]
        if self.device_params.get('config_mode') == ConfigMode.PRIVATE:
            additional.append('urn:nokia.com:nc:pc')

        return base + additional

    def get_xml_base_namespace_dict(self):
        return {None: BASE_NS_1_0}

    def get_xml_extra_prefix_kwargs(self):
        d = {}
        d.update(self.get_xml_base_namespace_dict())
        return {"nsmap": d}

    def add_additional_operations(self):
        operations = {
            'md_cli_raw_command': MdCliRawCommand,
            'commit': Commit,
        }
        return operations

    def perform_qualify_check(self):
        return False

    def transform_reply(self):
        return passthrough
