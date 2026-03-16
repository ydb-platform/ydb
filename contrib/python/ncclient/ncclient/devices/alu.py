from lxml import etree

from .default import DefaultDeviceHandler
from ncclient.operations.third_party.alu.rpc import GetConfiguration, LoadConfiguration, ShowCLI
from ncclient.xml_ import BASE_NS_1_0


def remove_namespaces(xml):

    for elem in xml.getiterator():
        if elem.tag is etree.Comment:
            continue
        i = elem.tag.find('}')
        if i > 0:
            elem.tag = elem.tag[i + 1:]

    etree.cleanup_namespaces(xml)
    return xml


class AluDeviceHandler(DefaultDeviceHandler):
    """
    Alcatel-Lucent 7x50 handler for device specific information.
    """

    def __init__(self, device_params, ignore_errors=None):
        super(AluDeviceHandler, self).__init__(device_params, ignore_errors)

    def get_capabilities(self):
        return [
            "urn:ietf:params:netconf:base:1.0",
        ]

    def get_xml_base_namespace_dict(self):
        return {None: BASE_NS_1_0}

    def get_xml_extra_prefix_kwargs(self):
        d = {}
        d.update(self.get_xml_base_namespace_dict())
        return {"nsmap": d}

    def add_additional_operations(self):
        dict = {}
        dict["get_configuration"] = GetConfiguration
        dict["show_cli"] = ShowCLI
        dict["load_configuration"] = LoadConfiguration
        return dict

    def transform_reply(self):
        return remove_namespaces
