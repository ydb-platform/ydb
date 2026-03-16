from .default import DefaultDeviceHandler
from ncclient.xml_ import BASE_NS_1_0


class CienaDeviceHandler(DefaultDeviceHandler):
    """
    Ciena handler for device specific information.
    """

    def __init__(self, device_params, ignore_errors=None):
        super(CienaDeviceHandler, self).__init__(device_params, ignore_errors)

    def get_xml_base_namespace_dict(self):
        return {None: BASE_NS_1_0}

    def get_xml_extra_prefix_kwargs(self):
        d = {}
        d.update(self.get_xml_base_namespace_dict())
        return {"nsmap": d}
