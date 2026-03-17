"""
Handler for Ericsson device specific information.

Note that for proper import, the classname has to be:

    "<Devicename>DeviceHandler"

...where <Devicename> is something like "Default", "Ericsson", etc.

All device-specific handlers derive from the DefaultDeviceHandler, which implements the
generic information needed for interaction with a Netconf server.

"""
from ncclient.xml_ import BASE_NS_1_0
from ncclient.operations.errors import OperationError
from .default import DefaultDeviceHandler


class EricssonDeviceHandler(DefaultDeviceHandler):
    """
    Ericsson handler for device specific information.

    """
    _EXEMPT_ERRORS = []

    def __init__(self, device_params, ignore_errors=None):
        super(EricssonDeviceHandler, self).__init__(device_params, ignore_errors)

    def get_xml_base_namespace_dict(self):
        return {None: BASE_NS_1_0}

    def get_xml_extra_prefix_kwargs(self):
        d = {}
        if self.check_device_params() is False:
            d.update(self.get_xml_base_namespace_dict())
        return {"nsmap": d}

    def check_device_params(self):
        value = self.device_params.get('with_ns')
        if value in [True, False]:
            return value
        elif value is None:
            return False
        else:
            raise OperationError('Invalid "with_ns" value: %s' % value)
