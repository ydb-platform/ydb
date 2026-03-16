"""
Handler for Cisco CSR device specific information.

Note that for proper import, the classname has to be:

    "<Devicename>DeviceHandler"

...where <Devicename> is something like "Default", "Nexus", etc.

All device-specific handlers derive from the DefaultDeviceHandler, which implements the
generic information needed for interaction with a Netconf server.

"""


from .default import DefaultDeviceHandler
from warnings import warn

def csr_unknown_host_cb(host, fingerprint):
        #This will ignore the unknown host check when connecting to CSR devices
        return True

class CsrDeviceHandler(DefaultDeviceHandler):
    """
    Cisco CSR handler for device specific information.

    """
    def __init__(self, device_params, ignore_errors=None):
        warn(
            'CsrDeviceHandler is deprecated, please use IosxeDeviceHandler',
            DeprecationWarning,
            stacklevel=2)
        super(CsrDeviceHandler, self).__init__(device_params, ignore_errors)

    def add_additional_ssh_connect_params(self, kwargs):
        warn(
            'CsrDeviceHandler is deprecated, please use IosxeDeviceHandler',
            DeprecationWarning,
            stacklevel=2)
        kwargs['unknown_host_cb'] = csr_unknown_host_cb
