"""
Handler for Cisco IOS-XR device specific information.

Note that for proper import, the classname has to be:

    "<Devicename>DeviceHandler"

...where <Devicename> is something like "Default", "Nexus", etc.

All device-specific handlers derive from the DefaultDeviceHandler, which implements the
generic information needed for interaction with a Netconf server.

"""


from .default import DefaultDeviceHandler

def iosxr_unknown_host_cb(host, fingerprint):
        #This will ignore the unknown host check when connecting to IOS-XR devices
        return True

class IosxrDeviceHandler(DefaultDeviceHandler):
    """
    Cisco IOS-XR handler for device specific information.

    """
    def __init__(self, device_params, ignore_errors=None):
        super(IosxrDeviceHandler, self).__init__(device_params, ignore_errors)

    def add_additional_ssh_connect_params(self, kwargs):
        kwargs['unknown_host_cb'] = iosxr_unknown_host_cb

    def perform_qualify_check(self):
        return False
