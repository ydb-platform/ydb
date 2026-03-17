from office365.runtime.client_value import ClientValue


class DeviceDetail(ClientValue):
    """Indicates device details associated with a device used for signing in. This includes information
    like device browser and operating system, and whether the device is Azure AD managed.
    """

    def __init__(self, browser=None, device_id=None, display_name=None):
        """
        :param str browser: Indicates the browser information of the used for signing in.
        :param str device_id: Refers to the UniqueID of the device used for signing in.
        :param str display_name: Refers to the name of the device used for signing in.
        """
        self.browser = browser
        self.deviceId = device_id
        self.displayName = display_name
