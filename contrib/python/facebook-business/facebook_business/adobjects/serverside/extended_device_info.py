# Copyright 2014 Facebook, Inc.

# You are hereby granted a non-exclusive, worldwide, royalty-free license to
# use, copy, modify, and distribute this software in source code or binary
# form for use in connection with the web services and APIs provided by
# Facebook.

# As with any software that integrates with the Facebook platform, your use
# of this software is subject to the Facebook Developer Principles and
# Policies [http://developers.facebook.com/policy/]. This copyright notice
# shall be included in all copies or substantial portions of the software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

class ExtendedDeviceInfo(object):

    EXT_INFO_VERSION        = 0
    APP_PACKAGE_NAME        = 1
    SHORT_VERSION           = 2
    LONG_VERSION            = 3
    OS_VERSION              = 4
    DEVICE_MODEL_NAME       = 5
    LOCALE                  = 6
    TIMEZONE_ABBREVIATION   = 7
    CARRIER                 = 8
    SCREEN_WIDTH            = 9
    SCREEN_HEIGHT           = 10
    SCREEN_DENSITY          = 11
    CPU_CORE_COUNT          = 12
    TOTAL_DISK_SPACE_GB     = 13
    FREE_DISK_SPACE_GB      = 14
    DEVICE_TIME_ZONE        = 15

    param_types = {
        'ext_info_version': 'str',
        'app_package_name': 'str',
        'short_version': 'str',
        'long_version': 'str',
        'os_version': 'str',
        'device_model_name': 'str',
        'locale': 'str',
        'timezone_abbreviation': 'str',
        'carrier': 'str',
        'screen_width': 'int',
        'screen_height': 'int',
        'screen_density': 'str',
        'cpu_core_count': 'int',
        'total_disk_space_gb': 'int',
        'free_disk_space_gb': 'int',
        'device_time_zone': 'str',
    }

    def __init__(
        self,
        ext_info_version=None,
        app_package_name=None,
        short_version=None,
        long_version=None,
        os_version=None,
        device_model_name=None,
        locale=None,
        timezone_abbreviation=None,
        carrier=None,
        screen_width=None,
        screen_height=None,
        screen_density=None,
        cpu_core_count=None,
        total_disk_space_gb=None,
        free_disk_space_gb=None,
        device_time_zone=None,
    ):
        # type: (str, str, str, str, str, str, str, str, str, int, int, str, int, int, int, str) -> None

        """ExtendedDeviceInfo"""
        self._ext_info_version = None
        self._app_package_name = None
        self._short_version = None
        self._long_version = None
        self._os_version = None
        self._device_model_name = None
        self._locale = None
        self._timezone_abbreviation = None
        self._carrier = None
        self._screen_width = None
        self._screen_height = None
        self._screen_density = None
        self._cpu_core_count = None
        self._total_disk_space_gb = None
        self._free_disk_space_gb = None
        self._device_time_zone = None

        if ext_info_version is not None:
            self.ext_info_version = ext_info_version
        if app_package_name is not None:
            self.app_package_name = app_package_name
        if short_version is not None:
            self.short_version = short_version
        if long_version is not None:
            self.long_version = long_version
        if os_version is not None:
            self.os_version = os_version
        if device_model_name is not None:
            self.device_model_name = device_model_name
        if locale is not None:
            self.locale = locale
        if timezone_abbreviation is not None:
            self.timezone_abbreviation = timezone_abbreviation
        if carrier is not None:
            self.carrier = carrier
        if screen_width is not None:
            self.screen_width = screen_width
        if screen_height is not None:
            self.screen_height = screen_height
        if screen_density is not None:
            self.screen_density = screen_density
        if cpu_core_count is not None:
            self.cpu_core_count = cpu_core_count
        if total_disk_space_gb is not None:
            self.total_disk_space_gb = total_disk_space_gb
        if free_disk_space_gb is not None:
            self.free_disk_space_gb = free_disk_space_gb
        if device_time_zone is not None:
            self.device_time_zone = device_time_zone

    @property
    def ext_info_version(self):
        return self._ext_info_version

    @ext_info_version.setter
    def ext_info_version(self, ext_info_version):
        self._ext_info_version = ext_info_version

    @property
    def app_package_name(self):
        return self._app_package_name

    @app_package_name.setter
    def app_package_name(self, app_package_name):
         self._app_package_name = app_package_name

    @property
    def short_version(self):
        return self._short_version

    @short_version.setter
    def short_version(self, short_version):
         self._short_version = short_version

    @property
    def long_version(self):
        return self._long_version

    @long_version.setter
    def long_version(self, long_version):
        self._long_version = long_version

    @property
    def os_version(self):
        return self._os_version

    @os_version.setter
    def os_version(self, os_version):
        self._os_version = os_version

    @property
    def device_model_name(self):
        return self._device_model_name

    @device_model_name.setter
    def device_model_name(self, device_model_name):
        self._device_model_name = device_model_name

    @property
    def locale(self):
        return self._locale

    @locale.setter
    def locale(self, locale):
        self._locale = locale

    @property
    def timezone_abbreviation(self):
        return self._timezone_abbreviation

    @timezone_abbreviation.setter
    def timezone_abbreviation(self, timezone_abbreviation):
        self._timezone_abbreviation = timezone_abbreviation

    @property
    def carrier(self):
        return self._carrier

    @carrier.setter
    def carrier(self, carrier):
        self._carrier = carrier

    @property
    def screen_width(self):
        return self._screen_width

    @screen_width.setter
    def screen_width(self, screen_width):
        self._screen_width = screen_width

    @property
    def screen_height(self):
        return self._screen_height

    @screen_height.setter
    def screen_height(self, screen_height):
        self._screen_height = screen_height

    @property
    def screen_density(self):
        return self._screen_density

    @screen_density.setter
    def screen_density(self, screen_density):
        self._screen_density = screen_density

    @property
    def cpu_core_count(self):
        return self._cpu_core_count

    @cpu_core_count.setter
    def cpu_core_count(self, cpu_core_count):
        self._cpu_core_count = cpu_core_count

    @property
    def total_disk_space_gb(self):
        return self._total_disk_space_gb

    @total_disk_space_gb.setter
    def total_disk_space_gb(self, total_disk_space_gb):
        self._total_disk_space_gb = total_disk_space_gb

    @property
    def free_disk_space_gb(self):
        return self._free_disk_space_gb

    @free_disk_space_gb.setter
    def free_disk_space_gb(self, free_disk_space_gb):
        self._free_disk_space_gb = free_disk_space_gb

    @property
    def device_time_zone(self):
        return self._device_time_zone

    @device_time_zone.setter
    def device_time_zone(self, device_time_zone):
        self._device_time_zone = device_time_zone

    def normalize(self):
        extended_device_info_array = []
        extended_device_info_array.insert(self.EXT_INFO_VERSION, self._ext_info_version)
        extended_device_info_array.insert(self.APP_PACKAGE_NAME, self._app_package_name)
        extended_device_info_array.insert(self.SHORT_VERSION, self._short_version)
        extended_device_info_array.insert(self.LONG_VERSION, self._long_version)
        extended_device_info_array.insert(self.OS_VERSION, self._os_version)
        extended_device_info_array.insert(self.DEVICE_MODEL_NAME, self._device_model_name)
        extended_device_info_array.insert(self.LOCALE, self._locale)
        extended_device_info_array.insert(self.TIMEZONE_ABBREVIATION, self._timezone_abbreviation)
        extended_device_info_array.insert(self.CARRIER, self._carrier)
        extended_device_info_array.insert(self.SCREEN_WIDTH, self._screen_width)
        extended_device_info_array.insert(self.SCREEN_HEIGHT, self._screen_height)
        extended_device_info_array.insert(self.SCREEN_DENSITY, self._screen_density)
        extended_device_info_array.insert(self.CPU_CORE_COUNT, self._cpu_core_count)
        extended_device_info_array.insert(self.TOTAL_DISK_SPACE_GB, self._total_disk_space_gb)
        extended_device_info_array.insert(self.FREE_DISK_SPACE_GB, self._free_disk_space_gb)
        extended_device_info_array.insert(self.DEVICE_TIME_ZONE, self._device_time_zone)
        return extended_device_info_array

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.param_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value
        if issubclass(ExtendedDeviceInfo, dict):
            for key, value in self.items():
                result[key] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, Event):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
