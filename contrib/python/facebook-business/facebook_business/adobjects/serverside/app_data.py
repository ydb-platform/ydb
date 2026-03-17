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

class AppData(object):

    param_types = {
        'application_tracking_enabled': 'bool',
        'advertiser_tracking_enabled': 'bool',
        'campaign_ids': 'str',
        'consider_views': 'bool',
        'extinfo': 'ExtendedDeviceInfo',
        'include_dwell_data': 'bool',
        'include_video_data': 'bool',
        'install_referrer': 'str',
        'installer_package': 'str',
        'receipt_data': 'str',
        'url_schemes': 'str',
        'windows_attribution_id': 'str',
    }

    def __init__(
        self,
        application_tracking_enabled=None,
        advertiser_tracking_enabled=None,
        campaign_ids=None,
        consider_views=None,
        extinfo=None,
        include_dwell_data=None,
        include_video_data=None,
        install_referrer=None,
        installer_package=None,
        receipt_data=None,
        url_schemes=None,
        windows_attribution_id=None,
    ):
        # type: (bool, bool, str, str, bool, ExtendedDeviceInfo, bool, bool, str, str, str, str, str) -> None

        """AppData which contains app data and device information for events happening from an app"""
        self._application_tracking_enabled = None
        self._advertiser_tracking_enabled = None
        self._campaign_ids = None
        self._consider_views = None
        self._extinfo = None
        self._include_dwell_data = None
        self._include_video_data = None
        self._install_referrer = None
        self._installer_package = None
        self._receipt_data = None
        self._url_schemes = None
        self._windows_attribution_id = None

        if application_tracking_enabled is not None:
            self.application_tracking_enabled = application_tracking_enabled
        if advertiser_tracking_enabled is not None:
            self.advertiser_tracking_enabled = advertiser_tracking_enabled
        if campaign_ids is not None:
            self.campaign_ids = campaign_ids
        if consider_views is not None:
            self.consider_views = consider_views
        if extinfo is not None:
            self.extinfo = extinfo
        if include_dwell_data is not None:
            self.include_dwell_data = include_dwell_data
        if include_video_data is not None:
            self.include_video_data = include_video_data
        if install_referrer is not None:
            self.install_referrer = install_referrer
        if installer_package is not None:
            self.installer_package = installer_package
        if receipt_data is not None:
            self.receipt_data = receipt_data
        if url_schemes is not None:
            self.url_schemes = url_schemes
        if windows_attribution_id is not None:
            self.windows_attribution_id = windows_attribution_id

    @property
    def application_tracking_enabled(self):
        return self._application_tracking_enabled

    @application_tracking_enabled.setter
    def application_tracking_enabled(self, application_tracking_enabled):
        self._application_tracking_enabled = application_tracking_enabled

    @property
    def advertiser_tracking_enabled(self):
        return self._advertiser_tracking_enabled

    @advertiser_tracking_enabled.setter
    def advertiser_tracking_enabled(self, advertiser_tracking_enabled):
        self._advertiser_tracking_enabled = advertiser_tracking_enabled

    @property
    def campaign_ids(self):
        return self._campaign_ids

    @campaign_ids.setter
    def campaign_ids(self, campaign_ids):
        self._campaign_ids = campaign_ids

    @property
    def consider_views(self):
        return self._consider_views

    @consider_views.setter
    def consider_views(self, consider_views):
        self._consider_views = consider_views

    @property
    def extinfo(self):
        return self._extinfo

    @extinfo.setter
    def extinfo(self, extinfo):
        self._extinfo = extinfo

    @property
    def include_dwell_data(self):
        return self._include_dwell_data

    @include_dwell_data.setter
    def include_dwell_data(self, include_dwell_data):
        self._include_dwell_data = include_dwell_data

    @property
    def include_video_data(self):
        return self._include_video_data

    @include_video_data.setter
    def include_video_data(self, include_video_data):
        self._include_video_data = include_video_data

    @property
    def install_referrer(self):
        return self._install_referrer

    @install_referrer.setter
    def install_referrer(self, install_referrer):
        self._install_referrer = install_referrer

    @property
    def installer_package(self):
        return self._installer_package

    @installer_package.setter
    def installer_package(self, installer_package):
        self._installer_package = installer_package

    @property
    def receipt_data(self):
        return self._receipt_data

    @receipt_data.setter
    def receipt_data(self, receipt_data):
        self._receipt_data = receipt_data

    @property
    def url_schemes(self):
        return self._url_schemes

    @url_schemes.setter
    def url_schemes(self, url_schemes):
        self._url_schemes = url_schemes

    @property
    def windows_attribution_id(self):
        return self._windows_attribution_id

    @windows_attribution_id.setter
    def windows_attribution_id(self, windows_attribution_id):
        self._windows_attribution_id = windows_attribution_id

    def normalize(self):
        normalized_payload = {
            'application_tracking_enabled': self.application_tracking_enabled,
            'advertiser_tracking_enabled': self.advertiser_tracking_enabled,
            'campaign_ids': self.campaign_ids,
            'consider_views': self.consider_views,
            'include_dwell_data': self.include_dwell_data,
            'include_video_data': self.include_video_data,
            'install_referrer': self.install_referrer,
            'installer_package': self.installer_package,
            'receipt_data': self.receipt_data,
            'url_schemes': self.url_schemes,
            'windows_attribution_id': self.windows_attribution_id
        }
        if self.extinfo is not None:
            normalized_payload['extinfo'] = self.extinfo.normalize()

        normalized_payload = {k: v for k, v in normalized_payload.items() if v is not None}
        return normalized_payload


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
        if issubclass(AppData, dict):
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
        if not isinstance(other, AppData):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
