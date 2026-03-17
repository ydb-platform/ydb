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

import pprint
import six

from facebook_business.adobjects.serverside.attribution_model import AttributionModel
from facebook_business.adobjects.serverside.attribution_method import AttributionMethod
from facebook_business.adobjects.serverside.decline_reason import DeclineReason
from facebook_business.adobjects.serverside.attribution_setting import AttributionSetting

class AttributionData(object):
    param_types = {
        'scope': 'str',
        'visit_time': 'int',
        'ad_id': 'str',
        'adset_id': 'str',
        'campaign_id': 'str',
        'attribution_share': 'float',
        'attribution_model': 'AttributionModel',
        'attr_window': 'int',
        'attribution_value': 'float',
        'attribution_source': 'str', 
        'touchpoint_type': 'str', 
        'touchpoint_ts': 'int',
        'attribution_method': 'AttributionMethod',
        'decline_reason': 'DeclineReason',
        'auditing_token': 'str',
        'linkage_key': 'str',
        'attribution_setting': 'AttributionSetting',
    }

    def __init__(self, scope = None, visit_time = None, ad_id = None, adset_id = None, campaign_id = None, attribution_share = None, attribution_model = None, attr_window = None, attribution_value = None, attribution_source = None, touchpoint_type = None, touchpoint_ts = None, attribution_method = None, decline_reason = None, auditing_token = None, linkage_key = None, attribution_setting = None):

        """Conversions API Attribution Data"""
        self._scope = None
        self._visit_time = None
        self._ad_id = None
        self._adset_id = None
        self._campaign_id = None
        self._attribution_share = None
        self._attribution_model = None
        self._attr_window = None
        self._attribution_value = None
        self._attribution_source = None
        self._touchpoint_type = None
        self._touchpoint_ts = None
        self._attribution_method = None
        self._decline_reason = None
        self._auditing_token = None
        self._linkage_key = None
        self._attribution_setting = None

        if scope is not None:
            self.scope = scope
        if visit_time is not None:
            self.visit_time = visit_time
        if ad_id is not None:
            self.ad_id = ad_id
        if adset_id is not None:
            self.adset_id = adset_id
        if campaign_id is not None:
            self.campaign_id = campaign_id
        if attribution_share is not None:
            self.attribution_share = attribution_share
        if attribution_model is not None:
            self.attribution_model = attribution_model
        if attr_window is not None:
            self.attr_window = attr_window
        if attribution_value is not None:
            self.attribution_value = attribution_value
        if attribution_source is not None:
            self.attribution_source = attribution_source
        if touchpoint_type is not None:
            self.touchpoint_type = touchpoint_type
        if touchpoint_ts is not None:
            self.touchpoint_ts = touchpoint_ts
        if attribution_method is not None:
            self.attribution_method = attribution_method
        if decline_reason is not None:
            self.decline_reason = decline_reason
        if auditing_token is not None:
            self.auditing_token = auditing_token
        if linkage_key is not None:
            self.linkage_key = linkage_key
        if attribution_setting is not None:
            self.attribution_setting = attribution_setting

    @property
    def scope(self):
        """Gets the scope of Attribution Data.

        Touchpoint type.

        :return: The scope of Attribution Data.
        :rtype: str
        """
        return self._scope

    @scope.setter
    def scope(self, scope):
        """Sets the scope of Attribution Data.

        Touchpoint type.

        :param scope: The scope of Attribution Data.
        :type: str
        """
        self._scope = scope

    @property
    def visit_time(self):
        """Gets the visit_time of Attribution Data.

        A Unix timestamp in seconds indicating time that the campaign_id or fbc was first received.

        :return: The visit_time of Attribution Data.
        :rtype: int
        """
        return self._visit_time

    @visit_time.setter
    def visit_time(self, visit_time):
        """Sets the visit_time of Attribution Data.

        A Unix timestamp in seconds indicating time that the campaign_id or fbc was first received.

        :param visit_time: The visit_time of Attribution Data.
        :type: int
        """
        if not isinstance(visit_time, int):
            raise TypeError('AttributionData.visit_time must be an int')

        self._visit_time = visit_time
        
    @property
    def ad_id(self):
        """Gets the ad_id of Attribution Data.

        Meta-provided ad id from URL/deeplink.

        :return: The ad_id of Attribution Data.
        :rtype: str
        """
        return self._ad_id

    @ad_id.setter
    def ad_id(self, ad_id):
        """Sets the ad_id of Attribution Data.

        Meta-provided ad id from URL/deeplink.

        :param ad_id: The ad_id of Attribution Data.
        :type: str
        """
        self._ad_id = ad_id
    
    @property
    def adset_id(self):
        """Gets the adset_id of Attribution Data.

        Meta-provided adset id from URL/deeplink.

        :return: The adset_id of Attribution Data.
        :rtype: str
        """
        return self._adset_id

    @adset_id.setter
    def adset_id(self, adset_id):
        """Sets the adset_id of Attribution Data.

        Meta-provided adset id from URL/deeplink.

        :param adset_id: The adset_id of Attribution Data.
        :type: str
        """
        self._adset_id = adset_id
    
    @property
    def campaign_id(self):
        """Gets the campaign_id of Attribution Data.

        Meta-provided campaign id from URL/deeplink.

        :return: The campaign_id of Attribution Data.
        :rtype: str
        """
        return self._campaign_id

    @campaign_id.setter
    def campaign_id(self, campaign_id):
        """Sets the campaign_id of Attribution Data.

        Meta-provided campaign id from URL/deeplink.

        :param campaign_id: The campaign_id of Attribution Data.
        :type: str
        """
        self._campaign_id = campaign_id
    
    @property
    def attribution_share(self):
        """Gets the attribution_share of Attribution Data.

        [0-1] weight of credit assigned to the visit.

        :return: The attribution_share of Attribution Data.
        :rtype: float
        """
        return self._attribution_share

    @attribution_share.setter
    def attribution_share(self, attribution_share):
        """Sets the attribution_share of Attribution Data.

        [0-1] weight of credit assigned to the visit.

        :param attribution_share: The attribution_share of Attribution Data.
        :type: float
        """
        self._attribution_share = attribution_share

    @property
    def attribution_model(self):
        """Gets the attribution_model of Attribution Data.

        Attribution model used to attribute the event.

        :return: The attribution_model of Attribution Data.
        :rtype: AttributionModel
        """
        return self._attribution_model

    @attribution_model.setter
    def attribution_model(self, attribution_model):
        """Sets the attribution_model of Attribution Data.

        Attribution model used to attribute the event.

        :param attribution_model: The attribution_model of Attribution Data.
        :type: AttributionModel
        """
        self._attribution_model = attribution_model
    
    @property
    def attr_window(self):
        """Gets the attr_window of Attribution Data.

        Attribution window in days.

        :return: The attr_window of Attribution Data.
        :rtype: int
        """
        return self._attr_window

    @attr_window.setter
    def attr_window(self, attr_window):
        """Sets the attr_window of Attribution Data.

        Attribution window in days.

        :param attr_window: The attr_window of Attribution Data.
        :type: int
        """
        self._attr_window = attr_window

    @property
    def attribution_value(self):
        """Gets the attribution_value of Attribution Data.

        The share of value generated by this click-conversion pair that is attributed to Meta.

        :return: The attribution_value of Attribution Data.
        :rtype: float
        """
        return self._attribution_value

    @attribution_value.setter
    def attribution_value(self, attribution_value):
        """Sets the attribution_value of Attribution Data.

        The share of value generated by this click-conversion pair that is attributed to Meta.

        :param attribution_value: The attribution_value of Attribution Data.
        :type: float
        """
        self._attribution_value = attribution_value

    @property
    def attribution_source(self):
        """Gets the attribution_source of Attribution Data.

        The attribution source To differentiate the source of the data, e.g. whether this is from AMM or Custom Attribution or any other sources.

        :return: The attribution_source of Attribution Data.
        :rtype: float
        """
        return self._attribution_source

    @attribution_source.setter
    def attribution_source(self, attribution_source):
        """Sets the attribution_source of Attribution Data.

        The attribution source To differentiate the source of the data, e.g. whether this is from AMM or Custom Attribution or any other sources.

        :param attribution_source: The attribution_source of Attribution Data.
        :type: float
        """
        self._attribution_source = attribution_source

    @property
    def touchpoint_type(self):
        """Gets the touchpoint_type of Attribution Data.

        The engagement type that caused the original credited conversion.

        :return: The touchpoint_type of Attribution Data.
        :rtype: float
        """
        return self._touchpoint_type

    @touchpoint_type.setter
    def touchpoint_type(self, touchpoint_type):
        """Sets the touchpoint_type of Attribution Data.

        The engagement type that caused the original credited conversion.

        :param touchpoint_type: The touchpoint_type of Attribution Data.
        :type: float
        """
        self._touchpoint_type = touchpoint_type

    @property
    def touchpoint_ts(self):
        """Gets the touchpoint_ts of Attribution Data.

        The time when the touchpoint event occurred with the ad that the install was credited to.

        :return: The touchpoint_ts of Attribution Data.
        :rtype: float
        """
        return self._touchpoint_ts

    @touchpoint_ts.setter
    def touchpoint_ts(self, touchpoint_ts):
        """Sets the touchpoint_ts of Attribution Data.

        The time when the touchpoint event occurred with the ad that the install was credited to.

        :param touchpoint_ts: The touchpoint_ts of Attribution Data.
        :type: float
        """
        self._touchpoint_ts = touchpoint_ts

    @property
    def attribution_method(self):
        """Gets the attribution_method of Attribution Data.

        The attribution method used to attribute the event.

        :return: The attribution_method of Attribution Data.
        :rtype: AttributionMethod
        """
        return self._attribution_method

    @attribution_method.setter
    def attribution_method(self, attribution_method):
        """Sets the attribution_method of Attribution Data.

        The attribution method used to attribute the event.

        :param attribution_method: The attribution_method of Attribution Data.
        :type: AttributionMethod
        """
        if attribution_method is not None and not isinstance(attribution_method, AttributionMethod):
            raise TypeError('AttributionData.attribution_method must be a AttributionMethod')
        
        self._attribution_method = attribution_method

    @property
    def decline_reason(self):
        """Gets the decline_reason of Attribution Data.

        The decline reason for the attribution.

        :return: The decline_reason of Attribution Data.
        :rtype: DeclineReason
        """
        return self._decline_reason

    @decline_reason.setter
    def decline_reason(self, decline_reason):
        """Sets the decline_reason of Attribution Data.

        The decline reason for the attribution.

        :param decline_reason: The decline_reason of Attribution Data.
        :type: DeclineReason
        """
        if decline_reason is not None and not isinstance(decline_reason, DeclineReason):
            raise TypeError('AttributionData.decline_reason must be a DeclineReason')
        
        self._decline_reason = decline_reason

    @property
    def auditing_token(self):
        """Gets the auditing_token of Attribution Data.

        The auditing token for the attribution.

        :return: The auditing_token of Attribution Data.
        :rtype: str
        """
        return self._auditing_token

    @auditing_token.setter
    def auditing_token(self, auditing_token):
        """Sets the auditing_token of Attribution Data.

        The auditing token for the attribution.

        :param auditing_token: The auditing_token of Attribution Data.
        :type: str
        """
        self._auditing_token = auditing_token

    @property
    def linkage_key(self):
        """Gets the linkage_key of Attribution Data.

        The linkage key for the attribution.

        :return: The linkage_key of Attribution Data.
        :rtype: str
        """
        return self._linkage_key

    @linkage_key.setter
    def linkage_key(self, linkage_key):
        """Sets the linkage_key of Attribution Data.

        The linkage key for the attribution.

        :param linkage_key: The linkage_key of Attribution Data.
        :type: str
        """
        self._linkage_key = linkage_key

    @property
    def attribution_setting(self):
        """Gets the attribution_setting of Attribution Data.

        Attribution settings including inactivity and reattribution windows.

        :return: The attribution_setting of Attribution Data.
        :rtype: AttributionSetting
        """
        return self._attribution_setting

    @attribution_setting.setter
    def attribution_setting(self, attribution_setting):
        """Sets the attribution_setting of Attribution Data.

        Attribution settings including inactivity and reattribution windows.

        :param attribution_setting: The attribution_setting of Attribution Data.
        :type: AttributionSetting
        """
        if attribution_setting is not None and not isinstance(attribution_setting, AttributionSetting):
            raise TypeError('AttributionData.attribution_setting must be an AttributionSetting')
        
        self._attribution_setting = attribution_setting


    def normalize(self):
        normalized_payload = {
            'scope': self.scope,
            'visit_time': self.visit_time,
            'ad_id': self.ad_id,
            'adset_id': self.adset_id,
            'campaign_id': self.campaign_id,
            'attribution_share': self.attribution_share,
            'attribution_model': self.attribution_model,
            'attr_window': self.attr_window,
            'attribution_value': self.attribution_value,
            'attribution_source': self.attribution_source,
            'touchpoint_type': self.touchpoint_type,
            'touchpoint_ts': self.touchpoint_ts,
            'attribution_method': self.attribution_method,
            'decline_reason': self.decline_reason,
            'auditing_token': self.auditing_token,
            'linkage_key': self.linkage_key,
            'attribution_setting': self.attribution_setting.normalize() if self.attribution_setting else None,
        }
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
        if issubclass(AttributionData, dict):
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
        if not isinstance(other, AttributionData):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
