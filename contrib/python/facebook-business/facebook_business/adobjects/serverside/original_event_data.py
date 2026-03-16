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


class OriginalEventData(object):
    param_types = {
        'event_name': 'str',
        'event_time': 'int',
        'order_id': 'str',
        'event_id': 'str'
    }

    def __init__(self, event_name = None, event_time = None, order_id = None, event_id = None):
        # type: (str, int, str, str) -> None

        """Conversions API Original Event Data"""
        self._event_name = None
        self._event_time = None
        self._order_id = None
        self._event_id = None

        if event_name is not None:
            self.event_name = event_name
        if event_time is not None:
            self.event_time = event_time
        if order_id is not None:
            self.order_id = order_id
        if event_id is not None:
            self.event_id = event_id

    @property
    def event_name(self):
        """Gets the event_name of original Event.

        A Meta pixel Standard Event or Custom Event name.

        :return: The event_name of original Event.
        :rtype: str
        """
        return self._event_name

    @event_name.setter
    def event_name(self, event_name):
        """Sets the event_name of original Event.

        A Meta pixel Standard Event or Custom Event name.

        :param event_name: The event_name of original Event.
        :type: str
        :return self
        """
        self._event_name = event_name

    @property
    def event_time(self):
        """Gets the event_time of original Event.

        A Unix timestamp in seconds indicating when the original event occurred.

        :return: The event_time of original Event.
        :rtype: int
        """
        return self._event_time

    @event_time.setter
    def event_time(self, event_time):
        """Sets the event_time of this Event.

        A Unix timestamp in seconds indicating when the actual event occurred.

        :param event_time: The event_time of this Event.
        :type: int
        """
        if not isinstance(event_time, int):
            raise TypeError('OriginalEventData.event_time must be an int')

        self._event_time = event_time

    @property
    def order_id(self):
        """Gets the order_id of original Event.

        The order ID for this transaction as a string.

        :return: The order_id of original Event.
        :rtype: str
        """
        return self._order_id

    @order_id.setter
    def order_id(self, order_id):
        """Sets the order_id of original Event.

        The order ID for this transaction as a string.

        :param order_id: The order_id of original Event.
        :type: str
        :return self
        """
        self._order_id = order_id

    @property
    def event_id(self):
        """Gets the event_id of original Event.

        A unique string chosen by the advertiser.

        :return: The event_id of original Event.
        :rtype: str
        """
        return self._event_id

    @event_id.setter
    def event_id(self, event_id):
        """Sets the event_id of original Event.

        A unique string chosen by the advertiser.

        :param event_id: The event_id of original Event.
        :type: str
        :return self
        """
        self._event_id = event_id

    def normalize(self):
        normalized_payload = {
            'event_name': self.event_name,
            'event_time': self.event_time,
            'order_id': self.order_id,
            'event_id': self.event_id,
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
        if issubclass(OriginalEventData, dict):
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
        if not isinstance(other, OriginalEventData):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
