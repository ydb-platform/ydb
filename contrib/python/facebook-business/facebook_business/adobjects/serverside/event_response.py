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
import re
import six


class EventResponse(object):
    param_types = {
        'events_received': 'int',
        'messages': 'list[str]',
        'fbtrace_id': 'str'
    }

    def __init__(self, events_received = None, messages = None, fbtrace_id = None):
        # type: (int, List[str], str) -> None

        """Conversions API Event Response"""
        self._events_received = None
        self._messages = None
        self._fbtrace_id = None
        if events_received is not None:
            self.events_received = events_received
        if messages is not None:
            self.messages = messages
        if fbtrace_id is not None:
            self.fbtrace_id = fbtrace_id

    @property
    def events_received(self):
        """Gets the count of events received.


        :return: The count of events received..
        :rtype: int
        """
        return self._events_received

    @events_received.setter
    def events_received(self, events_received):
        """Sets the count of events received.


        :param events_received: The count of events received..
        :type: int
        """

        self._events_received = events_received

    @property
    def messages(self):
        """Gets the messages.


        :return: The messages.
        :rtype: list[str]
        """
        return self._messages

    @messages.setter
    def messages(self, messages):
        """Sets the messages.


        :param messages: The messages.
        :type: list[str]
        """

        self._messages = messages

    @property
    def fbtrace_id(self):
        """Gets the Facebook API trace id.


        :return: The Facebook API trace id.
        :rtype: str
        """
        return self._fbtrace_id

    @fbtrace_id.setter
    def fbtrace_id(self, fbtrace_id):
        """Sets the messages.


        :param fbtrace_id: The Facebook API trace id.
        :type: str
        """

        self._fbtrace_id = fbtrace_id

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
        if issubclass(EventResponse, dict):
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
        if not isinstance(other, EventResponse):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
