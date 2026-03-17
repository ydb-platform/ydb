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

import json
import pprint
import six

from facebook_business import FacebookAdsApi
from facebook_business.adobjects.adspixel import AdsPixel
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_response import EventResponse
from facebook_business.adobjects.serverside.http_method import HttpMethod
from facebook_business.adobjects.serverside.request_options import RequestOptions
from facebook_business.adobjects.serverside.util import Util
from facebook_business.session import FacebookSession


class EventRequest(object):
    """
    Conversions API Event Request.
    """

    param_types = {
        'events': 'list[Event]',
        'test_event_code': 'str',
        'namespace_id': 'str',
        'upload_id': 'str',
        'upload_tag': 'str',
        'upload_source': 'str',
        'partner_agent': 'str',
    }

    def __init__(
        self,
        pixel_id=None,
        events=None,
        test_event_code=None,
        namespace_id=None,
        upload_id=None,
        upload_tag=None,
        upload_source=None,
        partner_agent=None,
        http_client=None,
        access_token=None,
        appsecret=None,
    ):
        # type: (str, List[Event], str) -> None

        self._events = None
        self._test_event_code = None
        self._namespace_id = None
        self._upload_id = None
        self._upload_tag = None
        self._upload_source = None
        self._partner_agent = None
        self.__pixel_id = None
        self.__http_client = None
        self.__access_token = None
        self.__appsecret = None
        if pixel_id is None:
            raise ValueError("Invalid value for `pixel_id`, must not be `None`")
        self.__pixel_id = pixel_id
        if http_client is not None and access_token is None:
            raise ValueError("An access_token must also be passed in when passing in an http_client")
        if http_client is not None:
            self.__http_client = http_client
        if access_token is not None:
            self.__access_token = access_token
        if appsecret is not None:
            self.__appsecret = appsecret
        self.events = events
        if test_event_code is not None:
            self.test_event_code = test_event_code
        if namespace_id is not None:
            self.namespace_id = namespace_id
        if upload_id is not None:
            self.upload_id = upload_id
        if upload_tag is not None:
            self.upload_tag = upload_tag
        if upload_source is not None:
            self.upload_source = upload_source
        if partner_agent is not None:
            self.partner_agent = partner_agent

    @property
    def events(self):
        """Gets the events.

        An array of Server Event objects

        :return: The events.
        :rtype: list[Event]
        """
        return self._events

    @events.setter
    def events(self, events):
        """Sets the events.

        An array of Server Event objects

        :param events: The events.
        :type: list[Event]
        """
        if events is None:
            raise ValueError("Invalid value for `events`, must not be `None`")

        self._events = events

    @property
    def test_event_code(self):
        """Gets the test_event_code.

        Code used to verify that your server events are received correctly by Facebook.
        Use this code to test your server events in the Test Events feature in Events Manager.
        See Test Events Tool (https://developers.facebook.com/docs/marketing-api/conversions-api/using-the-api#testEvents) for an example.

        :return: The test_event_code.
        :rtype: str
        """
        return self._test_event_code

    @test_event_code.setter
    def test_event_code(self, test_event_code):
        """Sets the test_event_code.

        Code used to verify that your server events are received correctly by Facebook.
        Use this code to test your server events in the Test Events feature in Events Manager.
        See Test Events Tool (https://developers.facebook.com/docs/marketing-api/conversions-api/using-the-api#testEvents) for an example.

        :param test_event_code: The test_event_code.
        :type: str
        """

        self._test_event_code = test_event_code

    @property
    def namespace_id(self):
        """Gets the namespace_id.

        :return: The namespace_id.
        :rtype: str
        """
        return self._namespace_id

    @namespace_id.setter
    def namespace_id(self, namespace_id):
        """Sets the namespace_id.

        :param namespace_id: The namespace_id.
        :type: str
        """

        self._namespace_id = namespace_id

    @property
    def upload_id(self):
        """Gets the upload_id.

        :return: The upload_id.
        :rtype: str
        """
        return self._upload_id

    @upload_id.setter
    def upload_id(self, upload_id):
        """Sets the upload_id.

        :param upload_id: The upload_id.
        :type: str
        """

        self._upload_id = upload_id

    @property
    def upload_tag(self):
        """Gets the upload_tag.

        :return: The upload_tag.
        :rtype: str
        """
        return self._upload_tag

    @upload_tag.setter
    def upload_tag(self, upload_tag):
        """Sets the upload_tag.

        :param upload_tag: The upload_tag.
        :type: str
        """

        self._upload_tag = upload_tag

    @property
    def upload_source(self):
        """Gets the upload_source.

        :return: The upload_source.
        :rtype: str
        """
        return self._upload_source

    @upload_source.setter
    def upload_source(self, upload_source):
        """Sets the upload_source.

        :param upload_source: The upload_source.
        :type: str
        """

        self._upload_source = upload_source

    @property
    def partner_agent(self):
        """Gets the partner_agent.

        Allows you to specify the platform from which the event is sent e.g. wordpress

        :return: The partner_agent.
        :rtype: str
        """
        return self._partner_agent

    @partner_agent.setter
    def partner_agent(self, partner_agent):
        """Sets the partner_agent.

        Allows you to specify the platform from which the event is sent e.g. wordpress

        :param partner_agent: The partner_agent.
        :type: str
        """

        self._partner_agent = partner_agent

    def get_request_params(self):
        params = {}
        if self.test_event_code is not None:
            params['test_event_code'] = self.test_event_code
        if self.namespace_id is not None:
            params['namespace_id'] = self.namespace_id
        if self.upload_id is not None:
            params['upload_id'] = self.upload_id
        if self.upload_tag is not None:
            params['upload_tag'] = self.upload_tag
        if self.upload_source is not None:
            params['upload_source'] = self.upload_source
        if self.partner_agent is not None:
            params['partner_agent'] = self.partner_agent

        return params

    def get_params(self):
        params = self.get_request_params()
        params["data"] = self.normalize()
        return params

    def execute(self):
        params = self.get_params()

        if self.__http_client is not None:
            return self.execute_with_client(params)

        response = AdsPixel(self.__pixel_id).create_event(
            fields=[],
            params=params,
        )
        if response.get('events_received'):
            return EventResponse(events_received=response['events_received'],
                                       fbtrace_id=response['fbtrace_id'],
                                       messages=response['messages'])
        else: 
            return EventResponse(events_received=0,
                                       fbtrace_id=response['fbtrace_id'],
                                       messages=response['messages']) 

    def execute_with_client(self, params):
        url = '/'.join([
            FacebookSession.GRAPH,
            FacebookAdsApi.API_VERSION,
            self.__pixel_id,
            'events',
        ])
        params['access_token'] = self.__access_token
        if self.__appsecret is not None:
            params['appsecret_proof'] = Util.appsecret_proof(self.__appsecret, self.__access_token)
        request_options = RequestOptions(
            ca_bundle_path=Util.ca_bundle_path()
        )

        return self.__http_client.execute(
            url=url,
            method=HttpMethod.POST,
            request_options=request_options,
            headers=FacebookAdsApi.HTTP_DEFAULT_HEADERS,
            params=params
        )

    def normalize(self):
        normalized_events = []
        for event in self.events:
            normalized_event = event.normalize()
            normalized_events.append(json.dumps(normalized_event))

        return normalized_events

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
        if issubclass(EventRequest, dict):
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
        if not isinstance(other, EventRequest):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
