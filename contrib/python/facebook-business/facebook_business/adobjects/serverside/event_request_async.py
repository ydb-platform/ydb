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

from facebook_business.adobjects.serverside.util import Util
if not Util.async_requests_available():
    raise Exception('EventRequestAsync requires Python >= 3.5.3')

from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.event_response import EventResponse
from facebook_business.api import FacebookAdsApi
from facebook_business.session import FacebookSession

import facebook_business.certs

import aiohttp
import json
import ssl


class EventRequestAsync(EventRequest):
    def __init__(
        self,
        pixel_id=None,
        events=None,
        test_event_code=None,
        namespace_id=None,
        upload_id=None,
        upload_tag=None,
        upload_source=None,
        partner_agent=None
    ):
        super().__init__(
            pixel_id,
            events,
            test_event_code,
            namespace_id,
            upload_id,
            upload_tag,
            upload_source,
            partner_agent
        )
        self.pixel_id = pixel_id

    async def create_event(self, session):
        api_version = FacebookAdsApi.get_default_api().API_VERSION
        url = '/'.join([
            FacebookSession.GRAPH,
            api_version,
            self.pixel_id,
            'events'
        ])

        facebook_session = FacebookAdsApi.get_default_api()._session
        access_token = facebook_session.access_token
        params = self.get_params()
        params['access_token'] = access_token
        if facebook_session.app_secret:
            params['appsecret_proof'] = facebook_session._gen_appsecret_proof()
        cafile = facebook_business.certs.cert_manager.get_cert_file()
        sslcontext = ssl.create_default_context(cafile=cafile)

        async with session.post(
            url=url,
            data=params,
            ssl=sslcontext,
            headers=FacebookAdsApi.get_default_api().HTTP_DEFAULT_HEADERS
        ) as response:
            return await response.json()

    async def execute(self):
        async with aiohttp.ClientSession() as session:
            response = await self.create_event(session)
            if response.get('events_received'):
                return EventResponse(events_received=response['events_received'],
                                           fbtrace_id=response['fbtrace_id'],
                                           messages=response['messages'])
            else: 
                return EventResponse(events_received=0,
                                           fbtrace_id=response['fbtrace_id'],
                                           messages=response['messages']) 

    def normalize(self):
        normalized_events = []
        for event in self.events:
            normalized_event = event.normalize()
            normalized_events.append(normalized_event)

        return json.dumps(normalized_events)

    def clone_without_events(self):
        return EventRequestAsync(
            pixel_id=self.pixel_id,
            events=[],
            test_event_code=self.test_event_code,
            namespace_id=self.namespace_id,
            upload_id=self.upload_id,
            upload_tag=self.upload_tag,
            upload_source=self.upload_source,
        )
