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
    raise Exception('BatchProcessor requires Python >= 3.5.3')

import asyncio

class BatchProcessor:
    def __init__(self, batch_size=50, concurrent_requests=4):
        self.batch_size = batch_size
        self.concurrent_requests = concurrent_requests

    def process_event_requests(self, event_requests_async):
        async def process():
            async for _ in self.process_event_requests_generator(event_requests_async):
                pass

        asyncio.run(process())

    async def process_event_requests_generator(self, event_requests_async):
        index = 0
        while index < len(event_requests_async):
            batch = event_requests_async[index:(index + self.concurrent_requests)]
            responses = []
            for request in batch:
                response = await request.execute()
                responses.append(response)
            yield responses

            index += self.concurrent_requests

    def process_events(self, event_request_async_to_clone, events):
        async def process():
            async for _ in self.process_events_generator(event_request_async_to_clone, events):
                pass

        asyncio.run(process())

    async def process_events_generator(self, event_request_async_to_clone, events):
        index = 0
        while index < len(events):
            responses = []
            while index < len(events) and len(responses) < self.concurrent_requests:
                event_request = event_request_async_to_clone.clone_without_events()
                event_request.events = events[index:(index + self.batch_size)]
                response = await event_request.execute()
                responses.append(response)
                index += self.batch_size

            yield responses
