# Copyright (c) 2016 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import mock
import pytest
from jaeger_client import ConstSampler, Tracer
from tornado.httpclient import AsyncHTTPClient


@pytest.fixture(scope='function')
def tracer():
    reporter = mock.MagicMock()
    sampler = ConstSampler(True)
    return Tracer(
        service_name='test_service_1', reporter=reporter, sampler=sampler)


AsyncHTTPClient.configure('tornado.curl_httpclient.CurlAsyncHTTPClient')
print('Configured AsyncHTTPClient to use tornado.curl_httpclient.CurlAsyncHTTPClient')
