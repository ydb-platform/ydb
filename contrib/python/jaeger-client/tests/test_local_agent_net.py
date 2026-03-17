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

import pytest
import tornado.web
from urllib.parse import urlparse
from jaeger_client.local_agent_net import LocalAgentSender
from jaeger_client.config import DEFAULT_REPORTING_PORT

test_strategy = """
    {
        "strategyType":0,
        "probabilisticSampling":
        {
            "samplingRate":0.002
        }
    }
"""

test_credits = """
    {
        \"balances\": [
            {
                \"operation\": \"test-operation\",
                \"balance\": 2.0
            }
        ]
    }
"""

test_client_id = 12345678


class AgentHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(test_strategy)


class CreditHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(test_credits)


application = tornado.web.Application([
    (r'/sampling', AgentHandler),
    (r'/credits', CreditHandler),
])


@pytest.fixture
def app():
    return application


@pytest.mark.gen_test
def test_request_sampling_strategy(http_client, base_url):
    o = urlparse(base_url)
    sender = LocalAgentSender(
        host='localhost',
        sampling_port=o.port,
        reporting_port=DEFAULT_REPORTING_PORT
    )
    response = yield sender.request_sampling_strategy(service_name='svc', timeout=15)
    assert response.body == test_strategy.encode('utf-8')


@pytest.mark.gen_test
def test_request_throttling_credits(http_client, base_url):
    o = urlparse(base_url)
    sender = LocalAgentSender(
        host='localhost',
        sampling_port=o.port,
        reporting_port=DEFAULT_REPORTING_PORT,
        throttling_port=o.port,
    )
    response = yield sender.request_throttling_credits(
        service_name='svc',
        client_id=test_client_id,
        operations=['test-operation'],
        timeout=15)
    assert response.body == test_credits.encode('utf-8')
