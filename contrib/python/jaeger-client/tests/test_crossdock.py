# Copyright (c) 2016-2018 Uber Technologies, Inc.
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
import json
import os
import pytest
import opentracing
from mock import MagicMock
from tornado.httpclient import HTTPRequest
from jaeger_client import Tracer, ConstSampler
from jaeger_client.reporter import InMemoryReporter
from crossdock.server.endtoend import EndToEndHandler, _determine_host_port, _parse_host_port

try:
    # The crossdock tests only work with Tornado 4.x
    import tornado
    assert tornado.version_info[0] == 4

    from crossdock.server import server
    from opentracing.scope_managers.tornado import TornadoScopeManager
    scope_manager = TornadoScopeManager
    SKIP_TESTS = False
except BaseException:
    scope_manager = None
    SKIP_TESTS = True

tchannel_port = '9999'


@pytest.fixture
def app():
    """Required by pytest-tornado's http_server fixture"""
    s = server.Server(int(tchannel_port))
    s.tchannel.listen()
    return server.make_app(s, EndToEndHandler())


# noinspection PyShadowingNames
@pytest.fixture
def tracer():
    tracer = Tracer(
        service_name='test-tracer',
        sampler=ConstSampler(True),
        reporter=InMemoryReporter(),
        scope_manager=scope_manager() if scope_manager else None
    )
    try:
        yield tracer
    finally:
        tracer.close()


PERMUTATIONS = []
for s2 in ['HTTP', 'TCHANNEL']:
    for s3 in ['HTTP', 'TCHANNEL']:
        for sampled in [True, False]:
            PERMUTATIONS.append((s2, s3, sampled))


# noinspection PyShadowingNames
@pytest.mark.parametrize('s2_transport,s3_transport,sampled', PERMUTATIONS)
@pytest.mark.gen_test
@pytest.mark.skipif(SKIP_TESTS, reason='crossdock tests only work with Tornado<6')
def test_trace_propagation(
        s2_transport, s3_transport, sampled, tracer,
        base_url, http_port, http_client):

    # verify that server is ready
    yield http_client.fetch(
        request=HTTPRequest(
            url=base_url,
            method='HEAD',
        )
    )

    level3 = dict()
    level3['serviceName'] = 'python'
    level3['serverRole'] = 's3'
    level3['transport'] = s3_transport
    level3['host'] = 'localhost'
    level3['port'] = str(http_port) if s3_transport == 'HTTP' else tchannel_port

    level2 = dict()
    level2['serviceName'] = 'python'
    level2['serverRole'] = 's2'
    level2['transport'] = s2_transport
    level2['host'] = 'localhost'
    level2['port'] = str(http_port) if s2_transport == 'HTTP' else tchannel_port
    level2['downstream'] = level3

    level1 = dict()
    level1['baggage'] = 'Zoidberg'
    level1['serverRole'] = 's1'
    level1['sampled'] = sampled
    level1['downstream'] = level2
    body = json.dumps(level1)

    with mock.patch('opentracing.tracer', tracer):
        assert opentracing.global_tracer() == tracer  # sanity check that patch worked

        req = HTTPRequest(url='%s/start_trace' % base_url, method='POST',
                          headers={'Content-Type': 'application/json'},
                          body=body,
                          request_timeout=2)

        response = yield http_client.fetch(req)
        assert response.code == 200
        tr = server.serializer.traceresponse_from_json(response.body)
        assert tr is not None
        assert tr.span is not None
        assert tr.span.baggage == level1.get('baggage')
        assert tr.span.sampled == sampled
        assert tr.span.traceId is not None
        assert tr.downstream is not None
        assert tr.downstream.span.baggage == level1.get('baggage')
        assert tr.downstream.span.sampled == sampled
        assert tr.downstream.span.traceId == tr.span.traceId
        assert tr.downstream.downstream is not None
        assert tr.downstream.downstream.span.baggage == level1.get('baggage')
        assert tr.downstream.downstream.span.sampled == sampled
        assert tr.downstream.downstream.span.traceId == tr.span.traceId


# noinspection PyShadowingNames
@pytest.mark.gen_test
@pytest.mark.skipif(SKIP_TESTS, reason='crossdock tests only work with Tornado<6')
def test_endtoend_handler(tracer):
    payload = dict()
    payload['operation'] = 'Zoidberg'
    payload['count'] = 2
    payload['tags'] = {'key': 'value'}
    body = json.dumps(payload)

    h = EndToEndHandler()
    request = MagicMock(body=body)
    response_writer = MagicMock()
    response_writer.finish.return_value = None

    h.tracers = {'remote': tracer}
    h.generate_traces(request, response_writer)

    spans = tracer.reporter.get_spans()
    assert len(spans) == 2


def test_determine_host_port():
    original_value = os.environ.get('AGENT_HOST_PORT', None)
    os.environ['AGENT_HOST_PORT'] = 'localhost:1234'
    host, port = _determine_host_port()

    # Before any assertions, restore original environment variable.
    if original_value:
        os.environ['AGENT_HOST_PORT'] = original_value

    assert host == 'localhost'
    assert port == 1234


def test_parse_host_port():
    test_cases = [
        [('', 'localhost', 5678), ('localhost', 5678)],
        [('test:1234', 'localhost', 5678), ('test', 1234)],
    ]
    for test_case in test_cases:
        args, result = test_case
        host, port = _parse_host_port(*args)
        assert (host, port) == result
