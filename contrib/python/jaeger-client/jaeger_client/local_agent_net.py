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


from threadloop import ThreadLoop
import tornado
import tornado.httpclient
from tornado.concurrent import Future
from tornado.httputil import url_concat
from .TUDPTransport import TUDPTransport
from thrift.transport.TTransport import TBufferedTransport


class LocalAgentHTTP(object):

    DEFAULT_TIMEOUT = 15

    def __init__(self, host, port):
        self.agent_http_host = host
        self.agent_http_port = int(port)

    def _request(self, path, timeout=DEFAULT_TIMEOUT, args=None):
        http_client = tornado.httpclient.AsyncHTTPClient(
            defaults=dict(request_timeout=timeout))
        url = 'http://%s:%d/%s' % (self.agent_http_host, self.agent_http_port, path)
        if args:
            url = url_concat(url, args)
        return http_client.fetch(url)

    def request_sampling_strategy(self, service_name, timeout=DEFAULT_TIMEOUT):
        return self._request('sampling', timeout=timeout, args={'service': service_name})

    def request_throttling_credits(self,
                                   service_name,
                                   client_id,
                                   operations,
                                   timeout=DEFAULT_TIMEOUT):
        return self._request('credits', timeout=timeout, args=[
            ('service', service_name),
            ('uuid', client_id),
        ] + [('operations', op) for op in operations])


class LocalAgentSender(TBufferedTransport):
    """
    LocalAgentSender implements everything necessary to communicate with
    local jaeger-agent. This class is designed to work in tornado and
    non-tornado environments. If in torndado, pass in the ioloop, if not
    then LocalAgentSender will create one for itself.

    NOTE: LocalAgentSender derives from TBufferedTransport. This will buffer
    up all written data until flush() is called. Flush gets called at the
    end of the batch span submission call.
    """

    def __init__(self, host, sampling_port, reporting_port, io_loop=None, throttling_port=None):
        # IOLoop
        self._thread_loop = None
        self.io_loop = io_loop or self._create_new_thread_loop()

        # HTTP sampling
        self.local_agent_http = LocalAgentHTTP(host, sampling_port)

        # HTTP throttling
        if throttling_port:
            self.throttling_http = LocalAgentHTTP(host, throttling_port)

        # UDP reporting - this will only get written to after our flush() call.
        # We are buffering things up because we are a TBufferedTransport.
        udp = TUDPTransport(host, reporting_port)
        TBufferedTransport.__init__(self, udp)

    def _create_new_thread_loop(self):
        """
        Create a daemonized thread that will run Tornado IOLoop.
        :return: the IOLoop backed by the new thread.
        """
        self._thread_loop = ThreadLoop()
        if not self._thread_loop.is_ready():
            self._thread_loop.start()
        return self._thread_loop._io_loop

    def readFrame(self):
        """Empty read frame that is never ready"""
        return Future()

    # Pass-through for HTTP sampling strategies request.
    def request_sampling_strategy(self, *args, **kwargs):
        return self.local_agent_http.request_sampling_strategy(*args, **kwargs)

    # Pass-through for HTTP throttling credit request.
    def request_throttling_credits(self, *args, **kwargs):
        return self.throttling_http.request_throttling_credits(*args, **kwargs)
