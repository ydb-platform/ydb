# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import uuid

import requests
import requests_mock
from . import base


class RequestTests(base.TestCase):

    def setUp(self):
        super(RequestTests, self).setUp()

        self.mocker = requests_mock.Mocker()
        self.addCleanup(self.mocker.stop)
        self.mocker.start()

    def do_request(self, **kwargs):
        method = kwargs.pop('method', 'GET')
        url = kwargs.pop('url', 'http://test.example.com/path')
        status_code = kwargs.pop('status_code', 200)
        data = uuid.uuid4().hex

        m = self.mocker.register_uri(method,
                                     url,
                                     text=data,
                                     status_code=status_code)

        resp = requests.request(method, url, **kwargs)

        self.assertEqual(status_code, resp.status_code)
        self.assertEqual(data, resp.text)

        self.assertTrue(m.called_once)
        return m.last_request

    def test_base_params(self):
        req = self.do_request(method='GET', status_code=200)

        self.assertIs(None, req.allow_redirects)
        self.assertIs(None, req.timeout)
        self.assertIs(True, req.verify)
        self.assertIs(None, req.cert)
        self.assertIs(False, req.stream)

        # actually it's an OrderedDict, but equality works fine
        # Skipping this check - it's problematic based on people's environments
        # and in CI systems where there are proxies set up at the environment
        # level. gh #127
        # self.assertEqual({}, req.proxies)

    def test_allow_redirects(self):
        req = self.do_request(allow_redirects=False, status_code=300)
        self.assertFalse(req.allow_redirects)

    def test_timeout(self):
        timeout = 300
        req = self.do_request(timeout=timeout)
        self.assertEqual(timeout, req.timeout)

    def test_verify_false(self):
        verify = False
        req = self.do_request(verify=verify)
        self.assertIs(verify, req.verify)

    def test_verify_path(self):
        verify = '/path/to/cacerts.pem'
        req = self.do_request(verify=verify)
        self.assertEqual(verify, req.verify)

    def test_stream(self):
        req = self.do_request()
        self.assertIs(False, req.stream)
        req = self.do_request(stream=False)
        self.assertIs(False, req.stream)
        req = self.do_request(stream=True)
        self.assertIs(True, req.stream)

    def test_certs(self):
        cert = ('/path/to/cert.pem', 'path/to/key.pem')
        req = self.do_request(cert=cert)
        self.assertEqual(cert, req.cert)
        self.assertTrue(req.verify)

    def test_proxies(self):
        proxies = {'http': 'foo.bar:3128',
                   'http://host.name': 'foo.bar:4012'}

        req = self.do_request(proxies=proxies)

        self.assertEqual(proxies, req.proxies)
        self.assertIsNot(proxies, req.proxies)

    def test_hostname_port_http(self):
        req = self.do_request(url='http://host.example.com:81/path')

        self.assertEqual('host.example.com:81', req.netloc)
        self.assertEqual('host.example.com', req.hostname)
        self.assertEqual(81, req.port)

    def test_hostname_port_https(self):
        req = self.do_request(url='https://host.example.com:8080/path')

        self.assertEqual('host.example.com:8080', req.netloc)
        self.assertEqual('host.example.com', req.hostname)
        self.assertEqual(8080, req.port)

    def test_hostname_default_port_http(self):
        req = self.do_request(url='http://host.example.com/path')

        self.assertEqual('host.example.com', req.netloc)
        self.assertEqual('host.example.com', req.hostname)
        self.assertEqual(80, req.port)

    def test_hostname_default_port_https(self):
        req = self.do_request(url='https://host.example.com/path')

        self.assertEqual('host.example.com', req.netloc)
        self.assertEqual('host.example.com', req.hostname)
        self.assertEqual(443, req.port)

    def test_to_string(self):
        req = self.do_request(url='https://host.example.com/path')
        self.assertEqual('GET https://host.example.com/path', str(req))

    def test_empty_query_string(self):
        req = self.do_request(url='https://host.example.com/path?key')
        self.assertEqual([''], req.qs['key'])
