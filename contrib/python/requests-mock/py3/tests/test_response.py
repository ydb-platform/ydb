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

import io
import pickle

from requests_mock import exceptions
from requests_mock import request
from requests_mock import response
from . import base


class ResponseTests(base.TestCase):

    def setUp(self):
        super(ResponseTests, self).setUp()
        self.method = 'GET'
        self.url = 'http://test.url/path'
        self.request = request._RequestObjectProxy._create(self.method,
                                                           self.url,
                                                           {})

    def create_response(self, **kwargs):
        return response.create_response(self.request, **kwargs)

    def test_create_response_body_args(self):
        self.assertRaises(RuntimeError,
                          self.create_response,
                          raw='abc',
                          body='abc')

        self.assertRaises(RuntimeError,
                          self.create_response,
                          text='abc',
                          json={'a': 1})

    def test_content_type(self):
        self.assertRaises(TypeError, self.create_response, text=55)
        self.assertRaises(TypeError, self.create_response, text={'a': 1})
        self.assertRaises(TypeError, self.create_response, text=b'')

    def test_text_type(self):
        self.assertRaises(TypeError, self.create_response, content=u't')
        self.assertRaises(TypeError, self.create_response, content={'a': 1})
        self.assertRaises(TypeError, self.create_response, content=u'')

    def test_json_body(self):
        data = {'a': 1}
        resp = self.create_response(json=data)

        self.assertEqual('{"a": 1}', resp.text)
        self.assertIsInstance(resp.text, str)
        self.assertIsInstance(resp.content, bytes)
        self.assertEqual(data, resp.json())

    def test_body_body(self):
        value = b'data'
        body = io.BytesIO(value)
        resp = self.create_response(body=body)

        self.assertEqual(value.decode(), resp.text)
        self.assertIsInstance(resp.text, str)
        self.assertIsInstance(resp.content, bytes)

    def test_setting_connection(self):
        conn = object()
        resp = self.create_response(connection=conn)
        self.assertIs(conn, resp.connection)

    def test_send_from_no_connection(self):
        resp = self.create_response()
        self.assertRaises(exceptions.InvalidRequest,
                          resp.connection.send, self.request)

    def test_cookies_from_header(self):
        # domain must be same as request url to pass policy check
        headers = {'Set-Cookie': 'fig=newton; Path=/test; domain=.test.url'}
        resp = self.create_response(headers=headers)

        self.assertEqual('newton', resp.cookies['fig'])
        self.assertEqual(['/test'], resp.cookies.list_paths())
        self.assertEqual(['.test.url'], resp.cookies.list_domains())

    def test_cookies_from_dict(self):
        # This is a syntax we get from requests. I'm not sure i like it.
        resp = self.create_response(cookies={'fig': 'newton',
                                             'sugar': 'apple'})

        self.assertEqual('newton', resp.cookies['fig'])
        self.assertEqual('apple', resp.cookies['sugar'])

    def test_cookies_with_jar(self):
        jar = response.CookieJar()
        jar.set('fig', 'newton', path='/foo', domain='.test.url')
        jar.set('sugar', 'apple', path='/bar', domain='.test.url')
        resp = self.create_response(cookies=jar)

        self.assertEqual('newton', resp.cookies['fig'])
        self.assertEqual('apple', resp.cookies['sugar'])
        self.assertEqual({'/foo', '/bar'}, set(resp.cookies.list_paths()))
        self.assertEqual(['.test.url'], resp.cookies.list_domains())

    def test_response_pickle(self):
        text = 'hello world'
        jar = response.CookieJar()
        jar.set('fig', 'newton', path='/foo', domain='.test.url')
        orig_resp = self.create_response(cookies=jar, text=text)

        d = pickle.dumps(orig_resp)
        new_resp = pickle.loads(d)

        self.assertEqual(text, new_resp.text)
        self.assertEqual('newton', new_resp.cookies['fig'])
        self.assertIsNone(new_resp.request.matcher)

    def test_response_encoding(self):
        headers = {"content-type": "text/html; charset=ISO-8859-1"}
        resp = self.create_response(headers=headers,
                                    text="<html><body></body></html")
        self.assertEqual('ISO-8859-1', resp.encoding)

    def test_default_reason(self):
        resp = self.create_response()
        self.assertEqual('OK', resp.reason)

    def test_custom_reason(self):
        reason = 'Live long and prosper'
        resp = self.create_response(status_code=201, reason=reason)

        self.assertEqual(201, resp.status_code)
        self.assertEqual(reason, resp.reason)

    def test_some_other_response_reasons(self):
        reasons = {
            301: 'Moved Permanently',
            410: 'Gone',
            503: 'Service Unavailable',
        }

        for code, reason in reasons.items():
            self.assertEqual(reason,
                             self.create_response(status_code=code).reason)
