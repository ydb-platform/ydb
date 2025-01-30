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

import requests
import six

import requests_mock
from . import base


class FailMatcher(object):

    def __init___(self):
        self.called = False

    def __call__(self, request):
        self.called = True
        return None


def match_all(request):
    return requests_mock.create_response(request, content=six.b('data'))


class CustomMatchersTests(base.TestCase):

    def assertMatchAll(self, resp):
        self.assertEqual(200, resp.status_code)
        self.assertEqual(resp.text, six.u('data'))

    @requests_mock.Mocker()
    def test_custom_matcher(self, mocker):
        mocker.add_matcher(match_all)

        resp = requests.get('http://any/thing')
        self.assertMatchAll(resp)

    @requests_mock.Mocker()
    def test_failing_matcher(self, mocker):
        failer = FailMatcher()

        mocker.add_matcher(match_all)
        mocker.add_matcher(failer)

        resp = requests.get('http://any/thing')

        self.assertMatchAll(resp)
        self.assertTrue(failer.called)

    @requests_mock.Mocker()
    def test_some_pass(self, mocker):

        def matcher_a(request):
            if 'a' in request.url:
                return match_all(request)

            return None

        mocker.add_matcher(matcher_a)

        resp = requests.get('http://any/thing')
        self.assertMatchAll(resp)

        self.assertRaises(requests_mock.NoMockAddress,
                          requests.get,
                          'http://other/thing')
