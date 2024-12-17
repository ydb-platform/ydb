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

import re

from requests_mock import adapter
from . import base
from requests_mock.response import _MatcherResponse

ANY = adapter.ANY


class TestMatcher(base.TestCase):

    def match(self,
              target,
              url,
              matcher_method='GET',
              request_method='GET',
              complete_qs=False,
              headers=None,
              request_data=None,
              request_headers={},
              additional_matcher=None,
              real_http=False,
              case_sensitive=False):
        matcher = adapter._Matcher(matcher_method,
                                   target,
                                   [],
                                   complete_qs=complete_qs,
                                   additional_matcher=additional_matcher,
                                   request_headers=request_headers,
                                   real_http=real_http,
                                   case_sensitive=case_sensitive)
        request = adapter._RequestObjectProxy._create(request_method,
                                                      url,
                                                      headers,
                                                      data=request_data)
        return matcher._match(request)

    def assertMatch(self,
                    target=ANY,
                    url='http://example.com/requests-mock',
                    matcher_method='GET',
                    request_method='GET',
                    **kwargs):
        self.assertEqual(True,
                         self.match(target,
                                    url,
                                    matcher_method=matcher_method,
                                    request_method=request_method,
                                    **kwargs),
                         'Matcher %s %s failed to match %s %s' %
                         (matcher_method, target, request_method, url))

    def assertMatchBoth(self,
                        target=ANY,
                        url='http://example.com/requests-mock',
                        matcher_method='GET',
                        request_method='GET',
                        **kwargs):
        self.assertMatch(target,
                         url,
                         matcher_method=matcher_method,
                         request_method=request_method,
                         **kwargs)
        self.assertMatch(url,
                         target,
                         matcher_method=request_method,
                         request_method=matcher_method,
                         **kwargs)

    def assertNoMatch(self,
                      target=ANY,
                      url='http://example.com/requests-mock',
                      matcher_method='GET',
                      request_method='GET',
                      **kwargs):
        self.assertEqual(False,
                         self.match(target,
                                    url,
                                    matcher_method=matcher_method,
                                    request_method=request_method,
                                    **kwargs),
                         'Matcher %s %s unexpectedly matched %s %s' %
                         (matcher_method, target, request_method, url))

    def assertNoMatchBoth(self,
                          target=ANY,
                          url='http://example.com/requests-mock',
                          matcher_method='GET',
                          request_method='GET',
                          **kwargs):
        self.assertNoMatch(target,
                           url,
                           matcher_method=matcher_method,
                           request_method=request_method,
                           **kwargs)
        self.assertNoMatch(url,
                           target,
                           matcher_method=request_method,
                           request_method=matcher_method,
                           **kwargs)

    def assertMatchMethodBoth(self, matcher_method, request_method, **kwargs):
        url = 'http://www.test.com'

        self.assertMatchBoth(url,
                             url,
                             request_method=request_method,
                             matcher_method=matcher_method,
                             **kwargs)

    def assertNoMatchMethodBoth(self,
                                matcher_method,
                                request_method,
                                **kwargs):
        url = 'http://www.test.com'

        self.assertNoMatchBoth(url,
                               url,
                               request_method=request_method,
                               matcher_method=matcher_method,
                               **kwargs)

    def test_url_matching(self):
        self.assertMatchBoth('http://www.test.com',
                             'http://www.test.com')
        self.assertMatchBoth('http://www.test.com',
                             'http://www.test.com/')
        self.assertMatchBoth('http://www.test.com/abc',
                             'http://www.test.com/abc')
        self.assertMatchBoth('http://www.test.com:5000/abc',
                             'http://www.test.com:5000/abc')
        self.assertNoMatchBoth('https://www.test.com',
                               'http://www.test.com')
        self.assertNoMatchBoth('http://www.test.com/abc',
                               'http://www.test.com')
        self.assertNoMatchBoth('http://test.com',
                               'http://www.test.com')
        self.assertNoMatchBoth('http://test.com',
                               'http://www.test.com')
        self.assertNoMatchBoth('http://test.com/abc',
                               'http://www.test.com/abc/')
        self.assertNoMatchBoth('http://test.com/abc/',
                               'http://www.test.com/abc')
        self.assertNoMatchBoth('http://test.com:5000/abc/',
                               'http://www.test.com/abc')
        self.assertNoMatchBoth('http://test.com/abc/',
                               'http://www.test.com:5000/abc')

    def test_quotation(self):
        self.assertMatchBoth('http://www.test.com/a string%url',
                             'http://www.test.com/a string%url')
        self.assertMatchBoth('http://www.test.com/ABC 123',
                             'http://www.test.com/ABC%20123')
        self.assertMatchBoth('http://www.test.com/user@example.com',
                             'http://www.test.com/user@example.com')

    def test_subset_match(self):
        self.assertMatch('/path', 'http://www.test.com/path')
        self.assertMatch('/path', 'http://www.test.com/path')
        self.assertMatch('//www.test.com/path', 'http://www.test.com/path')
        self.assertMatch('//www.test.com/path', 'https://www.test.com/path')

    def test_query_string(self):
        self.assertMatch('/path?a=1&b=2',
                         'http://www.test.com/path?a=1&b=2')
        self.assertMatch('/path?a=1',
                         'http://www.test.com/path?a=1&b=2',
                         complete_qs=False)
        self.assertNoMatch('/path?a=1',
                           'http://www.test.com/path?a=1&b=2',
                           complete_qs=True)
        self.assertNoMatch('/path?a=1&b=2',
                           'http://www.test.com/path?a=1')

    def test_query_empty_string(self):
        self.assertMatch('/path?a',
                         'http://www.test.com/path?a')
        self.assertMatch('/path?bob&paul',
                         'http://www.test.com/path?paul&bob')
        self.assertNoMatch('/path?bob',
                           'http://www.test.com/path?paul')
        self.assertNoMatch('/path?pual&bob',
                           'http://www.test.com/path?bob')

    def test_method_match(self):
        self.assertNoMatchMethodBoth('GET', 'POST')
        self.assertMatchMethodBoth('GET', 'get')
        self.assertMatchMethodBoth('GeT', 'geT')

    def test_match_ANY_url(self):
        self.assertMatch(ANY, 'http://anything')
        self.assertMatch(ANY, 'http://somethingelse')
        self.assertNoMatch(ANY, 'http://somethingelse', request_method='POST')

    def test_match_ANY_method(self):
        for m in ('GET', 'POST', 'HEAD', 'OPTION'):
            self.assertMatch('http://www.test.com',
                             'http://www.test.com',
                             matcher_method=ANY,
                             request_method=m)

        self.assertNoMatch('http://www.test.com',
                           'http://another',
                           matcher_method=ANY)

    def test_match_with_regex(self):
        r1 = re.compile('test.com/a')
        r2 = re.compile('/b/c')

        self.assertMatch(r1, 'http://mock.test.com/a/b')
        self.assertMatch(r1, 'http://test.com/a/')
        self.assertMatch(r1, 'mock://test.com/a/b')
        self.assertNoMatch(r1, 'mock://test.com/')

        self.assertMatch(r2, 'http://anything/a/b/c/d')
        self.assertMatch(r2, 'mock://anything/a/b/c/d')

    def test_match_with_headers(self):
        self.assertMatch('/path',
                         'http://www.test.com/path',
                         headers={'A': 'abc', 'b': 'def'},
                         request_headers={'a': 'abc'})

        self.assertMatch('/path',
                         'http://www.test.com/path',
                         headers={'A': 'abc', 'b': 'def'})

        self.assertNoMatch('/path',
                           'http://www.test.com/path',
                           headers={'A': 'abc', 'b': 'def'},
                           request_headers={'b': 'abc'})

        self.assertNoMatch('/path',
                           'http://www.test.com/path',
                           headers={'A': 'abc', 'b': 'def'},
                           request_headers={'c': 'ghi'})

        # headers should be key insensitive and value sensitive, we have no
        # choice here because they go into an insensitive dict.
        self.assertMatch('/path',
                         'http://www.test.com/path',
                         headers={'aBc': 'abc', 'DEF': 'def'},
                         request_headers={'abC': 'abc'})

        self.assertNoMatch('/path',
                           'http://www.test.com/path',
                           headers={'abc': 'aBC', 'DEF': 'def'},
                           request_headers={'abc': 'Abc'})

    def test_case_sensitive_ignored_for_netloc_and_protocol(self):
        for case_sensitive in (True, False):
            self.assertMatch('http://AbC.CoM',
                             'http://aBc.CoM',
                             case_sensitive=case_sensitive)

            self.assertMatch('htTP://abc.com',
                             'hTTp://abc.com',
                             case_sensitive=case_sensitive)

            self.assertMatch('htTP://aBC.cOm',
                             'hTTp://AbC.Com',
                             case_sensitive=case_sensitive)

    def assertSensitiveMatch(self, target, url, **kwargs):
        self.assertMatch(target, url, case_sensitive=False, **kwargs)
        self.assertNoMatch(target, url, case_sensitive=True, **kwargs)

    def test_case_sensitive_paths(self):
        self.assertSensitiveMatch('http://abc.com/pAtH', 'http://abc.com/path')
        self.assertSensitiveMatch('/pAtH', 'http://abc.com/path')

    def test_case_sensitive_query(self):
        self.assertSensitiveMatch('http://abc.com/path?abCD=efGH',
                                  'http://abc.com/path?abCd=eFGH')

        self.assertSensitiveMatch('http://abc.com/path?abcd=efGH',
                                  'http://abc.com/path?abcd=eFGH')

    def test_additional_matcher(self):

        def test_match_body(request):
            return 'hello' in request.text

        self.assertMatch(request_method='POST',
                         matcher_method='POST',
                         request_data='hello world',
                         additional_matcher=test_match_body)

        self.assertNoMatch(request_method='POST',
                           matcher_method='POST',
                           request_data='goodbye world',
                           additional_matcher=test_match_body)

    def test_reset_reverts_count(self):
        url = 'mock://test/site/'
        matcher = adapter._Matcher('GET',
                                   url,
                                   [_MatcherResponse()],
                                   complete_qs=False,
                                   additional_matcher=None,
                                   request_headers={},
                                   real_http=False,
                                   case_sensitive=False)
        request = adapter._RequestObjectProxy._create('GET', url)

        call_count = 3
        for _ in range(call_count):
            matcher(request)

        self.assertEqual(matcher.call_count, call_count)
        matcher.reset()
        self.assertEqual(matcher.call_count, 0)
