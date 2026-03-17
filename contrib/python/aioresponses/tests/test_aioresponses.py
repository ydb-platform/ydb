# -*- coding: utf-8 -*-
import asyncio
import re
from asyncio import CancelledError, TimeoutError
from random import uniform
from typing import Coroutine, Generator, Union
from unittest.mock import patch

from aiohttp import hdrs
from aiohttp import http
from aiohttp import web
from aiohttp.client import ClientSession
from aiohttp.client_reqrep import ClientResponse
from aiohttp.test_utils import TestServer
from packaging.version import Version

try:
    from aiohttp.errors import (
        ClientConnectionError,
        ClientResponseError,
        HttpProcessingError,
    )
except ImportError:
    from aiohttp.client_exceptions import (
        ClientConnectionError,
        ClientResponseError,
    )
    from aiohttp.http_exceptions import HttpProcessingError

from aioresponses.compat import AIOHTTP_VERSION, URL
from aioresponses import CallbackResult, aioresponses
from .base import fail_on, skipIf, AsyncTestCase


MINIMUM_AIOHTTP_VERSION = Version('3.4.0')

class AIOResponsesTestCase(AsyncTestCase):

    async def setup(self):
        self.url = 'http://example.com/api?foo=bar#fragment'
        self.session = ClientSession()
        self.external_server = self.make_external_server()
        await self.external_server.start_server()

    async def teardown(self):
        close_result = self.session.close()
        if close_result is not None:
            await close_result

    def run_async(self, coroutine: Union[Coroutine, Generator]):
        return self.loop.run_until_complete(coroutine)

    def make_external_server(self):
        """
        В оригинальном коде для тестирования passthrough исполняются настоящие
        запросы до сайта http://httpbin.org. В sandbox нет интернета, потому я заменил
        httpbin на локальный сервер. Осторожнее при обновлении!
        """
        async def testserver_status_201(request):
            return web.Response(status=201)

        async def testserver_get(request):
            return web.Response()

        app = web.Application()
        app.router.add_get('/status/201', testserver_status_201)
        app.router.add_get('/get', testserver_get)

        return TestServer(app)

    async def request(self, url: str):
        return await self.session.get(url)

    @fail_on(unused_loop=False)
    def test_shortcut_method(self):
        for http_method in (
            hdrs.METH_HEAD,
            hdrs.METH_GET,
            hdrs.METH_POST,
            hdrs.METH_PUT,
            hdrs.METH_PATCH,
            hdrs.METH_DELETE,
            hdrs.METH_OPTIONS,
        ):
            with self.subTest(), \
                    patch('aioresponses.aioresponses.add') as mocked, \
                    aioresponses() as m:
                getattr(m, http_method.lower())(self.url)
                mocked.assert_called_once_with(self.url, method=http_method)

    @aioresponses()
    def test_returned_instance(self, m):
        m.get(self.url)
        response = self.run_async(self.session.get(self.url))
        self.assertIsInstance(response, ClientResponse)

    @aioresponses()
    async def test_returned_instance_and_status_code(self, m):
        m.get(self.url, status=204)
        response = await self.session.get(self.url)
        self.assertIsInstance(response, ClientResponse)
        self.assertEqual(response.status, 204)

    @aioresponses()
    async def test_returned_response_headers(self, m):
        m.get(self.url,
              content_type='text/html',
              headers={'Connection': 'keep-alive'})
        response = await self.session.get(self.url)

        self.assertEqual(response.headers['Connection'], 'keep-alive')
        self.assertEqual(response.headers[hdrs.CONTENT_TYPE], 'text/html')

    @aioresponses()
    async def test_returned_response_cookies(self, m):
        m.get(self.url,
              headers={'Set-Cookie': 'cookie=value'})
        response = await self.session.get(self.url)

        self.assertEqual(response.cookies['cookie'].value, 'value')

    @aioresponses()
    async def test_returned_response_raw_headers(self, m):
        m.get(self.url,
              content_type='text/html',
              headers={'Connection': 'keep-alive'})
        response = await self.session.get(self.url)
        expected_raw_headers = (
            (hdrs.CONTENT_TYPE.encode(), b'text/html'),
            (b'Connection', b'keep-alive')
        )

        self.assertEqual(response.raw_headers, expected_raw_headers)

    @aioresponses()
    async def test_raise_for_status(self, m):
        m.get(self.url, status=400)
        with self.assertRaises(ClientResponseError) as cm:
            response = await self.session.get(self.url)
            response.raise_for_status()
        self.assertEqual(cm.exception.message, http.RESPONSES[400][0])

    @aioresponses()
    @skipIf(condition=AIOHTTP_VERSION < MINIMUM_AIOHTTP_VERSION,
            reason='aiohttp<3.4.0 does not support raise_for_status '
                   'arguments for requests')
    async def test_request_raise_for_status(self, m):
        m.get(self.url, status=400)
        with self.assertRaises(ClientResponseError) as cm:
            await self.session.get(self.url, raise_for_status=True)
        self.assertEqual(cm.exception.message, http.RESPONSES[400][0])

    @aioresponses()
    async def test_returned_instance_and_params_handling(self, m):
        expected_url = 'http://example.com/api?foo=bar&x=42#fragment'
        m.get(expected_url)
        response = await self.session.get(self.url, params={'x': 42})
        self.assertIsInstance(response, ClientResponse)
        self.assertEqual(response.status, 200)

        expected_url = 'http://example.com/api?x=42#fragment'
        m.get(expected_url)
        response = await self.session.get(
            'http://example.com/api#fragment',
            params={'x': 42}
        )
        self.assertIsInstance(response, ClientResponse)
        self.assertEqual(response.status, 200)
        self.assertEqual(len(m.requests), 2)
        with self.assertRaises(AssertionError):
            m.assert_called_once()

    @aioresponses()
    def test_method_dont_match(self, m):
        m.get(self.url)
        with self.assertRaises(ClientConnectionError):
            self.run_async(self.session.post(self.url))

    @aioresponses()
    def test_post_with_data(self, m: aioresponses):
        body = {'foo': 'bar'}
        payload = {'spam': 'eggs'}
        user_agent = {'User-Agent': 'aioresponses'}
        m.post(
            self.url,
            payload=payload,
            headers=dict(connection='keep-alive'),
            body=body,
        )
        response = self.run_async(
            self.session.post(
                self.url,
                data=payload,
                headers=user_agent
            )
        )
        self.assertIsInstance(response, ClientResponse)
        self.assertEqual(response.status, 200)
        response_data = self.run_async(response.json())
        self.assertEqual(response_data, payload)
        m.assert_called_once_with(
            self.url,
            method='POST',
            data=payload,
            headers={'User-Agent': 'aioresponses'}
        )
        # Wrong data
        with self.assertRaises(AssertionError):
            m.assert_called_once_with(
                self.url,
                method='POST',
                data=body,
                headers={'User-Agent': 'aioresponses'}
            )
        # Wrong url
        with self.assertRaises(AssertionError):
            m.assert_called_once_with(
                'http://httpbin.org/',
                method='POST',
                data=payload,
                headers={'User-Agent': 'aioresponses'}
            )
        # Wrong headers
        with self.assertRaises(AssertionError):
            m.assert_called_once_with(
                self.url,
                method='POST',
                data=payload,
                headers={'User-Agent': 'aiorequest'}
            )

    @aioresponses()
    async def test_streaming(self, m):
        m.get(self.url, body='Test')
        resp = await self.session.get(self.url)
        content = await resp.content.read()
        self.assertEqual(content, b'Test')

    @aioresponses()
    async def test_streaming_up_to(self, m):
        m.get(self.url, body='Test')
        resp = await self.session.get(self.url)
        content = await resp.content.read(2)
        self.assertEqual(content, b'Te')
        content = await resp.content.read(2)
        self.assertEqual(content, b'st')

    @aioresponses()
    async def test_binary_body(self, m):
        body = b'Invalid utf-8: \x95\x00\x85'
        m.get(self.url, body=body)
        resp = await self.session.get(self.url)
        content = await resp.read()
        self.assertEqual(content, body)

    @aioresponses()
    async def test_binary_body_via_callback(self, m):
        body = b'\x00\x01\x02\x80\x81\x82\x83\x84\x85'

        def callback(url, **kwargs):
            return CallbackResult(body=body)

        m.get(self.url, callback=callback)
        resp = await self.session.get(self.url)
        content = await resp.read()
        self.assertEqual(content, body)

    async def test_mocking_as_context_manager(self):
        with aioresponses() as aiomock:
            aiomock.add(self.url, payload={'foo': 'bar'})
            resp = await self.session.get(self.url)
            self.assertEqual(resp.status, 200)
            payload = await resp.json()
            self.assertDictEqual(payload, {'foo': 'bar'})

    def test_mocking_as_decorator(self):
        @aioresponses()
        def foo(loop, m):
            m.add(self.url, payload={'foo': 'bar'})

            resp = loop.run_until_complete(self.session.get(self.url))
            self.assertEqual(resp.status, 200)
            payload = loop.run_until_complete(resp.json())
            self.assertDictEqual(payload, {'foo': 'bar'})

        foo(self.loop)

    async def test_passing_argument(self):
        @aioresponses(param='mocked')
        async def foo(mocked):
            mocked.add(self.url, payload={'foo': 'bar'})
            resp = await self.session.get(self.url)
            self.assertEqual(resp.status, 200)

        await foo()

    @fail_on(unused_loop=False)
    def test_mocking_as_decorator_wrong_mocked_arg_name(self):
        @aioresponses(param='foo')
        def foo(bar):
            # no matter what is here it should raise an error
            pass

        with self.assertRaises(TypeError) as cm:
            foo()
        exc = cm.exception
        self.assertIn("foo() got an unexpected keyword argument 'foo'",
                      str(exc))

    async def test_unknown_request(self):
        with aioresponses() as aiomock:
            aiomock.add(self.url, payload={'foo': 'bar'})
            with self.assertRaises(ClientConnectionError):
                await self.session.get('http://example.com/foo')

    async def test_raising_exception(self):
        with aioresponses() as aiomock:
            url = 'http://example.com/Exception'
            aiomock.get(url, exception=Exception)
            with self.assertRaises(Exception):
                await self.session.get(url)

            url = 'http://example.com/Exception_object'
            aiomock.get(url, exception=Exception())
            with self.assertRaises(Exception):
                await self.session.get(url)

            url = 'http://example.com/BaseException'
            aiomock.get(url, exception=BaseException)
            with self.assertRaises(BaseException):
                await self.session.get(url)

            url = 'http://example.com/BaseException_object'
            aiomock.get(url, exception=BaseException())
            with self.assertRaises(BaseException):
                await self.session.get(url)

            url = 'http://example.com/CancelError'
            aiomock.get(url, exception=CancelledError)
            with self.assertRaises(CancelledError):
                await self.session.get(url)

            url = 'http://example.com/TimeoutError'
            aiomock.get(url, exception=TimeoutError)
            with self.assertRaises(TimeoutError):
                await self.session.get(url)

            url = 'http://example.com/HttpProcessingError'
            aiomock.get(url, exception=HttpProcessingError(message='foo'))
            with self.assertRaises(HttpProcessingError):
                await self.session.get(url)

            callback_called = asyncio.Event()
            url = 'http://example.com/HttpProcessingError'
            aiomock.get(url, exception=HttpProcessingError(message='foo'),
                        callback=lambda *_, **__: callback_called.set())
            with self.assertRaises(HttpProcessingError):
                await self.session.get(url)

            await callback_called.wait()

    async def test_multiple_requests(self):
        """Ensure that requests are saved the way they would have been sent."""
        with aioresponses() as m:
            m.get(self.url, status=200)
            m.get(self.url, status=201)
            m.get(self.url, status=202)
            json_content_as_ref = [1]
            resp = await self.session.get(
                self.url, json=json_content_as_ref
            )
            self.assertEqual(resp.status, 200)
            json_content_as_ref[:] = [2]
            resp = await self.session.get(
                self.url, json=json_content_as_ref
            )
            self.assertEqual(resp.status, 201)
            json_content_as_ref[:] = [3]
            resp = await self.session.get(
                self.url, json=json_content_as_ref
            )
            self.assertEqual(resp.status, 202)

            key = ('GET', URL(self.url))
            self.assertIn(key, m.requests)
            self.assertEqual(len(m.requests[key]), 3)

            first_request = m.requests[key][0]
            self.assertEqual(first_request.args, tuple())
            self.assertEqual(first_request.kwargs,
                             {'allow_redirects': True, "json": [1]})

            second_request = m.requests[key][1]
            self.assertEqual(second_request.args, tuple())
            self.assertEqual(second_request.kwargs,
                             {'allow_redirects': True, "json": [2]})

            third_request = m.requests[key][2]
            self.assertEqual(third_request.args, tuple())
            self.assertEqual(third_request.kwargs,
                             {'allow_redirects': True, "json": [3]})

    async def test_request_with_non_deepcopyable_parameter(self):
        def non_deep_copyable():
            """A generator does not allow deepcopy."""
            for line in ["header1,header2", "v1,v2", "v10,v20"]:
                yield line

        generator_value = non_deep_copyable()

        with aioresponses() as m:
            m.get(self.url, status=200)
            resp = await self.session.get(self.url, data=generator_value)
            self.assertEqual(resp.status, 200)

            key = ('GET', URL(self.url))
            self.assertIn(key, m.requests)
            self.assertEqual(len(m.requests[key]), 1)

            request = m.requests[key][0]
            self.assertEqual(request.args, tuple())
            self.assertEqual(request.kwargs,
                             {'allow_redirects': True,
                                 "data": generator_value})

    async def test_request_retrieval_in_case_no_response(self):
        with aioresponses() as m:
            with self.assertRaises(ClientConnectionError):
                await self.session.get(self.url)

            key = ('GET', URL(self.url))
            self.assertIn(key, m.requests)
            self.assertEqual(len(m.requests[key]), 1)
            self.assertEqual(m.requests[key][0].args, tuple())
            self.assertEqual(m.requests[key][0].kwargs,
                             {'allow_redirects': True})

    async def test_request_failure_in_case_session_is_closed(self):
        async def do_request(session):
            return (await session.get(self.url))

        with aioresponses():
            coro = do_request(self.session)
            await self.session.close()

            with self.assertRaises(RuntimeError) as exception_info:
                await coro
            assert str(exception_info.exception) == "Session is closed"

    async def test_address_as_instance_of_url_combined_with_pass_through(self):
        external_api = str(self.external_server.make_url('/status/201'))

        async def doit():
            api_resp = await self.session.get(self.url)
            # we have to hit actual url,
            # otherwise we do not test pass through option properly
            ext_rep = await self.session.get(URL(external_api))
            return api_resp, ext_rep

        with aioresponses(passthrough=[external_api]) as m:
            m.get(self.url, status=200)
            api, ext = await doit()

            self.assertEqual(api.status, 200)
            self.assertEqual(ext.status, 201)

    async def test_pass_through_with_origin_params(self):
        external_api = str(self.external_server.make_url('/get'))

        async def doit(params):
            # we have to hit actual url,
            # otherwise we do not test pass through option properly
            ext_rep = await self.session.get(
                URL(external_api), params=params
            )
            return ext_rep

        with aioresponses(passthrough=[external_api]) as m:
            params = {'foo': 'bar'}
            ext = await doit(params=params)
            self.assertEqual(ext.status, 200)
            self.assertEqual(str(ext.url), external_api + '?foo=bar')

    @aioresponses()
    async def test_custom_response_class(self, m):
        class CustomClientResponse(ClientResponse):
            pass

        m.get(self.url, body='Test', response_class=CustomClientResponse)
        resp = await self.session.get(self.url)
        self.assertTrue(isinstance(resp, CustomClientResponse))

    @aioresponses()
    def test_exceptions_in_the_middle_of_responses(self, mocked):
        mocked.get(self.url, payload={}, status=204)
        mocked.get(self.url, exception=ValueError('oops'), )
        mocked.get(self.url, payload={}, status=204)
        mocked.get(self.url, exception=ValueError('oops'), )
        mocked.get(self.url, payload={}, status=200)

        async def doit():
            return (await self.session.get(self.url))

        self.assertEqual(self.run_async(doit()).status, 204)
        with self.assertRaises(ValueError):
            self.run_async(doit())
        self.assertEqual(self.run_async(doit()).status, 204)
        with self.assertRaises(ValueError):
            self.run_async(doit())
        self.assertEqual(self.run_async(doit()).status, 200)

    @aioresponses()
    async def test_request_should_match_regexp(self, mocked):
        mocked.get(
            re.compile(r'^http://example\.com/api\?foo=.*$'),
            payload={}, status=200
        )

        response = await self.request(self.url)
        self.assertEqual(response.status, 200)

    @aioresponses()
    async def test_request_does_not_match_regexp(self, mocked):
        mocked.get(
            re.compile(r'^http://exampleexample\.com/api\?foo=.*$'),
            payload={}, status=200
        )
        with self.assertRaises(ClientConnectionError):
            await self.request(self.url)

    @aioresponses()
    def test_timeout(self, mocked):
        mocked.get(self.url, timeout=True)

        with self.assertRaises(asyncio.TimeoutError):
            self.run_async(self.request(self.url))

    @aioresponses()
    def test_callback(self, m):
        body = b'New body'

        def callback(url, **kwargs):
            self.assertEqual(str(url), self.url)
            self.assertEqual(kwargs, {'allow_redirects': True})
            return CallbackResult(body=body)

        m.get(self.url, callback=callback)
        response = self.run_async(self.request(self.url))
        data = self.run_async(response.read())
        assert data == body

    @aioresponses()
    def test_callback_coroutine(self, m):
        body = b'New body'
        event = asyncio.Event()

        async def callback(url, **kwargs):
            await event.wait()
            self.assertEqual(str(url), self.url)
            self.assertEqual(kwargs, {'allow_redirects': True})
            return CallbackResult(body=body)

        m.get(self.url, callback=callback)
        future = asyncio.ensure_future(self.request(self.url))
        self.run_async(asyncio.wait([future], timeout=0))
        assert not future.done()
        event.set()
        self.run_async(asyncio.wait([future], timeout=0))
        assert future.done()
        response = future.result()
        data = self.run_async(response.read())
        assert data == body

    @aioresponses()
    def test_assert_not_called(self, m: aioresponses):
        m.get(self.url)
        m.assert_not_called()
        self.run_async(self.session.get(self.url))
        with self.assertRaises(AssertionError):
            m.assert_not_called()

    @aioresponses()
    def test_assert_called(self, m: aioresponses):
        m.get(self.url)
        with self.assertRaises(AssertionError):
            m.assert_called()
        self.run_async(self.session.get(self.url))

        m.assert_called_once()
        m.assert_called_once_with(self.url)
        m.assert_called_with(self.url)
        with self.assertRaises(AssertionError):
            m.assert_not_called()

        with self.assertRaises(AssertionError):
            m.assert_called_with("http://foo.bar")

    @aioresponses()
    async def test_assert_called_twice(self, m: aioresponses):
        m.get(self.url, repeat=True)
        m.assert_not_called()
        await self.session.get(self.url)
        await self.session.get(self.url)
        with self.assertRaises(AssertionError):
            m.assert_called_once()

    @aioresponses()
    async def test_assert_any_call(self, m: aioresponses):
        http_bin_url = "http://httpbin.org"
        m.get(self.url)
        m.get(http_bin_url)
        await self.session.get(self.url)
        response = await self.session.get(http_bin_url)
        self.assertEqual(response.status, 200)
        m.assert_any_call(self.url)
        m.assert_any_call(http_bin_url)

    @aioresponses()
    async def test_assert_any_call_not_called(self, m: aioresponses):
        http_bin_url = "http://httpbin.org"
        m.get(self.url)
        response = await self.session.get(self.url)
        self.assertEqual(response.status, 200)
        m.assert_any_call(self.url)
        with self.assertRaises(AssertionError):
            m.assert_any_call(http_bin_url)

    @aioresponses()
    async def test_exception_requests_are_tracked(self, mocked):
        kwargs = {"json": [42], "allow_redirects": True}
        mocked.get(self.url, exception=ValueError('oops'))

        with self.assertRaises(ValueError):
            await self.session.get(self.url, **kwargs)

        key = ('GET', URL(self.url))
        mocked_requests = mocked.requests[key]
        self.assertEqual(len(mocked_requests), 1)

        request = mocked_requests[0]
        self.assertEqual(request.args, ())
        self.assertEqual(request.kwargs, kwargs)

    async def test_possible_race_condition(self):
        async def random_sleep_cb(url, **kwargs):
            await asyncio.sleep(uniform(0.1, 1))
            return CallbackResult(body='test')

        with aioresponses() as mocked:
            for i in range(20):
                mocked.get(
                    'http://example.org/id-{}'.format(i),
                    callback=random_sleep_cb
                )

            tasks = [
                self.session.get('http://example.org/id-{}'.format(i)) for
                i in range(20)
            ]
            await asyncio.gather(*tasks)


class AIOResponsesRaiseForStatusSessionTestCase(AsyncTestCase):
    """Test case for sessions with raise_for_status=True.

    This flag, introduced in aiohttp v2.0.0, automatically calls
    `raise_for_status()`.
    It is overridden by the `raise_for_status` argument of the request since
    aiohttp v3.4.a0.

    """

    async def setup(self):
        self.url = 'http://example.com/api?foo=bar#fragment'
        self.session = ClientSession(raise_for_status=True)

    async def teardown(self):
        close_result = self.session.close()
        if close_result is not None:
            await close_result

    @aioresponses()
    async def test_raise_for_status(self, m):
        m.get(self.url, status=400)
        with self.assertRaises(ClientResponseError) as cm:
            await self.session.get(self.url)
        self.assertEqual(cm.exception.message, http.RESPONSES[400][0])

    @aioresponses()
    @skipIf(condition=AIOHTTP_VERSION < MINIMUM_AIOHTTP_VERSION,
            reason='aiohttp<3.4.0 does not support raise_for_status '
                   'arguments for requests')
    async def test_do_not_raise_for_status(self, m):
        m.get(self.url, status=400)
        response = await self.session.get(self.url,
                                          raise_for_status=False)

        self.assertEqual(response.status, 400)

    @aioresponses()
    @skipIf(condition=AIOHTTP_VERSION < Version('3.9.0'),
            reason='aiohttp<3.9.0 does not support callable raise_for_status '
                   'arguments for requests')
    async def test_callable_raise_for_status(self, m):
        async def raise_for_status(response: ClientResponse):
            if response.status >= 400:
                raise Exception("callable raise_for_status")

        m.get(self.url, status=400)
        with self.assertRaises(Exception) as cm:
            await self.session.get(self.url,
                                   raise_for_status=raise_for_status)
        self.assertEqual(str(cm.exception), "callable raise_for_status")


class AIOResponseRedirectTest(AsyncTestCase):

    async def setup(self):
        self.url = "http://10.1.1.1:8080/redirect"
        self.session = ClientSession()

    async def teardown(self):
        close_result = self.session.close()
        if close_result is not None:
            await close_result

    @aioresponses()
    async def test_redirect_followed(self, rsps):
        rsps.get(
            self.url,
            status=307,
            headers={"Location": "https://httpbin.org"},
        )
        rsps.get("https://httpbin.org")
        response = await self.session.get(
            self.url, allow_redirects=True
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(str(response.url), "https://httpbin.org")
        self.assertEqual(len(response.history), 1)
        self.assertEqual(str(response.history[0].url), self.url)

    @aioresponses()
    async def test_post_redirect_followed(self, rsps):
        rsps.post(
            self.url,
            status=307,
            headers={"Location": "https://httpbin.org"},
        )
        rsps.get("https://httpbin.org")
        response = await self.session.post(
            self.url, allow_redirects=True
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(str(response.url), "https://httpbin.org")
        self.assertEqual(response.method, "get")
        self.assertEqual(len(response.history), 1)
        self.assertEqual(str(response.history[0].url), self.url)

    @aioresponses()
    async def test_redirect_missing_mocked_match(self, rsps):
        rsps.get(
            self.url,
            status=307,
            headers={"Location": "https://httpbin.org"},
        )
        with self.assertRaises(ClientConnectionError) as cm:
            await self.session.get(
                self.url, allow_redirects=True
            )
        self.assertEqual(
            str(cm.exception),
            'Connection refused: GET http://10.1.1.1:8080/redirect'
        )

    @aioresponses()
    async def test_redirect_missing_location_header(self, rsps):
        rsps.get(self.url, status=307)
        response = await self.session.get(self.url, allow_redirects=True)
        self.assertEqual(str(response.url), self.url)

    @aioresponses()
    async def test_request_info(self, rsps):
        rsps.get(self.url, status=200)

        response = await self.session.get(self.url)

        request_info = response.request_info
        assert str(request_info.url) == self.url
        assert request_info.headers == {}

    @aioresponses()
    async def test_request_info_with_original_request_headers(self, rsps):
        headers = {"Authorization": "Bearer access-token"}
        rsps.get(self.url, status=200)

        response = await self.session.get(self.url, headers=headers)

        request_info = response.request_info
        assert str(request_info.url) == self.url
        assert request_info.headers == headers

    @aioresponses()
    async def test_relative_url_redirect_followed(self, rsps):
        base_url = "https://httpbin.org"
        url = f"{base_url}/foo/bar"
        rsps.get(
            url,
            status=307,
            headers={"Location": "../baz"},
        )
        rsps.get(f"{base_url}/baz")

        response = await self.session.get(url, allow_redirects=True)

        self.assertEqual(response.status, 200)
        self.assertEqual(str(response.url), f"{base_url}/baz")
        self.assertEqual(len(response.history), 1)
        self.assertEqual(str(response.history[0].url), url)

    async def _test_pass_through_unmatched_requests(self):
        matched_url = "https://matched_example.org"
        unmatched_url = "https://httpbin.org/get"
        params_unmatched = {'foo': 'bar'}

        with aioresponses(passthrough_unmatched=True) as m:
            m.post(URL(matched_url), status=200)
            mocked_response = await self.session.post(URL(matched_url))
            response = await self.session.get(
                URL(unmatched_url), params=params_unmatched
            )
            self.assertEqual(response.status, 200)
            self.assertEqual(str(response.url), 'https://httpbin.org/get?foo=bar')
            self.assertEqual(mocked_response.status, 200)
