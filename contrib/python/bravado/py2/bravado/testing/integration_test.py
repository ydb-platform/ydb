# -*- coding: utf-8 -*-
import json
import time
import typing
from multiprocessing import Process

import bottle
import ephemeral_port_reserve
import pytest
import requests.exceptions
from bravado_core.content_type import APP_MSGPACK
from bravado_core.response import IncomingResponse
from msgpack import packb
from msgpack import unpackb

from bravado.client import SwaggerClient
from bravado.exception import BravadoConnectionError
from bravado.exception import BravadoTimeoutError
from bravado.exception import HTTPMovedPermanently
from bravado.http_client import HttpClient
from bravado.http_future import FutureAdapter
from bravado.swagger_model import Loader


def _class_fqn(clz):
    return '{t.__module__}.{t.__name__}'.format(t=clz)


ROUTE_1_RESPONSE = b'HEY BUDDY'
ROUTE_2_RESPONSE = b'BYE BUDDY'
API_RESPONSE = {'answer': 42}
SWAGGER_SPEC_DICT = {
    'swagger': '2.0',
    'info': {'version': '1.0.0', 'title': 'Integration tests'},
    'definitions': {
        'api_response': {
            'properties': {
                'answer': {
                    'type': 'integer'
                },
            },
            'required': ['answer'],
            'type': 'object',
            'x-model': 'api_response',
            'title': 'api_response',
        }
    },
    'basePath': '/',
    'paths': {
        '/json': {
            'get': {
                'produces': ['application/json'],
                'responses': {
                    '200': {
                        'description': 'HTTP/200',
                        'schema': {'$ref': '#/definitions/api_response'},
                    },
                },
            },
        },
        '/json_or_msgpack': {
            'get': {
                'produces': [
                    'application/msgpack',
                    'application/json'
                ],
                'responses': {
                    '200': {
                        'description': 'HTTP/200',
                        'schema': {'$ref': '#/definitions/api_response'},
                    }
                }
            }
        },
        '/echo': {
            'get': {
                'produces': ['application/json'],
                'parameters': [
                    {
                        'in': 'query',
                        'name': 'message',
                        'type': 'string',
                        'required': True,
                    }
                ],
                'responses': {
                    '200': {
                        'description': 'HTTP/200',
                        'schema': {
                            'type': 'object',
                            'properties': {
                                'message': {
                                    'type': 'string',
                                },
                            },
                            'required': ['message'],
                        },
                    },
                },
            },
        },
        '/char_test/{special}': {
            'get': {
                'operationId': 'get_char_test',
                'produces': ['application/json'],
                'parameters': [
                    {
                        'in': 'path',
                        'name': 'special',
                        'type': 'string',
                        'required': True,
                    }
                ],
                'responses': {
                    '200': {
                        'description': 'HTTP/200',
                        'schema': {'$ref': '#/definitions/api_response'},
                    },
                },
            },
        },
        '/sanitize-test': {
            'get': {
                'operationId': 'get_sanitized_param',
                'produces': ['application/json'],
                'parameters': [
                    {
                        'in': 'header',
                        'name': 'X-User-Id',
                        'type': 'string',
                        'required': True,
                    }
                ],
                'responses': {
                    '200': {
                        'description': 'HTTP/200',
                        'schema': {'$ref': '#/definitions/api_response'},
                    },
                },
            },
        },
        '/sleep': {
            'get': {
                'operationId': 'sleep',
                'produces': ['application/json'],
                'parameters': [
                    {
                        'in': 'query',
                        'name': 'sec',
                        'type': 'number',
                        'format': 'float',
                        'required': True,
                    }
                ],
                'responses': {
                    '200': {
                        'description': 'HTTP/200',
                        'schema': {'$ref': '#/definitions/api_response'},
                    },
                },
            },
        },
        '/redirect': {
            'get': {
                'operationId': 'redirect_test',
                'produces': ['application/json'],
                'responses': {
                    '301': {
                        'description': 'HTTP/301',
                    },
                },
            },
        },
    },
}


@bottle.get('/swagger.json')
def swagger_spec():
    return SWAGGER_SPEC_DICT


@bottle.get('/json')
def api_json():
    return API_RESPONSE


@bottle.route('/json_or_msgpack')
def api_json_or_msgpack():
    if bottle.request.headers.get('accept') == APP_MSGPACK:
        bottle.response.content_type = APP_MSGPACK
        return packb(API_RESPONSE)
    else:
        return API_RESPONSE


@bottle.route('/1')
def one():
    return ROUTE_1_RESPONSE


@bottle.route('/2')
def two():
    return ROUTE_2_RESPONSE


@bottle.post('/double')
def double():
    x = bottle.request.params['number']
    return str(int(x) * 2)


@bottle.route('/headers')
def headers():
    return {
        header_name: {
            'type': _class_fqn(type(header_value)),
            'representation': repr(header_value),
        }
        for header_name, header_value in bottle.request.headers.items()
        if header_name.startswith('Header-')
    }


@bottle.get('/echo')
def echo():
    return {'message': bottle.request.params['message']}


@bottle.get('/char_test/spe%ial?')
def char_test():
    return API_RESPONSE


@bottle.get('/sanitize-test')
def sanitize_test():
    if bottle.request.headers.get('X-User-Id') == 'admin':
        return API_RESPONSE
    return bottle.HTTPResponse(status=404)


@bottle.get('/sleep')
def sleep_api():
    sec_to_sleep = float(bottle.request.GET.get('sec', '1'))
    time.sleep(sec_to_sleep)
    return sec_to_sleep


@bottle.get('/redirect')
def redirect_test():
    return bottle.HTTPResponse(
        status=301,
        headers={'Location': '/json'},
    )


def run_bottle_server(port):
    """Wrapper function for bottle.run so that the child Python interpreter finds the bottle routes on Windows."""
    bottle.run(quiet=True, host='127.0.0.1', port=port)


class IntegrationTestingServicesAndClient:
    @pytest.fixture(scope='session')
    def swagger_http_server(self):
        def wait_unit_service_starts(url, max_wait_time=10):
            start = time.time()
            check_url = '{url}/swagger.json'.format(url=url)
            while time.time() < start + max_wait_time:
                try:
                    requests.get(check_url, timeout=1)
                except requests.ConnectionError:
                    time.sleep(0.1)
                else:
                    return

        port = ephemeral_port_reserve.reserve()

        web_service_process = Process(
            target=run_bottle_server,
            kwargs={'port': port},
        )
        try:
            web_service_process.start()
            server_address = 'http://127.0.0.1:{port}'.format(port=port)
            wait_unit_service_starts(server_address, 10)
            yield server_address
        finally:
            web_service_process.terminate()

    @pytest.fixture(scope='session')
    def not_answering_http_server(self):
        yield 'http://127.0.0.1:{}'.format(ephemeral_port_reserve.reserve())

    @pytest.fixture(params=['result', 'response'])
    def result_getter(self, request):
        if request.param == 'result':
            return lambda future, timeout: future.result(timeout)
        elif request.param == 'response':
            def _response_getter(future, timeout):
                response = future.response(timeout)
                return response.result, response.incoming_response

            return _response_getter

        raise ValueError  # pragma: no cover


class IntegrationTestingFixturesMixin(IntegrationTestingServicesAndClient):
    """
    Generic class to run integration tests with the different HTTP clients definitions
    """

    http_client = None   # type: HttpClient
    http_client_type = None  # type: typing.Type[HttpClient]
    http_future_adapter_type = None  # type: typing.Type[FutureAdapter]
    connection_errors_exceptions = None  # type: typing.Set[Exception]

    @classmethod
    def setup_class(cls):
        if cls.http_client_type is None:
            raise RuntimeError(  # pragma: no cover
                'Define http_client_type for {}'.format(cls.__name__)
            )
        if cls.http_future_adapter_type is None:
            raise RuntimeError(  # pragma: no cover
                'Define http_future_adapter_type for {}'.format(cls.__name__)
            )
        if cls.connection_errors_exceptions is None:
            raise RuntimeError(  # pragma: no cover
                'Define connection_errors_exceptions for {}'.format(
                    cls.__name__,
                ),
            )
        cls.http_client = cls.http_client_type()

    @pytest.fixture
    def swagger_client(self, swagger_http_server):
        return SwaggerClient.from_url(
            spec_url='{server_address}/swagger.json'.format(
                server_address=swagger_http_server),
            http_client=self.http_client,
            config={'use_models': False, 'also_return_response': True}
        )

    @classmethod
    def encode_expected_response(cls, response):
        if isinstance(response, bytes):
            return response.decode('utf-8')
        else:
            return str(response)  # pragma: no cover


class IntegrationTestsBaseClass(IntegrationTestingFixturesMixin):
    def test_fetch_specs(self, swagger_http_server):
        loader = Loader(
            http_client=self.http_client,
            request_headers={'boolean-header': True},
        )
        spec = loader.load_spec('{server_address}/swagger.json'.format(server_address=swagger_http_server))
        assert spec == SWAGGER_SPEC_DICT

    def test_swagger_client_json_response(self, swagger_client, result_getter):
        marshaled_response, raw_response = result_getter(swagger_client.json.get_json(), timeout=1)
        assert marshaled_response == API_RESPONSE
        assert raw_response.raw_bytes == json.dumps(API_RESPONSE).encode('utf-8')

    def test_swagger_client_msgpack_response_without_flag(self, swagger_client, result_getter):
        marshaled_response, raw_response = result_getter(
            swagger_client.json_or_msgpack.get_json_or_msgpack(),
            timeout=1,
        )
        assert marshaled_response == API_RESPONSE
        assert raw_response.raw_bytes == json.dumps(API_RESPONSE).encode('utf-8')

    def test_swagger_client_msgpack_response_with_flag(self, swagger_client, result_getter):
        marshaled_response, raw_response = result_getter(
            swagger_client.json_or_msgpack.get_json_or_msgpack(
                _request_options={
                    'use_msgpack': True,
                },
            ),
            timeout=1,
        )
        assert marshaled_response == API_RESPONSE
        assert raw_response.raw_bytes == packb(API_RESPONSE)

    def test_swagger_client_special_chars_query(self, swagger_client, result_getter):
        message = 'My Me$$age with %pecial characters?"'
        marshaled_response, _ = result_getter(swagger_client.echo.get_echo(message=message), timeout=1)
        assert marshaled_response == {'message': message}

    def test_swagger_client_special_chars_path(self, swagger_client, result_getter):
        marshaled_response, _ = result_getter(swagger_client.char_test.get_char_test(special='spe%ial?'), timeout=1)
        assert marshaled_response == API_RESPONSE

    def test_sanitized_resource_and_param(self, swagger_client, result_getter):
        marshaled_response, _ = result_getter(
            swagger_client.sanitize_test.get_sanitized_param(X_User_Id='admin'),
            timeout=1,
        )
        assert marshaled_response == API_RESPONSE

    def test_unsanitized_resource_and_param(self, swagger_client, result_getter):
        params = {'X-User-Id': 'admin'}
        marshaled_response, _ = result_getter(
            swagger_client._get_resource(
                'sanitize-test',
            ).get_sanitized_param(**params),
            timeout=1,
        )
        assert marshaled_response == API_RESPONSE

    def test_unsanitized_param_as_header(self, swagger_client, result_getter):
        headers = {'X-User-Id': 'admin'}
        marshaled_response, _ = result_getter(swagger_client.sanitize_test.get_sanitized_param(
            _request_options={'headers': headers},
        ), timeout=1)
        assert marshaled_response == API_RESPONSE

    def test_multiple_requests(self, swagger_http_server):
        request_one_params = {
            'method': 'GET',
            'headers': {},
            'url': '{server_address}/1'.format(server_address=swagger_http_server),
            'params': {},
        }

        request_two_params = {
            'method': 'GET',
            'headers': {},
            'url': '{server_address}/2'.format(server_address=swagger_http_server),
            'params': {},
        }

        http_future_1 = self.http_client.request(request_one_params)
        http_future_2 = self.http_client.request(request_two_params)
        resp_one = typing.cast(IncomingResponse, http_future_1.result(timeout=1))
        resp_two = typing.cast(IncomingResponse, http_future_2.result(timeout=1))

        assert resp_one.text == self.encode_expected_response(ROUTE_1_RESPONSE)
        assert resp_two.text == self.encode_expected_response(ROUTE_2_RESPONSE)

    def test_post_request(self, swagger_http_server):

        request_args = {
            'method': 'POST',
            'headers': {},
            'url': '{server_address}/double'.format(server_address=swagger_http_server),
            'data': {"number": 3},
        }

        http_future = self.http_client.request(request_args)
        resp = typing.cast(IncomingResponse, http_future.result(timeout=1))

        assert resp.text == self.encode_expected_response(b'6')

    def test_non_string_headers(self, swagger_http_server):
        headers = {
            'Header-Boolean': True,
            'Header-Integer': 1,
            'Header-Bytes': b'0',
        }
        response = typing.cast(IncomingResponse, self.http_client.request({
            'method': 'GET',
            'headers': headers,
            'url': '{server_address}/headers'.format(server_address=swagger_http_server),
            'params': {},
        }).result(timeout=1))

        expected_header_representations = {
            'Header-Boolean': repr('True'),
            'Header-Integer': repr('1'),
            'Header-Bytes': repr('0'),
        }
        assert {
            header_name: {
                'type': _class_fqn(str),
                'representation': expected_header_representations[header_name]
            }
            for header_name, header_value in headers.items()
        } == response.json()

    def test_msgpack_support(self, swagger_http_server):
        response = typing.cast(IncomingResponse, self.http_client.request({
            'method': 'GET',
            'url': '{server_address}/json_or_msgpack'.format(server_address=swagger_http_server),
            'params': {},
            'headers': {
                'Accept': APP_MSGPACK,
            },
        }).result(timeout=1))

        assert response.headers['Content-Type'] == APP_MSGPACK
        assert unpackb(response.raw_bytes) == API_RESPONSE

    def test_following_redirects(self, swagger_http_server):
        try:
            # if running the default tests, the fido dependencies aren't loaded
            # so rather than load dependencies unnecessarily, skip the test if that's not the case
            # the coverage test incorrectly marks the exception handling as uncovered
            # hence the pragma usage
            from bravado.fido_client import FidoClient
        except ImportError:  # pragma: no cover
            pytest.skip('Fido dependencies have not been loaded, skipping test')  # pragma: no cover
        else:
            # the FidoClient doesn't have a way to turn off redirects being followed
            # so limit this test to the RequestsClient instead
            if isinstance(self.http_client, FidoClient):
                pytest.skip('Following redirects is not supported in the FidoClient')

        response = self.http_client.request({
            'method': 'GET',
            'url': '{server_address}/redirect'.format(server_address=swagger_http_server),
            'params': {},
            'follow_redirects': True,
        }).result(timeout=1)

        assert isinstance(response, IncomingResponse) and response.status_code == 200

    def test_redirects_are_not_followed(self, swagger_http_server):
        with pytest.raises(HTTPMovedPermanently) as excinfo:
            self.http_client.request({
                'method': 'GET',
                'url': '{server_address}/redirect'.format(server_address=swagger_http_server),
                'params': {},
            }).result(timeout=1)

        exc = excinfo.value

        assert isinstance(exc.response, IncomingResponse) and exc.response.status_code == 301
        assert isinstance(exc.response, IncomingResponse) and exc.response.headers['Location'] == '/json'

    def test_timeout_errors_are_thrown_as_BravadoTimeoutError(self, swagger_http_server):
        if not self.http_future_adapter_type.timeout_errors:
            pytest.skip('{} does NOT defines timeout_errors'.format(self.http_future_adapter_type))

        with pytest.raises(BravadoTimeoutError):
            self.http_client.request({
                'method': 'GET',
                'url': '{server_address}/sleep?sec=0.1'.format(server_address=swagger_http_server),
                'params': {},
            }).result(timeout=0.01)

    def test_request_timeout_errors_are_thrown_as_BravadoTimeoutError(self, swagger_http_server):
        if not self.http_future_adapter_type.timeout_errors:
            pytest.skip('{} does NOT defines timeout_errors'.format(self.http_future_adapter_type))

        with pytest.raises(BravadoTimeoutError):
            self.http_client.request({
                'method': 'GET',
                'url': '{server_address}/sleep?sec=0.1'.format(server_address=swagger_http_server),
                'params': {},
                'timeout': 0.01,
            }).result()

    def test_swagger_client_timeout_errors_are_thrown_as_BravadoTimeoutError(
        self, swagger_client, result_getter,
    ):
        if not self.http_future_adapter_type.timeout_errors:
            pytest.skip('{} does NOT defines timeout_errors'.format(self.http_future_adapter_type))

        with pytest.raises(BravadoTimeoutError):
            result_getter(
                swagger_client.sleep.sleep(sec=0.5),
                timeout=0.1,
            )

    def test_timeout_errors_are_catchable_with_original_exception_types(self, swagger_http_server):
        if not self.http_future_adapter_type.timeout_errors:
            pytest.skip('{} does NOT defines timeout_errors'.format(self.http_future_adapter_type))

        for expected_exception in self.http_future_adapter_type.timeout_errors:
            with pytest.raises(expected_exception) as excinfo:
                self.http_client.request({
                    'method': 'GET',
                    'url': '{server_address}/sleep?sec=0.1'.format(server_address=swagger_http_server),
                    'params': {},
                }).result(timeout=0.01)
            assert isinstance(excinfo.value, BravadoTimeoutError)

    def test_connection_errors_are_thrown_as_BravadoConnectionError(self, not_answering_http_server):
        if not self.http_future_adapter_type.connection_errors:
            pytest.skip('{} does NOT defines connection_errors'.format(self.http_future_adapter_type))

        with pytest.raises(BravadoConnectionError):
            self.http_client.request({
                'method': 'GET',
                'url': '{server_address}/sleep?sec=0.1'.format(server_address=not_answering_http_server),
                'params': {},
                'connect_timeout': 0.001,
                'timeout': 0.01,
            }).result(timeout=1)

    def test_connection_errors_exceptions_contains_all_future_adapter_connection_errors(self):
        assert set(
            type(e) for e in self.connection_errors_exceptions
        ) == set(self.http_future_adapter_type.connection_errors)

    def test_connection_errors_are_catchable_with_original_exception_types(
        self, not_answering_http_server,
    ):
        for expected_exception in self.connection_errors_exceptions:
            with pytest.raises(type(expected_exception)) as excinfo:
                http_future = self.http_client.request({
                    'method': 'GET',
                    'url': not_answering_http_server,
                    'params': {},
                })
                http_future.cancel()

                # Finding a way to force all the http clients to raise
                # the expected exception while sending the real request is hard
                # so we're mocking the future in order to throw the expected
                # exception so we can validate that the exception is catchable
                # with the original exception type too
                def raise_expected_exception(*args, **kwargs):
                    raise expected_exception
                http_future.future.result = raise_expected_exception  # type: ignore

                http_future.result(timeout=0.1)

            # check that the raised exception is catchable as BravadoConnectionError too
            assert isinstance(excinfo.value, BravadoConnectionError)

    def test_swagger_client_connection_errors_are_thrown_as_BravadoConnectionError(
        self, not_answering_http_server, swagger_client, result_getter,
    ):
        if not self.http_future_adapter_type.connection_errors:
            pytest.skip('{} does NOT defines connection_errors'.format(self.http_future_adapter_type))

        # override api url to communicate with a non responding http server
        swagger_client.swagger_spec.api_url = not_answering_http_server
        with pytest.raises(BravadoConnectionError):
            result_getter(
                swagger_client.json.get_json(_request_options={
                    'connect_timeout': 0.001,
                    'timeout': 0.01,
                }),
                timeout=0.1,
            )
