"""OpenAPI core contrib falcon middlewares module"""

from openapi_core.contrib.falcon.handlers import FalconOpenAPIErrorsHandler
from openapi_core.contrib.falcon.requests import FalconOpenAPIRequestFactory
from openapi_core.contrib.falcon.responses import FalconOpenAPIResponseFactory
from openapi_core.validation.processors import OpenAPIProcessor
from openapi_core.validation.request.validators import RequestValidator
from openapi_core.validation.response.validators import ResponseValidator


class FalconOpenAPIMiddleware(OpenAPIProcessor):

    def __init__(
            self,
            request_validator,
            response_validator,
            request_factory,
            response_factory,
            openapi_errors_handler,
    ):
        super(FalconOpenAPIMiddleware, self).__init__(
            request_validator, response_validator)
        self.request_factory = request_factory
        self.response_factory = response_factory
        self.openapi_errors_handler = openapi_errors_handler

    def process_request(self, req, resp):
        openapi_req = self._get_openapi_request(req)
        req_result = super(FalconOpenAPIMiddleware, self).process_request(
            openapi_req)
        if req_result.errors:
            return self._handle_request_errors(req, resp, req_result)
        req.openapi = req_result

    def process_response(self, req, resp, resource, req_succeeded):
        openapi_req = self._get_openapi_request(req)
        openapi_resp = self._get_openapi_response(resp)
        resp_result = super(FalconOpenAPIMiddleware, self).process_response(
            openapi_req, openapi_resp)
        if resp_result.errors:
            return self._handle_response_errors(req, resp, resp_result)

    def _handle_request_errors(self, req, resp, request_result):
        return self.openapi_errors_handler.handle(
            req, resp, request_result.errors)

    def _handle_response_errors(self, req, resp, response_result):
        return self.openapi_errors_handler.handle(
            req, resp, response_result.errors)

    def _get_openapi_request(self, request):
        return self.request_factory.create(request)

    def _get_openapi_response(self, response):
        return self.response_factory.create(response)

    @classmethod
    def from_spec(
            cls,
            spec,
            request_factory=FalconOpenAPIRequestFactory,
            response_factory=FalconOpenAPIResponseFactory,
            openapi_errors_handler=FalconOpenAPIErrorsHandler,
    ):
        request_validator = RequestValidator(spec)
        response_validator = ResponseValidator(spec)
        return cls(
            request_validator=request_validator,
            response_validator=response_validator,
            request_factory=request_factory,
            response_factory=response_factory,
            openapi_errors_handler=openapi_errors_handler,
        )
