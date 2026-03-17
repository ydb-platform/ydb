"""OpenAPI core contrib flask decorators module"""
from openapi_core.contrib.flask.handlers import FlaskOpenAPIErrorsHandler
from openapi_core.contrib.flask.providers import FlaskRequestProvider
from openapi_core.contrib.flask.requests import FlaskOpenAPIRequestFactory
from openapi_core.contrib.flask.responses import FlaskOpenAPIResponseFactory
from openapi_core.validation.decorators import OpenAPIDecorator
from openapi_core.validation.request.validators import RequestValidator
from openapi_core.validation.response.validators import ResponseValidator


class FlaskOpenAPIViewDecorator(OpenAPIDecorator):

    def __init__(
            self,
            request_validator,
            response_validator,
            request_factory=FlaskOpenAPIRequestFactory,
            response_factory=FlaskOpenAPIResponseFactory,
            request_provider=FlaskRequestProvider,
            openapi_errors_handler=FlaskOpenAPIErrorsHandler,
    ):
        super(FlaskOpenAPIViewDecorator, self).__init__(
            request_validator, response_validator,
            request_factory, response_factory,
            request_provider, openapi_errors_handler,
        )

    def _handle_request_view(self, request_result, view, *args, **kwargs):
        request = self._get_request(*args, **kwargs)
        request.openapi = request_result
        return super(FlaskOpenAPIViewDecorator, self)._handle_request_view(
            request_result, view, *args, **kwargs)

    @classmethod
    def from_spec(
            cls,
            spec,
            request_factory=FlaskOpenAPIRequestFactory,
            response_factory=FlaskOpenAPIResponseFactory,
            request_provider=FlaskRequestProvider,
            openapi_errors_handler=FlaskOpenAPIErrorsHandler,
    ):
        request_validator = RequestValidator(spec)
        response_validator = ResponseValidator(spec)
        return cls(
            request_validator=request_validator,
            response_validator=response_validator,
            request_factory=request_factory,
            response_factory=response_factory,
            request_provider=request_provider,
            openapi_errors_handler=openapi_errors_handler,
        )
