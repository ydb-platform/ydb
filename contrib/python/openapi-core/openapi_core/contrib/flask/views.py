"""OpenAPI core contrib flask views module"""
from flask.views import MethodView

from openapi_core.contrib.flask.decorators import FlaskOpenAPIViewDecorator
from openapi_core.contrib.flask.handlers import FlaskOpenAPIErrorsHandler
from openapi_core.validation.request.validators import RequestValidator
from openapi_core.validation.response.validators import ResponseValidator


class FlaskOpenAPIView(MethodView):
    """Brings OpenAPI specification validation and unmarshalling for views."""

    openapi_errors_handler = FlaskOpenAPIErrorsHandler

    def __init__(self, spec):
        super(FlaskOpenAPIView, self).__init__()
        self.request_validator = RequestValidator(spec)
        self.response_validator = ResponseValidator(spec)

    def dispatch_request(self, *args, **kwargs):
        decorator = FlaskOpenAPIViewDecorator(
            request_validator=self.request_validator,
            response_validator=self.response_validator,
            openapi_errors_handler=self.openapi_errors_handler,
        )
        return decorator(super(FlaskOpenAPIView, self).dispatch_request)(
            *args, **kwargs)
