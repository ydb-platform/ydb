from django.http import HttpResponse

from .codecs import _OpenAPICodec
from .errors import SwaggerValidationError


class SwaggerExceptionMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        return self.get_response(request)

    def process_exception(self, request, exception):
        if isinstance(exception, SwaggerValidationError):
            err = {"errors": exception.errors, "message": str(exception)}
            codec = exception.source_codec
            if isinstance(codec, _OpenAPICodec):
                err = codec.encode_error(err)
                content_type = codec.media_type
                return HttpResponse(err, status=500, content_type=content_type)

        return None  # pragma: no cover
