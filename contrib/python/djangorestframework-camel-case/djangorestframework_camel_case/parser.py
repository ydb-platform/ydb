import json

from django.conf import settings
from django.http.multipartparser import (
    MultiPartParser as DjangoMultiPartParser,
    MultiPartParserError,
)
from rest_framework.exceptions import ParseError
from rest_framework.parsers import MultiPartParser, DataAndFiles
from rest_framework.parsers import FormParser

from djangorestframework_camel_case.settings import api_settings
from djangorestframework_camel_case.util import underscoreize


class CamelCaseJSONParser(api_settings.PARSER_CLASS):
    json_underscoreize = api_settings.JSON_UNDERSCOREIZE

    def parse(self, stream, media_type=None, parser_context=None):
        parser_context = parser_context or {}
        encoding = parser_context.get("encoding", settings.DEFAULT_CHARSET)

        try:
            data = stream.read().decode(encoding)
            return underscoreize(json.loads(data), **self.json_underscoreize)
        except ValueError as exc:
            raise ParseError("JSON parse error - %s" % str(exc))


class CamelCaseFormParser(FormParser):
    """
    Parser for form data.
    """

    def parse(self, stream, media_type=None, parser_context=None):
        return underscoreize(
            super().parse(stream, media_type, parser_context),
            **api_settings.JSON_UNDERSCOREIZE,
        )


class CamelCaseMultiPartParser(MultiPartParser):
    """
    Parser for multipart form data, which may include file data.
    """

    media_type = "multipart/form-data"

    def parse(self, stream, media_type=None, parser_context=None):
        """
        Parses the incoming bytestream as a multipart encoded form,
        and returns a DataAndFiles object.

        `.data` will be a `QueryDict` containing all the form parameters.
        `.files` will be a `QueryDict` containing all the form files.
        """
        parser_context = parser_context or {}
        request = parser_context["request"]
        encoding = parser_context.get("encoding", settings.DEFAULT_CHARSET)
        meta = request.META.copy()
        meta["CONTENT_TYPE"] = media_type
        upload_handlers = request.upload_handlers

        try:
            parser = DjangoMultiPartParser(meta, stream, upload_handlers, encoding)
            data, files = parser.parse()
            return DataAndFiles(
                underscoreize(data, **api_settings.JSON_UNDERSCOREIZE),
                underscoreize(files, **api_settings.JSON_UNDERSCOREIZE),
            )
        except MultiPartParserError as exc:
            raise ParseError("Multipart form parse error - %s" % str(exc))
