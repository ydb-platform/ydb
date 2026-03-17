from typing import Any, Optional

import orjson
from django.conf import settings
from rest_framework.exceptions import ParseError
from rest_framework.parsers import BaseParser


__all__ = ["ORJSONParser"]


class ORJSONParser(BaseParser):
    """
    Parses JSON-serialized data by orjson parser.
    """

    media_type: str = "application/json"

    def parse(
        self,
        stream,
        media_type: Optional[Any] = None,
        parser_context: Any = None,
    ) -> Any:
        """
        De-serializes JSON strings to Python objects.

        :param stream: A stream-like object representing the body of the request.
        :param media_type: If provided, this is the media type of the incoming
                request content specified in the `Content-Type` HTTP header.
        :param parser_context: If supplied, this argument will be a dictionary
                containing any additional context that may be required to parse
                the request content.

                By default this will include the following
                keys: view, request, args, kwargs.
        :return: Python native instance of the JSON string.
        """
        parser_context = parser_context or {}
        encoding: str = parser_context.get("encoding", settings.DEFAULT_CHARSET)

        try:
            data: Any = stream.read().decode(encoding)
            return orjson.loads(data)
        except ValueError as exc:
            raise ParseError(f"JSON parse error - {exc}")
