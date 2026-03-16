import functools
import operator
import uuid
from decimal import Decimal
from typing import Any, Optional

import django
import orjson


if django.VERSION < (5, 0):
    from django.db.models.enums import ChoicesMeta as ChoicesType
else:
    from django.db.models.enums import ChoicesType

from django.utils.functional import Promise
from rest_framework.renderers import BaseRenderer
from rest_framework.settings import api_settings


__all__ = ["ORJSONRenderer"]


class ORJSONRenderer(BaseRenderer):
    """
    Renderer which serializes to JSON.
    Uses the Rust-backed orjson library for serialization speed.
    """

    format: str = "json"
    html_media_type: str = "text/html"
    json_media_type: str = "application/json"
    media_type: str = json_media_type

    # We don't set a charset because JSON is a binary encoding,
    # that can be encoded as utf-8, utf-16 or utf-32.
    # See: https://www.ietf.org/rfc/rfc4627.txt
    # Also: http://lucumr.pocoo.org/2013/7/19/application-mimetypes-and-encodings/
    charset: Optional[str] = None

    options = functools.reduce(
        operator.or_,
        api_settings.user_settings.get("ORJSON_RENDERER_OPTIONS", ()),
        orjson.OPT_SERIALIZE_NUMPY,
    )

    @staticmethod
    def default(obj: Any) -> Any:
        """
        When orjson doesn't recognize an object type for serialization it passes
        that object to this function which then converts the object to its
        native Python equivalent.

        :param obj: Object of any type to be converted.
        :return: native python object
        """

        if isinstance(obj, dict):
            return dict(obj)
        elif isinstance(obj, list):
            return list(obj)
        elif isinstance(obj, Decimal):
            if api_settings.COERCE_DECIMAL_TO_STRING:
                return str(obj)
            else:
                return float(obj)
        elif isinstance(obj, (str, uuid.UUID, Promise, ChoicesType)):
            return str(obj)
        elif hasattr(obj, "tolist"):
            return obj.tolist()
        elif hasattr(obj, "__iter__"):
            return list(item for item in obj)

    def render(
        self,
        data: Any,
        media_type: Optional[str] = None,
        renderer_context: Any = None,
    ) -> bytes:
        """
        Serializes Python objects to JSON.

        :param data: The response data, as set by the Response() instantiation.
        :param media_type: If provided, this is the accepted media type, of the
                `Accept` HTTP header.
        :param renderer_context: If provided, this is a dictionary of contextual
                information provided by the view. By default this will include
                the following keys: view, request, response, args, kwargs
        :return: bytes() representation of the data encoded to UTF-8
        """
        if data is None:
            return b""

        renderer_context = renderer_context or {}

        # By default, this function will use its own version of `default()` in
        # order to safely serialize known Django types like QuerySets. If you
        # know you won't need this you can pass `None` to the renderer_context
        # ORJSON will only serialize native Python built-in types. If you know
        # that you need to serialize additional types such as Numpy you can
        # override the default here.
        #
        # Instead of the full if-else, the temptation here is to optimize
        # this block by calling:
        #
        # `default = renderer_context.get("default_function", self.default)`
        #
        # Don't do that here because you will lose the ability to pass `None`
        # to ORJSON.
        if "default_function" not in renderer_context:
            default = self.default
        else:
            default = renderer_context["default_function"]

        # If `indent` is provided in the context, then pretty print the result.
        # E.g. If we're being called by RestFramework's BrowsableAPIRenderer.
        options = self.options
        if media_type and self.html_media_type in media_type:
            options |= orjson.OPT_INDENT_2

        serialized: bytes = orjson.dumps(data, default=default, option=options)
        return serialized
