"""
flask_marshmallow.fields
~~~~~~~~~~~~~~~~~~~~~~~~

Custom, Flask-specific fields.

See the `marshmallow.fields` module for the list of all fields available from the
marshmallow library.
"""

from __future__ import annotations

import re
import typing
from collections.abc import Sequence

from flask import current_app, url_for
from marshmallow import fields, missing

__all__ = [
    "URLFor",
    "UrlFor",
    "AbsoluteURLFor",
    "AbsoluteUrlFor",
    "Hyperlinks",
    "File",
    "Config",
]


_tpl_pattern = re.compile(r"\s*<\s*(\S*)\s*>\s*")


def _tpl(val: str) -> str | None:
    """Return value within ``< >`` if possible, else return ``None``."""
    match = _tpl_pattern.match(val)
    if match:
        return match.groups()[0]
    return None


def _get_value(obj, key, default=missing):
    """Slightly-modified version of marshmallow.utils.get_value.
    If a dot-delimited ``key`` is passed and any attribute in the
    path is `None`, return `None`.
    """
    if "." in key:
        return _get_value_for_keys(obj, key.split("."), default)
    else:
        return _get_value_for_key(obj, key, default)


def _get_value_for_keys(obj, keys, default):
    if len(keys) == 1:
        return _get_value_for_key(obj, keys[0], default)
    else:
        value = _get_value_for_key(obj, keys[0], default)
        # XXX This differs from the marshmallow implementation
        if value is None:
            return None
        return _get_value_for_keys(value, keys[1:], default)


def _get_value_for_key(obj, key, default):
    if not hasattr(obj, "__getitem__"):
        return getattr(obj, key, default)

    try:
        return obj[key]
    except (KeyError, IndexError, TypeError, AttributeError):
        return getattr(obj, key, default)


class URLFor(fields.Field):
    """Field that outputs the URL for an endpoint. Acts identically to
    Flask's ``url_for`` function, except that arguments can be pulled from the
    object to be serialized, and ``**values`` should be passed to the ``values``
    parameter.

    Usage: ::

        url = URLFor("author_get", values=dict(id="<id>"))
        https_url = URLFor(
            "author_get",
            values=dict(id="<id>", _scheme="https", _external=True),
        )

    :param str endpoint: Flask endpoint name.
    :param dict values: Same keyword arguments as Flask's url_for, except string
        arguments enclosed in `< >` will be interpreted as attributes to pull
        from the object.
    :param kwargs: keyword arguments to pass to marshmallow field (e.g. ``required``).
    """

    _CHECK_ATTRIBUTE = False

    def __init__(
        self,
        endpoint: str,
        values: dict[str, typing.Any] | None = None,
        **kwargs,
    ):
        self.endpoint = endpoint
        self.values = values or {}
        fields.Field.__init__(self, **kwargs)

    def _serialize(self, value, key, obj):
        """Output the URL for the endpoint, given the kwargs passed to
        ``__init__``.
        """
        param_values = {}
        for name, attr_tpl in self.values.items():
            attr_name = _tpl(str(attr_tpl))
            if attr_name:
                attribute_value = _get_value(obj, attr_name, default=missing)
                if attribute_value is None:
                    return None
                if attribute_value is not missing:
                    param_values[name] = attribute_value
                else:
                    raise AttributeError(
                        f"{attr_name!r} is not a valid attribute of {obj!r}"
                    )
            else:
                param_values[name] = attr_tpl
        return url_for(self.endpoint, **param_values)


UrlFor = URLFor


class AbsoluteURLFor(URLFor):
    """Field that outputs the absolute URL for an endpoint."""

    def __init__(
        self,
        endpoint: str,
        values: dict[str, typing.Any] | None = None,
        **kwargs,
    ):
        if values:
            values["_external"] = True
        else:
            values = {"_external": True}
        URLFor.__init__(self, endpoint=endpoint, values=values, **kwargs)


AbsoluteUrlFor = AbsoluteURLFor


def _rapply(d: dict | typing.Iterable, func: typing.Callable, *args, **kwargs):
    """Apply a function to all values in a dictionary or
    list of dictionaries, recursively.
    """
    if isinstance(d, (tuple, list)):
        return [_rapply(each, func, *args, **kwargs) for each in d]
    if isinstance(d, dict):
        return {key: _rapply(value, func, *args, **kwargs) for key, value in d.items()}
    else:
        return func(d, *args, **kwargs)


def _url_val(val: typing.Any, key: str, obj: typing.Any, **kwargs):
    """Function applied by `HyperlinksField` to get the correct value in the
    schema.
    """
    if isinstance(val, URLFor):
        return val.serialize(key, obj, **kwargs)
    else:
        return val


class Hyperlinks(fields.Field):
    """Field that outputs a dictionary of hyperlinks,
    given a dictionary schema with :class:`~flask_marshmallow.fields.URLFor`
    objects as values.

    Example: ::

        _links = Hyperlinks(
            {
                "self": URLFor("author", values=dict(id="<id>")),
                "collection": URLFor("author_list"),
            }
        )

    `URLFor` objects can be nested within the dictionary. ::

        _links = Hyperlinks(
            {
                "self": {
                    "href": URLFor("book", values=dict(id="<id>")),
                    "title": "book detail",
                }
            }
        )

    :param dict schema: A dict that maps names to
        :class:`~flask_marshmallow.fields.URLFor` fields.
    """

    _CHECK_ATTRIBUTE = False

    def __init__(self, schema: dict[str, URLFor | str], **kwargs):
        self.schema = schema
        fields.Field.__init__(self, **kwargs)

    def _serialize(self, value, attr, obj):
        return _rapply(self.schema, _url_val, key=attr, obj=obj)


class File(fields.Field):
    """A binary file field for uploaded files.

    Examples: ::

        class ImageSchema(Schema):
            image = File(required=True)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Metadata used by apispec
        self.metadata["type"] = "string"
        self.metadata["format"] = "binary"

    default_error_messages = {"invalid": "Not a valid file."}

    def deserialize(
        self,
        value: typing.Any,
        attr: str | None = None,
        data: typing.Mapping[str, typing.Any] | None = None,
        **kwargs,
    ):
        if isinstance(value, Sequence) and len(value) == 0:
            value = missing
        return super().deserialize(value, attr, data, **kwargs)

    def _deserialize(self, value, attr, data, **kwargs):
        from werkzeug.datastructures import FileStorage

        if not isinstance(value, FileStorage):
            raise self.make_error("invalid")
        return value


class Config(fields.Field):
    """A field for Flask configuration values.

    Examples: ::

        from flask import Flask

        app = Flask(__name__)
        app.config["API_TITLE"] = "Pet API"


        class FooSchema(Schema):
            user = String()
            title = Config("API_TITLE")

    This field should only be used in an output schema. A ``ValueError`` will
    be raised if the config key is not found in the app config.

    :param str key: The key of the configuration value.
    """

    _CHECK_ATTRIBUTE = False

    def __init__(self, key: str, **kwargs):
        fields.Field.__init__(self, **kwargs)
        self.key = key

    def _serialize(self, value, attr, obj, **kwargs):
        if self.key not in current_app.config:
            raise ValueError(f"The key {self.key!r} is not found in the app config.")
        return current_app.config[self.key]
