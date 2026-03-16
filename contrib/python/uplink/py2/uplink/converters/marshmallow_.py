"""
This module defines a converter that uses :py:mod:`marshmallow` schemas
to deserialize and serialize values.
"""

# Local imports
from uplink import utils
from uplink.converters import interfaces, register_default_converter_factory


class MarshmallowConverter(interfaces.Factory):
    """
    A converter that serializes and deserializes values using
    :py:mod:`marshmallow` schemas.

    To deserialize JSON responses into Python objects with this
    converter, define a :py:class:`marshmallow.Schema` subclass and set
    it as the return annotation of a consumer method:

    .. code-block:: python

        @get("/users")
        def get_users(self, username) -> UserSchema():
            '''Fetch a single user'''

    Note:

        This converter is an optional feature and requires the
        :py:mod:`marshmallow` package. For example, here's how to
        install this feature using pip::

            $ pip install uplink[marshmallow]
    """

    try:
        import marshmallow
    except ImportError:  # pragma: no cover
        marshmallow = None
        is_marshmallow_3 = None
    else:
        is_marshmallow_3 = marshmallow.__version__ >= "3.0"

    def __init__(self):
        if self.marshmallow is None:
            raise ImportError("No module named 'marshmallow'")

    class ResponseBodyConverter(interfaces.Converter):
        def __init__(self, extract_data, schema):
            self._extract_data = extract_data
            self._schema = schema

        def convert(self, response):
            try:
                json = response.json()
            except AttributeError:
                # Assume that the response is already json
                json = response

            return self._extract_data(self._schema.load(json))

    class RequestBodyConverter(interfaces.Converter):
        def __init__(self, extract_data, schema):
            self._extract_data = extract_data
            self._schema = schema

        def convert(self, value):
            return self._extract_data(self._schema.dump(value))

    @classmethod
    def _get_schema(cls, type_):
        if utils.is_subclass(type_, cls.marshmallow.Schema):
            return type_()
        elif isinstance(type_, cls.marshmallow.Schema):
            return type_
        raise ValueError("Expected marshmallow.Scheme subclass or instance.")

    def _extract_data(self, m):
        # After marshmallow 3.0, Schema.load() and Schema.dump() don't
        # return a (data, errors) tuple any more. Only `data` is returned.
        return m if self.is_marshmallow_3 else m.data

    def _make_converter(self, converter_cls, type_):
        try:
            # Try to generate schema instance from the given type.
            schema = self._get_schema(type_)
        except ValueError:
            # Failure: the given type is not a `marshmallow.Schema`.
            return None
        else:
            return converter_cls(self._extract_data, schema)

    def create_request_body_converter(self, type_, *args, **kwargs):
        return self._make_converter(self.RequestBodyConverter, type_)

    def create_response_body_converter(self, type_, *args, **kwargs):
        return self._make_converter(self.ResponseBodyConverter, type_)

    @classmethod
    def register_if_necessary(cls, register_func):
        if cls.marshmallow is not None:
            register_func(cls)


MarshmallowConverter.register_if_necessary(register_default_converter_factory)
