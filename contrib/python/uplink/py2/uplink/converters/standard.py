# Local imports
from uplink.converters import interfaces, register_default_converter_factory


class StringConverter(interfaces.Converter):
    def convert(self, value):
        return str(value)


@register_default_converter_factory
class StandardConverter(interfaces.Factory):
    """
    The default converter, this class seeks to provide sane alternatives
    for (de)serialization when all else fails -- e.g., no other
    converters could handle a particular type.
    """

    def create_request_body_converter(self, cls, *args, **kwargs):
        if isinstance(cls, interfaces.Converter):
            return cls

    def create_response_body_converter(self, cls, *args, **kwargs):
        if isinstance(cls, interfaces.Converter):
            return cls

    def create_string_converter(self, cls, *args, **kwargs):
        if isinstance(cls, interfaces.Converter):
            return cls
        return StringConverter()
