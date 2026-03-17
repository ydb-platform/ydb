import attr

from openapi_core.exceptions import OpenAPIError


class UnmarshalError(OpenAPIError):
    """Schema unmarshal operation error"""
    pass


class ValidateError(UnmarshalError):
    """Schema validate operation error"""
    pass


class UnmarshallerError(UnmarshalError):
    """Unmarshaller error"""
    pass


@attr.s(hash=True)
class InvalidSchemaValue(ValidateError):
    value = attr.ib()
    type = attr.ib()
    schema_errors = attr.ib(factory=tuple)

    def __str__(self):
        return (
            "Value {value} not valid for schema of type {type}: {errors}"
        ).format(value=self.value, type=self.type, errors=self.schema_errors)


@attr.s(hash=True)
class InvalidSchemaFormatValue(UnmarshallerError):
    """Value failed to format with formatter"""
    value = attr.ib()
    type = attr.ib()
    original_exception = attr.ib()

    def __str__(self):
        return (
            "Failed to format value {value} to format {type}: {exception}"
        ).format(
            value=self.value, type=self.type,
            exception=self.original_exception,
        )


@attr.s(hash=True)
class FormatterNotFoundError(UnmarshallerError):
    """Formatter not found to unmarshal"""
    type_format = attr.ib()

    def __str__(self):
        return "Formatter not found for {format} format".format(
            format=self.type_format)
