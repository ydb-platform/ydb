class ProtobufValueError(ValueError):
    """Base class for value errors."""


class ProtobufTypeError(TypeError):
    """Base class for type errors."""


class IncorrectWireTypeError(ProtobufValueError):
    """Incorrect wire type in the stream."""


class UnexpectedWireTypeError(ProtobufValueError):
    """The wire type is correct, but a different one was expected."""


class IncorrectAnnotationError(ProtobufTypeError):
    """Something wrong with the attribute annotation."""


class UnsupportedAnnotationError(ProtobufTypeError):
    """The type annotation is not supported."""


class IncorrectValueError(ProtobufValueError):
    """Something's wrong with the field value."""
