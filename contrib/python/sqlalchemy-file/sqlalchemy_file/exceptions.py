class ValidationError(Exception):
    """Base class for ValidationError
    Parameters:
        key: Current Column key
        msg: Validation error message.
    """

    def __init__(self, key: str, msg: str):  # pragma: no cover
        super().__init__(f"{key}: {msg}")
        self.key = key
        self.msg = msg


class SizeValidationError(ValidationError):
    pass


class ContentTypeValidationError(ValidationError):
    pass


class InvalidImageError(ValidationError):
    pass


class DimensionValidationError(ValidationError):
    pass


class AspectRatioValidationError(ValidationError):
    pass
