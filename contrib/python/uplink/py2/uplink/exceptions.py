class Error(Exception):
    """Base exception for this package"""

    message = None

    def __str__(self):
        return str(self.message)


class UplinkBuilderError(Error):
    """Something went wrong while building a service."""

    message = "`%s`: %s"

    def __init__(self, class_name, definition_name, error):
        fullname = class_name + "." + definition_name
        self.message = self.message % (fullname, error)
        self.error = error


class InvalidRequestDefinition(Error):
    """Something went wrong when building the request definition."""


class AnnotationError(Error):
    """Something went wrong with an annotation."""
