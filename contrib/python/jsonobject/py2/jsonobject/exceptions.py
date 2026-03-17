from __future__ import unicode_literals
class DeleteNotAllowed(Exception):
    pass


class BadValueError(Exception):
    """raised when a value can't be validated or is required"""


class WrappingAttributeError(AttributeError):
    pass
