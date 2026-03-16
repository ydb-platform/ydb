""" Exceptions used with GeoAlchemy2.
"""


class GeoAlchemyError(Exception):
    """ Generic error class. """


class ArgumentError(GeoAlchemyError):
    """ Raised when an invalid or conflicting function argument is
    supplied.
    """
