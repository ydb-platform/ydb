"""
django-simple-history exceptions and warnings classes.
"""


class MultipleRegistrationsError(Exception):
    """The model has been registered to have history tracking more than once"""

    pass


class NotHistoricalModelError(TypeError):
    """No related history model found."""

    pass


class RelatedNameConflictError(Exception):
    """Related name conflicting with history manager"""

    pass


class AlternativeManagerError(Exception):
    """Manager does not belong to model"""

    pass
