class RevertError(Exception):

    """Exception thrown when something goes wrong with reverting a model."""


class RevisionManagementError(Exception):

    """Exception that is thrown when something goes wrong with revision managment."""


class RegistrationError(Exception):

    """Exception thrown when registration with django-reversion goes wrong."""
