
class UwsgiconfException(Exception):
    """Base for exceptions."""


class ConfigurationError(UwsgiconfException):
    """Configuration related error."""


class RuntimeConfigurationError(ConfigurationError):
    """Runtime configuration related error."""
