"""This module contains all custom exceptions in the `cloudpathlib` library. All exceptions
subclass the [`CloudPathException` base exception][cloudpathlib.exceptions.CloudPathException] to
facilitate catching any exception from this library.
"""


class CloudPathException(Exception):
    """Base exception for all cloudpathlib custom exceptions."""


class AnyPathTypeError(CloudPathException, TypeError):
    pass


class ClientMismatchError(CloudPathException, ValueError):
    pass


class CloudPathFileExistsError(CloudPathException, FileExistsError):
    pass


class CloudPathNotExistsError(CloudPathException):
    pass


class CloudPathFileNotFoundError(CloudPathException, FileNotFoundError):
    pass


class CloudPathIsADirectoryError(CloudPathException, IsADirectoryError):
    pass


class CloudPathNotADirectoryError(CloudPathException, NotADirectoryError):
    pass


class CloudPathNotImplementedError(CloudPathException, NotImplementedError):
    pass


class DirectoryNotEmptyError(CloudPathException):
    pass


class IncompleteImplementationError(CloudPathException, NotImplementedError):
    pass


class InvalidPrefixError(CloudPathException, ValueError):
    pass


class InvalidConfigurationException(CloudPathException, ValueError):
    pass


class MissingCredentialsError(CloudPathException):
    pass


class MissingDependenciesError(CloudPathException, ModuleNotFoundError):
    pass


class NoStatError(CloudPathException):
    """Used if stats cannot be retrieved; e.g., file does not exist
    or for some backends path is a directory (which doesn't have
    stats available).
    """


class OverwriteDirtyFileError(CloudPathException):
    pass


class OverwriteNewerCloudError(CloudPathException):
    pass


class OverwriteNewerLocalError(CloudPathException):
    pass


class InvalidGlobArgumentsError(CloudPathException):
    pass
