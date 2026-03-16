__all__ = [
    "ArrayIndexError",
    "ArrayNotFoundError",
    "BaseZarrError",
    "BoundsCheckError",
    "ContainsArrayAndGroupError",
    "ContainsArrayError",
    "ContainsGroupError",
    "GroupNotFoundError",
    "MetadataValidationError",
    "NegativeStepError",
    "NodeTypeValidationError",
    "UnstableSpecificationWarning",
    "VindexInvalidSelectionError",
    "ZarrDeprecationWarning",
    "ZarrFutureWarning",
    "ZarrRuntimeWarning",
]


class BaseZarrError(ValueError):
    """
    Base error which all zarr errors are sub-classed from.
    """

    _msg: str = "{}"

    def __init__(self, *args: object) -> None:
        """
        If a single argument is passed, treat it as a pre-formatted message.

        If multiple arguments are passed, they are used as arguments for a template string class
        variable. This behavior is deprecated.
        """
        if len(args) == 1:
            super().__init__(args[0])
        else:
            super().__init__(self._msg.format(*args))


class NodeNotFoundError(BaseZarrError, FileNotFoundError):
    """
    Raised when a node (array or group) is not found at a certain path.
    """


class ArrayNotFoundError(NodeNotFoundError):
    """
    Raised when an array isn't found at a certain path.
    """

    _msg = "No array found in store {!r} at path {!r}"


class GroupNotFoundError(NodeNotFoundError):
    """
    Raised when a group isn't found at a certain path.
    """

    _msg = "No group found in store {!r} at path {!r}"


class ContainsGroupError(BaseZarrError):
    """Raised when a group already exists at a certain path."""

    _msg = "A group exists in store {!r} at path {!r}."


class ContainsArrayError(BaseZarrError):
    """Raised when an array already exists at a certain path."""

    _msg = "An array exists in store {!r} at path {!r}."


class ContainsArrayAndGroupError(BaseZarrError):
    """Raised when both array and group metadata are found at the same path."""

    _msg = (
        "Array and group metadata documents (.zarray and .zgroup) were both found in store "
        "{!r} at path {!r}. "
        "Only one of these files may be present in a given directory / prefix. "
        "Remove the .zarray file, or the .zgroup file, or both."
    )


class MetadataValidationError(BaseZarrError):
    """Raised when the Zarr metadata is invalid in some way"""

    _msg = "Invalid value for '{}'. Expected '{}'. Got '{}'."


class UnknownCodecError(BaseZarrError):
    """
    Raised when an unknown codec was used.
    """


class NodeTypeValidationError(MetadataValidationError):
    """
    Specialized exception when the node_type of the metadata document is incorrect.

    This can be raised when the value is invalid or unexpected given the context,
    for example an 'array' node when we expected a 'group'.
    """


class ZarrFutureWarning(FutureWarning):
    """
    A warning intended for end users raised to indicate deprecated features.
    """


class UnstableSpecificationWarning(ZarrFutureWarning):
    """
    A warning raised to indicate that a feature is outside the Zarr specification.
    """


class ZarrDeprecationWarning(DeprecationWarning):
    """
    A warning raised to indicate that a feature will be removed in a future release.
    """


class ZarrUserWarning(UserWarning):
    """
    A warning raised to report problems with user code.
    """


class ZarrRuntimeWarning(RuntimeWarning):
    """
    A warning for dubious runtime behavior.
    """


class VindexInvalidSelectionError(IndexError): ...


class NegativeStepError(IndexError): ...


class BoundsCheckError(IndexError): ...


class ArrayIndexError(IndexError): ...
