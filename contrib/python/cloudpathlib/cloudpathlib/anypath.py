import os
from abc import ABC
from pathlib import Path
from typing import Any, Union

from .cloudpath import InvalidPrefixError, CloudPath
from .exceptions import AnyPathTypeError
from .url_utils import path_from_fileurl


class AnyPath(ABC):
    """Polymorphic virtual superclass for CloudPath and pathlib.Path. Constructing an instance will
    automatically dispatch to CloudPath or Path based on the input. It also supports both
    isinstance and issubclass checks.

    This class also integrates with Pydantic. When used as a type declaration for a Pydantic
    BaseModel, the Pydantic validation process will appropriately run inputs through this class'
    constructor and dispatch to CloudPath or Path.
    """

    def __new__(cls, *args, **kwargs) -> Union[CloudPath, Path]:  # type: ignore
        try:
            return CloudPath(*args, **kwargs)  # type: ignore
        except InvalidPrefixError as cloudpath_exception:
            try:
                if isinstance(args[0], str) and args[0].lower().startswith("file:"):
                    path = path_from_fileurl(args[0], **kwargs)
                    for part in args[1:]:
                        path /= part
                    return path

                return Path(*args, **kwargs)
            except TypeError as path_exception:
                raise AnyPathTypeError(
                    "Invalid input for both CloudPath and Path. "
                    f"CloudPath exception: {repr(cloudpath_exception)} "
                    f"Path exception: {repr(path_exception)}"
                )

    # ===========  pydantic integration special methods ===============
    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler):
        """Pydantic special method. See
        https://docs.pydantic.dev/2.0/usage/types/custom/"""
        try:
            from pydantic_core import core_schema

            return core_schema.no_info_after_validator_function(
                cls.validate,
                core_schema.any_schema(),
            )
        except ImportError:
            return None

    @classmethod
    def validate(cls, v: str) -> Union[CloudPath, Path]:
        """Pydantic special method. See
        https://docs.pydantic.dev/2.0/usage/types/custom/"""
        try:
            return cls.__new__(cls, v)
        except AnyPathTypeError as e:
            # type errors no longer converted to validation errors
            #  https://docs.pydantic.dev/2.0/migration/#typeerror-is-no-longer-converted-to-validationerror-in-validators
            raise ValueError(e)

    @classmethod
    def __get_validators__(cls):
        """Pydantic special method. See
        https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types"""
        yield cls._validate

    @classmethod
    def _validate(cls, value) -> Union[CloudPath, Path]:
        """Used as a Pydantic validator. See
        https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types"""
        # Note __new__ is static method and not a class method
        return cls.__new__(cls, value)


AnyPath.register(CloudPath)  # type: ignore
AnyPath.register(Path)


def to_anypath(s: Union[str, os.PathLike]) -> Union[CloudPath, Path]:
    """Convenience method to convert a str or os.PathLike to the
    proper Path or CloudPath object using AnyPath.
    """
    # shortcut pathlike items that are already valid Path/CloudPath
    if isinstance(s, (CloudPath, Path)):
        return s

    return AnyPath(s)  # type: ignore
