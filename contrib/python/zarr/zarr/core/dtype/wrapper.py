"""
Wrapper for native array data types.

The ``ZDType`` class is an abstract base class for wrapping native array data types, e.g. NumPy dtypes.
``ZDType`` provides a common interface for working with data types in a way that is independent of the
underlying data type system.

The wrapper class encapsulates a native data type. Instances of the class can be created from a
native data type instance, and a native data type instance can be created from an instance of the
wrapper class.

The wrapper class is responsible for:
- Serializing and deserializing a native data type to Zarr V2 or Zarr V3 metadata.
  This ensures that the data type can be properly stored and retrieved from array metadata.
- Serializing and deserializing scalar values to Zarr V2 or Zarr V3 metadata. This is important for
  storing a fill value for an array in a manner that is valid for the data type.

You can add support for a new data type in Zarr by subclassing ``ZDType`` wrapper class and adapt its methods
to support your native data type. The wrapper class must be added to a data type registry
(defined elsewhere) before array creation routines or array reading routines can use your new data
type.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Generic,
    Literal,
    Self,
    TypeGuard,
    TypeVar,
    overload,
)

import numpy as np

if TYPE_CHECKING:
    from zarr.core.common import JSON, ZarrFormat
    from zarr.core.dtype.common import DTypeJSON, DTypeSpec_V2, DTypeSpec_V3

# This the upper bound for the scalar types we support. It's numpy scalars + str,
# because the new variable-length string dtype in numpy does not have a corresponding scalar type
TBaseScalar = np.generic | str | bytes
# This is the bound for the dtypes that we support. If we support non-numpy dtypes,
# then this bound will need to be widened.
TBaseDType = np.dtype[np.generic]

# These two type parameters are covariant because we want
# x : ZDType[BaseDType, BaseScalar] = ZDType[SubDType, SubScalar]
# to type check
TScalar_co = TypeVar("TScalar_co", bound=TBaseScalar, covariant=True)
TDType_co = TypeVar("TDType_co", bound=TBaseDType, covariant=True)


@dataclass(frozen=True, kw_only=True, slots=True)
class ZDType(ABC, Generic[TDType_co, TScalar_co]):
    """
    Abstract base class for wrapping native array data types, e.g. numpy dtypes

    Attributes
    ----------
    dtype_cls : ClassVar[type[TDType]]
        The wrapped dtype class. This is a class variable.
    _zarr_v3_name : ClassVar[str]
        The name given to the data type by a Zarr v3 data type specification. This is a
        class variable, and it should generally be unique across different data types.
    """

    # this class will create a native data type
    dtype_cls: ClassVar[type[TDType_co]]
    _zarr_v3_name: ClassVar[str]

    @classmethod
    def _check_native_dtype(cls: type[Self], dtype: TBaseDType) -> TypeGuard[TDType_co]:
        """
        Check that a native data type matches the dtype_cls class attribute.

        Used as a type guard.

        Parameters
        ----------
        dtype : TDType
            The dtype to check.

        Returns
        -------
        Bool
            True if the dtype matches, False otherwise.
        """
        return type(dtype) is cls.dtype_cls

    @classmethod
    @abstractmethod
    def from_native_dtype(cls: type[Self], dtype: TBaseDType) -> Self:
        """
        Create a ZDType instance from a native data type.

        This method is used when taking a user-provided native data type, like a NumPy data type,
        and creating the corresponding ZDType instance from them.

        Parameters
        ----------
        dtype : TDType
            The native data type object to wrap.

        Returns
        -------
        Self
            The ZDType that wraps the native data type.

        Raises
        ------
        TypeError
            If the native data type is not consistent with the wrapped data type.
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def to_native_dtype(self: Self) -> TDType_co:
        """
        Return an instance of the wrapped data type. This operation inverts ``from_native_dtype``.

        Returns
        -------
        TDType
            The native data type wrapped by this ZDType.
        """
        raise NotImplementedError  # pragma: no cover

    @classmethod
    @abstractmethod
    def _from_json_v2(cls: type[Self], data: DTypeJSON) -> Self:
        raise NotImplementedError  # pragma: no cover

    @classmethod
    @abstractmethod
    def _from_json_v3(cls: type[Self], data: DTypeJSON) -> Self:
        raise NotImplementedError  # pragma: no cover

    @classmethod
    def from_json(cls: type[Self], data: DTypeJSON, *, zarr_format: ZarrFormat) -> Self:
        """
        Create an instance of this ZDType from JSON data.

        Parameters
        ----------
        data : DTypeJSON
            The JSON representation of the data type.

        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        Self
            An instance of this data type.
        """
        if zarr_format == 2:
            return cls._from_json_v2(data)
        if zarr_format == 3:
            return cls._from_json_v3(data)
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    @overload
    def to_json(self, zarr_format: Literal[2]) -> DTypeSpec_V2: ...

    @overload
    def to_json(self, zarr_format: Literal[3]) -> DTypeSpec_V3: ...

    @abstractmethod
    def to_json(self, zarr_format: ZarrFormat) -> DTypeSpec_V2 | DTypeSpec_V3:
        """
        Serialize this ZDType to JSON.

        Parameters
        ----------
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        DTypeJSON_V2 | DTypeJSON_V3
            The JSON-serializable representation of the wrapped data type
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def _check_scalar(self, data: object) -> bool:
        """
        Check that an python object is a valid scalar value for the wrapped data type.

        Parameters
        ----------
        data : object
            A value to check.

        Returns
        -------
        Bool
            True if the object is valid, False otherwise.
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def cast_scalar(self, data: object) -> TScalar_co:
        """
        Cast a python object to the wrapped scalar type.

        The type of the provided scalar is first checked for compatibility.
        If it's incompatible with the associated scalar type, a ``TypeError`` will be raised.

        Parameters
        ----------
        data : object
            The python object to cast.

        Returns
        -------
        TScalar
            The cast value.
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def default_scalar(self) -> TScalar_co:
        """
        Get the default scalar value for the wrapped data type.

        This is a method, rather than an attribute, because the default value for some data types depends on parameters that are
        not known until a concrete data type is wrapped. For example, data types parametrized by a
        length like fixed-length strings or bytes will generate scalars consistent with that length.

        Returns
        -------
        TScalar
            The default value for this data type.
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def from_json_scalar(self: Self, data: JSON, *, zarr_format: ZarrFormat) -> TScalar_co:
        """
        Read a JSON-serializable value as a scalar.

        Parameters
        ----------
        data : JSON
            A JSON representation of a scalar value.
        zarr_format : ZarrFormat
            The zarr format version. This is specified because the JSON serialization of scalars
            differs between Zarr V2 and Zarr V3.

        Returns
        -------
        TScalar
            The deserialized scalar value.
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def to_json_scalar(self, data: object, *, zarr_format: ZarrFormat) -> JSON:
        """
        Serialize a python object to the JSON representation of a scalar.

        The value will first be cast to the scalar type associated with this ZDType, then serialized
        to JSON.

        Parameters
        ----------
        data : object
            The value to convert.
        zarr_format : ZarrFormat
            The zarr format version. This is specified because the JSON serialization of scalars
            differs between Zarr V2 and Zarr V3.

        Returns
        -------
        JSON
            The JSON-serialized scalar.
        """
        raise NotImplementedError  # pragma: no cover


def scalar_failed_type_check_msg(
    cls_instance: ZDType[TBaseDType, TBaseScalar], bad_scalar: object
) -> str:
    """
    Generate an error message reporting that a particular value failed a type check when attempting
    to cast that value to a scalar.
    """
    return (
        f"The value {bad_scalar!r} failed a type check. "
        f"It cannot be safely cast to a scalar compatible with {cls_instance}. "
        f"Consult the documentation for {cls_instance} to determine the possible values that can "
        "be cast to scalars of the wrapped data type."
    )
