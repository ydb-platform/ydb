from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Literal, Self, TypeGuard, overload

import numpy as np

from zarr.core.dtype.common import (
    DataTypeValidationError,
    DTypeConfig_V2,
    DTypeJSON,
    HasItemSize,
    check_dtype_spec_v2,
)
from zarr.core.dtype.wrapper import TBaseDType, ZDType

if TYPE_CHECKING:
    from zarr.core.common import JSON, ZarrFormat


@dataclass(frozen=True, kw_only=True, slots=True)
class Bool(ZDType[np.dtypes.BoolDType, np.bool_], HasItemSize):
    """
    A Zarr data type for arrays containing booleans.

    Wraps the [`np.dtypes.BoolDType`][numpy.dtypes.BoolDType] data type. Scalars for this data type are instances of
    [`np.bool_`][numpy.bool_].

    Attributes
    ----------

    _zarr_v3_name : Literal["bool"] = "bool"
        The Zarr v3 name of the dtype.
    _zarr_v2_name : ``Literal["|b1"]`` = ``"|b1"``
        The Zarr v2 name of the dtype, which is also a string representation
        of the boolean dtype used by NumPy.
    dtype_cls : ClassVar[type[np.dtypes.BoolDType]] = np.dtypes.BoolDType
        The NumPy dtype class.

    References
    ----------
    This class implements the boolean data type defined in Zarr V2 and V3.

    See the [Zarr V2](https://github.com/zarr-developers/zarr-specs/blob/main/docs/v2/v2.0.rst#data-type-encoding)and [Zarr V3](https://github.com/zarr-developers/zarr-specs/blob/main/docs/v3/data-types/index.rst) specification documents for details.
    """

    _zarr_v3_name: ClassVar[Literal["bool"]] = "bool"
    _zarr_v2_name: ClassVar[Literal["|b1"]] = "|b1"
    dtype_cls = np.dtypes.BoolDType

    @classmethod
    def from_native_dtype(cls, dtype: TBaseDType) -> Self:
        """
        Create an instance of Bool from an instance of np.dtypes.BoolDType.

        Parameters
        ----------
        dtype : TBaseDType
            The NumPy boolean dtype instance to convert.

        Returns
        -------
        Bool
            An instance of Bool.

        Raises
        ------
        DataTypeValidationError
            If the provided dtype is not compatible with this ZDType.
        """
        if cls._check_native_dtype(dtype):
            return cls()
        raise DataTypeValidationError(
            f"Invalid data type: {dtype}. Expected an instance of {cls.dtype_cls}"
        )

    def to_native_dtype(self: Self) -> np.dtypes.BoolDType:
        """
        Create a NumPy boolean dtype instance from this ZDType.

        Returns
        -------
        np.dtypes.BoolDType
            The NumPy boolean dtype.
        """
        return self.dtype_cls()

    @classmethod
    def _check_json_v2(
        cls,
        data: DTypeJSON,
    ) -> TypeGuard[DTypeConfig_V2[Literal["|b1"], None]]:
        """
        Check that the input is a valid JSON representation of a Bool.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        ``TypeGuard[DTypeConfig_V2[Literal["|b1"], None]]``
            True if the input is a valid JSON representation, False otherwise.
        """
        return (
            check_dtype_spec_v2(data)
            and data["name"] == cls._zarr_v2_name
            and data["object_codec_id"] is None
        )

    @classmethod
    def _check_json_v3(cls, data: DTypeJSON) -> TypeGuard[Literal["bool"]]:
        """
        Check that the input is a valid JSON representation of this class in Zarr V3.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        bool
            True if the input is a valid JSON representation, False otherwise.
        """
        return data == cls._zarr_v3_name

    @classmethod
    def _from_json_v2(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of Bool from Zarr V2-flavored JSON.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data.

        Returns
        -------
        Bool
            An instance of Bool.

        Raises
        ------
        DataTypeValidationError
            If the input JSON is not a valid representation of this class.
        """
        if cls._check_json_v2(data):
            return cls()
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected the string {cls._zarr_v2_name!r}"
        raise DataTypeValidationError(msg)

    @classmethod
    def _from_json_v3(cls: type[Self], data: DTypeJSON) -> Self:
        """
        Create an instance of Bool from Zarr V3-flavored JSON.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data.

        Returns
        -------
        Bool
            An instance of Bool.

        Raises
        ------
        DataTypeValidationError
            If the input JSON is not a valid representation of this class.
        """
        if cls._check_json_v3(data):
            return cls()
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected the string {cls._zarr_v3_name!r}"
        raise DataTypeValidationError(msg)

    @overload
    def to_json(self, zarr_format: Literal[2]) -> DTypeConfig_V2[Literal["|b1"], None]: ...

    @overload
    def to_json(self, zarr_format: Literal[3]) -> Literal["bool"]: ...

    def to_json(
        self, zarr_format: ZarrFormat
    ) -> DTypeConfig_V2[Literal["|b1"], None] | Literal["bool"]:
        """
        Serialize this Bool instance to JSON.

        Parameters
        ----------
        zarr_format : ZarrFormat
            The Zarr format version (2 or 3).

        Returns
        -------
        ``DTypeConfig_V2[Literal["|b1"], None] | Literal["bool"]``
            The JSON representation of the Bool instance.

        Raises
        ------
        ValueError
            If the zarr_format is not 2 or 3.
        """
        if zarr_format == 2:
            return {"name": self._zarr_v2_name, "object_codec_id": None}
        elif zarr_format == 3:
            return self._zarr_v3_name
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    def _check_scalar(self, data: object) -> bool:
        """
        Check if the input can be cast to a boolean scalar.

        Parameters
        ----------
        data : object
            The data to check.

        Returns
        -------
        bool
            True if the input can be cast to a boolean scalar, False otherwise.
        """
        return True

    def cast_scalar(self, data: object) -> np.bool_:
        """
        Cast the input to a numpy boolean scalar.

        Parameters
        ----------
        data : object
            The data to cast.

        Returns
        -------
        bool : np.bool_
            The numpy boolean scalar.

        Raises
        ------
        TypeError
            If the input cannot be converted to a numpy boolean.
        """
        if self._check_scalar(data):
            return np.bool_(data)
        msg = (  # pragma: no cover
            f"Cannot convert object {data!r} with type {type(data)} to a scalar compatible with the "
            f"data type {self}."
        )
        raise TypeError(msg)  # pragma: no cover

    def default_scalar(self) -> np.bool_:
        """
        Get the default value for the boolean dtype.

        Returns
        -------
        bool : np.bool_
            The default value.
        """
        return np.False_

    def to_json_scalar(self, data: object, *, zarr_format: ZarrFormat) -> bool:
        """
        Convert a scalar to a python bool.

        Parameters
        ----------
        data : object
            The value to convert.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        bool
            The JSON-serializable format.
        """
        return bool(data)

    def from_json_scalar(self, data: JSON, *, zarr_format: ZarrFormat) -> np.bool_:
        """
        Read a JSON-serializable value as a numpy boolean scalar.

        Parameters
        ----------
        data : JSON
            The JSON-serializable value.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        bool : np.bool_
            The numpy boolean scalar.

        Raises
        ------
        TypeError
            If the input is not a valid boolean type.
        """
        if self._check_scalar(data):
            return np.bool_(data)
        raise TypeError(f"Invalid type: {data}. Expected a boolean.")  # pragma: no cover

    @property
    def item_size(self) -> int:
        """
        The size of a single scalar in bytes.

        Returns
        -------
        int
            The size of a single scalar in bytes.
        """
        return 1
