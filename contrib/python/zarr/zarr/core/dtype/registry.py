from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

import numpy as np

from zarr.core.dtype.common import (
    DataTypeValidationError,
    DTypeJSON,
)

if TYPE_CHECKING:
    from importlib.metadata import EntryPoint

    from zarr.core.common import ZarrFormat
    from zarr.core.dtype.wrapper import TBaseDType, TBaseScalar, ZDType


# This class is different from the other registry classes, which inherit from
# dict. IMO it's simpler to just do a dataclass. But long-term we should
# have just 1 registry class in use.
@dataclass(frozen=True, kw_only=True)
class DataTypeRegistry:
    """
    A registry for ZDType classes.

    This registry is a mapping from Zarr data type names to their
    corresponding ZDType classes.

    Attributes
    ----------
    contents : dict[str, type[ZDType[TBaseDType, TBaseScalar]]]
        The mapping from Zarr data type names to their corresponding
        ZDType classes.
    """

    contents: dict[str, type[ZDType[TBaseDType, TBaseScalar]]] = field(
        default_factory=dict, init=False
    )

    _lazy_load_list: list[EntryPoint] = field(default_factory=list, init=False)

    def _lazy_load(self) -> None:
        """
        Load all data types from the lazy load list and register them with
        the registry. After loading, clear the lazy load list.
        """
        for e in self._lazy_load_list:
            self.register(e.load()._zarr_v3_name, e.load())

        self._lazy_load_list.clear()

    def register(self: Self, key: str, cls: type[ZDType[TBaseDType, TBaseScalar]]) -> None:
        """
        Register a data type with the registry.

        Parameters
        ----------
        key : str
            The Zarr V3 name of the data type.
        cls : type[ZDType[TBaseDType, TBaseScalar]]
            The class of the data type to register.

        Notes
        -----
        This method is idempotent. If the data type is already registered, this
        method does nothing.
        """
        if key not in self.contents or self.contents[key] != cls:
            self.contents[key] = cls

    def unregister(self, key: str) -> None:
        """
        Unregister a data type from the registry.

        Parameters
        ----------
        key : str
            The key associated with the ZDType class to be unregistered.

        Returns
        -------
        None

        Raises
        ------
        KeyError
            If the data type is not found in the registry.
        """
        if key in self.contents:
            del self.contents[key]
        else:
            raise KeyError(f"Data type '{key}' not found in registry.")

    def get(self, key: str) -> type[ZDType[TBaseDType, TBaseScalar]]:
        """
        Retrieve a registered ZDType class by its key.

        Parameters
        ----------
        key : str
            The key associated with the desired ZDType class.

        Returns
        -------
        type[ZDType[TBaseDType, TBaseScalar]]
            The ZDType class registered under the given key.

        Raises
        ------
        KeyError
            If the key is not found in the registry.
        """

        return self.contents[key]

    def match_dtype(self, dtype: TBaseDType) -> ZDType[TBaseDType, TBaseScalar]:
        """
        Match a native data type, e.g. a NumPy data type, to a registered ZDType.

        Parameters
        ----------
        dtype : TBaseDType
            The native data type to match.

        Returns
        -------
        ZDType[TBaseDType, TBaseScalar]
            The matched ZDType corresponding to the provided NumPy data type.

        Raises
        ------
        ValueError
            If the data type is a NumPy "Object" type, which is ambiguous, or if multiple
            or no Zarr data types are found that match the provided dtype.

        Notes
        -----
        This function attempts to resolve a Zarr data type from a given native data type.
        If the dtype is a NumPy "Object" data type, it raises a ValueError, as this type
        can represent multiple Zarr data types. In such cases, a specific Zarr data type
        should be explicitly constructed instead of relying on dynamic resolution.

        If multiple matches are found, it will also raise a ValueError. In this case
        conflicting data types must be unregistered, or the Zarr data type should be explicitly
        constructed.
        """

        if dtype == np.dtype("O"):
            msg = (
                f"Zarr data type resolution from {dtype} failed. "
                'Attempted to resolve a zarr data type from a numpy "Object" data type, which is '
                'ambiguous, as multiple zarr data types can be represented by the numpy "Object" '
                "data type. "
                "In this case you should construct your array by providing a specific Zarr data "
                'type. For a list of Zarr data types that are compatible with the numpy "Object"'
                "data type, see https://github.com/zarr-developers/zarr-python/issues/3117"
            )
            raise ValueError(msg)
        matched: list[ZDType[TBaseDType, TBaseScalar]] = []
        for val in self.contents.values():
            with contextlib.suppress(DataTypeValidationError):
                matched.append(val.from_native_dtype(dtype))
        if len(matched) == 1:
            return matched[0]
        elif len(matched) > 1:
            msg = (
                f"Zarr data type resolution from {dtype} failed. "
                f"Multiple data type wrappers found that match dtype '{dtype}': {matched}. "
                "You should unregister one of these data types, or avoid Zarr data type inference "
                "entirely by providing a specific Zarr data type when creating your array."
                "For more information, see https://github.com/zarr-developers/zarr-python/issues/3117"
            )
            raise ValueError(msg)
        raise ValueError(f"No Zarr data type found that matches dtype '{dtype!r}'")

    def match_json(
        self, data: DTypeJSON, *, zarr_format: ZarrFormat
    ) -> ZDType[TBaseDType, TBaseScalar]:
        """
        Match a JSON representation of a data type to a registered ZDType.

        Parameters
        ----------
        data : DTypeJSON
            The JSON representation of a data type to match.
        zarr_format : ZarrFormat
            The Zarr format version to consider when matching data types.

        Returns
        -------
        ZDType[TBaseDType, TBaseScalar]
            The matched ZDType corresponding to the JSON representation.

        Raises
        ------
        ValueError
            If no matching Zarr data type is found for the given JSON data.
        """

        for val in self.contents.values():
            try:
                return val.from_json(data, zarr_format=zarr_format)
            except DataTypeValidationError:
                pass
        raise ValueError(f"No Zarr data type found that matches {data!r}")
