from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import deprecated

import zarr.api.asynchronous as async_api
import zarr.core.array
from zarr.core.array import DEFAULT_FILL_VALUE, Array, AsyncArray, CompressorLike
from zarr.core.group import Group
from zarr.core.sync import sync
from zarr.core.sync_group import create_hierarchy
from zarr.errors import ZarrDeprecationWarning

if TYPE_CHECKING:
    from collections.abc import Iterable

    import numpy as np
    import numpy.typing as npt

    from zarr.abc.codec import Codec
    from zarr.abc.numcodec import Numcodec
    from zarr.api.asynchronous import ArrayLike, PathLike
    from zarr.core.array import (
        CompressorsLike,
        FiltersLike,
        SerializerLike,
        ShardsLike,
    )
    from zarr.core.array_spec import ArrayConfigLike
    from zarr.core.buffer import NDArrayLike, NDArrayLikeOrScalar
    from zarr.core.chunk_key_encodings import ChunkKeyEncoding, ChunkKeyEncodingLike
    from zarr.core.common import (
        JSON,
        AccessModeLiteral,
        DimensionNames,
        MemoryOrder,
        ShapeLike,
        ZarrFormat,
    )
    from zarr.core.dtype import ZDTypeLike
    from zarr.storage import StoreLike
    from zarr.types import AnyArray

__all__ = [
    "array",
    "consolidate_metadata",
    "copy",
    "copy_all",
    "copy_store",
    "create",
    "create_array",
    "create_hierarchy",
    "empty",
    "empty_like",
    "from_array",
    "full",
    "full_like",
    "group",
    "load",
    "ones",
    "ones_like",
    "open",
    "open_array",
    "open_consolidated",
    "open_group",
    "open_like",
    "save",
    "save_array",
    "save_group",
    "tree",
    "zeros",
    "zeros_like",
]


def consolidate_metadata(
    store: StoreLike,
    path: str | None = None,
    zarr_format: ZarrFormat | None = None,
) -> Group:
    """
    Consolidate the metadata of all nodes in a hierarchy.

    Upon completion, the metadata of the root node in the Zarr hierarchy will be
    updated to include all the metadata of child nodes. For Stores that do
    not use consolidated metadata, this operation raises a `TypeError`.

    Parameters
    ----------
    store : StoreLike
        The store-like object whose metadata you wish to consolidate. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    path : str, optional
        A path to a group in the store to consolidate at. Only children
        below that group will be consolidated.

        By default, the root node is used so all the metadata in the
        store is consolidated.
    zarr_format : {2, 3, None}, optional
        The zarr format of the hierarchy. By default the zarr format
        is inferred.

    Returns
    -------
    group: Group
        The group, with the ``consolidated_metadata`` field set to include
        the metadata of each child node. If the Store doesn't support
        consolidated metadata, this function raises a `TypeError`.
        See ``Store.supports_consolidated_metadata``.

    """
    return Group(sync(async_api.consolidate_metadata(store, path=path, zarr_format=zarr_format)))


def copy(*args: Any, **kwargs: Any) -> tuple[int, int, int]:
    """
    Not implemented.
    """
    return sync(async_api.copy(*args, **kwargs))


def copy_all(*args: Any, **kwargs: Any) -> tuple[int, int, int]:
    """
    Not implemented.
    """
    return sync(async_api.copy_all(*args, **kwargs))


def copy_store(*args: Any, **kwargs: Any) -> tuple[int, int, int]:
    """
    Not implemented.
    """
    return sync(async_api.copy_store(*args, **kwargs))


def load(
    store: StoreLike,
    path: str | None = None,
    zarr_format: ZarrFormat | None = None,
    zarr_version: ZarrFormat | None = None,
) -> NDArrayLikeOrScalar | dict[str, NDArrayLikeOrScalar]:
    """Load data from an array or group into memory.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    path : str or None, optional
        The path within the store from which to load.

    Returns
    -------
    out
        If the path contains an array, out will be a numpy array. If the path contains
        a group, out will be a dict-like object where keys are array names and values
        are numpy arrays.

    See Also
    --------
    save, savez

    Notes
    -----
    If loading data from a group of arrays, data will not be immediately loaded into
    memory. Rather, arrays will be loaded into memory as they are requested.
    """
    return sync(
        async_api.load(store=store, zarr_version=zarr_version, zarr_format=zarr_format, path=path)
    )


def open(
    store: StoreLike | None = None,
    *,
    mode: AccessModeLiteral | None = None,
    zarr_version: ZarrFormat | None = None,  # deprecated
    zarr_format: ZarrFormat | None = None,
    path: str | None = None,
    storage_options: dict[str, Any] | None = None,
    **kwargs: Any,  # TODO: type kwargs as valid args to async_api.open
) -> AnyArray | Group:
    """Open a group or array using file-mode-like semantics.

    Parameters
    ----------
    store : StoreLike or None, default=None
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    mode : {'r', 'r+', 'a', 'w', 'w-'}, optional
        Persistence mode: 'r' means read only (must exist); 'r+' means
        read/write (must exist); 'a' means read/write (create if doesn't
        exist); 'w' means create (overwrite if exists); 'w-' means create
        (fail if exists).
        If the store is read-only, the default is 'r'; otherwise, it is 'a'.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
    path : str or None, optional
        The path within the store to open.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    **kwargs
        Additional parameters are passed through to [`zarr.creation.open_array`][] or
        [`open_group`][zarr.api.asynchronous.open_group].

    Returns
    -------
    z : array or group
        Return type depends on what exists in the given store.
    """
    obj = sync(
        async_api.open(
            store=store,
            mode=mode,
            zarr_version=zarr_version,
            zarr_format=zarr_format,
            path=path,
            storage_options=storage_options,
            **kwargs,
        )
    )
    if isinstance(obj, AsyncArray):
        return Array(obj)
    else:
        return Group(obj)


def open_consolidated(*args: Any, use_consolidated: Literal[True] = True, **kwargs: Any) -> Group:
    """
    Alias for [`open_group`][zarr.api.synchronous.open_group] with ``use_consolidated=True``.
    """
    return Group(
        sync(async_api.open_consolidated(*args, use_consolidated=use_consolidated, **kwargs))
    )


def save(
    store: StoreLike,
    *args: NDArrayLike,
    zarr_version: ZarrFormat | None = None,  # deprecated
    zarr_format: ZarrFormat | None = None,
    path: str | None = None,
    **kwargs: Any,  # TODO: type kwargs as valid args to async_api.save
) -> None:
    """Save an array or group of arrays to the local file system.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    *args : ndarray
        NumPy arrays with data to save.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
    path : str or None, optional
        The path within the group where the arrays will be saved.
    **kwargs
        NumPy arrays with data to save.
    """
    return sync(
        async_api.save(
            store, *args, zarr_version=zarr_version, zarr_format=zarr_format, path=path, **kwargs
        )
    )


def save_array(
    store: StoreLike,
    arr: NDArrayLike,
    *,
    zarr_version: ZarrFormat | None = None,  # deprecated
    zarr_format: ZarrFormat | None = None,
    path: str | None = None,
    storage_options: dict[str, Any] | None = None,
    **kwargs: Any,  # TODO: type kwargs as valid args to async_api.save_array
) -> None:
    """Save a NumPy array to the local file system.

    Follows a similar API to the NumPy save() function.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    arr : ndarray
        NumPy array with data to save.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving. The default is ``None``, which will
        use the default Zarr format defined in the global configuration object.
    path : str or None, optional
        The path within the store where the array will be saved.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    **kwargs
        Passed through to [`create`][zarr.api.asynchronous.create], e.g., compressor.
    """
    return sync(
        async_api.save_array(
            store=store,
            arr=arr,
            zarr_version=zarr_version,
            zarr_format=zarr_format,
            path=path,
            storage_options=storage_options,
            **kwargs,
        )
    )


def save_group(
    store: StoreLike,
    *args: NDArrayLike,
    zarr_version: ZarrFormat | None = None,  # deprecated
    zarr_format: ZarrFormat | None = None,
    path: str | None = None,
    storage_options: dict[str, Any] | None = None,
    **kwargs: NDArrayLike,
) -> None:
    """Save several NumPy arrays to the local file system.

    Follows a similar API to the NumPy savez()/savez_compressed() functions.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    *args : ndarray
        NumPy arrays with data to save.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
    path : str or None, optional
        Path within the store where the group will be saved.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    **kwargs
        NumPy arrays with data to save.
    """

    return sync(
        async_api.save_group(
            store,
            *args,
            zarr_version=zarr_version,
            zarr_format=zarr_format,
            path=path,
            storage_options=storage_options,
            **kwargs,
        )
    )


@deprecated("Use Group.tree instead.", category=ZarrDeprecationWarning)
def tree(grp: Group, expand: bool | None = None, level: int | None = None) -> Any:
    """Provide a rich display of the hierarchy.

    !!! warning "Deprecated"
        `zarr.tree()` is deprecated since v3.0.0 and will be removed in a future release.
        Use `group.tree()` instead.

    Parameters
    ----------
    grp : Group
        Zarr or h5py group.
    expand : bool, optional
        Only relevant for HTML representation. If True, tree will be fully expanded.
    level : int, optional
        Maximum depth to descend into hierarchy.

    Returns
    -------
    TreeRepr
        A pretty-printable object displaying the hierarchy.
    """
    return sync(async_api.tree(grp._async_group, expand=expand, level=level))


# TODO: add type annotations for kwargs
def array(data: npt.ArrayLike | AnyArray, **kwargs: Any) -> AnyArray:
    """Create an array filled with `data`.

    Parameters
    ----------
    data : array_like
        The data to fill the array with.
    **kwargs
        Passed through to [`create`][zarr.api.asynchronous.create].

    Returns
    -------
    array : Array
        The new array.
    """

    return Array(sync(async_api.array(data=data, **kwargs)))


def group(
    store: StoreLike | None = None,
    *,
    overwrite: bool = False,
    chunk_store: StoreLike | None = None,  # not used
    cache_attrs: bool | None = None,  # not used, default changed
    synchronizer: Any | None = None,  # not used
    path: str | None = None,
    zarr_version: ZarrFormat | None = None,  # deprecated
    zarr_format: ZarrFormat | None = None,
    meta_array: Any | None = None,  # not used
    attributes: dict[str, JSON] | None = None,
    storage_options: dict[str, Any] | None = None,
) -> Group:
    """Create a group.

    Parameters
    ----------
    store : StoreLike or None, default=None
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    overwrite : bool, optional
        If True, delete any pre-existing data in `store` at `path` before
        creating the group.
    chunk_store : StoreLike or None, default=None
        Separate storage for chunks. Not implemented.
    cache_attrs : bool, optional
        If True (default), user attributes will be cached for attribute read
        operations. If False, user attributes are reloaded from the store prior
        to all attribute read operations.
    synchronizer : object, optional
        Array synchronizer.
    path : str, optional
        Group path within store.
    meta_array : array-like, optional
        An array instance to use for determining arrays to create and return
        to users. Use `numpy.empty(())` by default.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.

    Returns
    -------
    g : Group
        The new group.
    """
    return Group(
        sync(
            async_api.group(
                store=store,
                overwrite=overwrite,
                chunk_store=chunk_store,
                cache_attrs=cache_attrs,
                synchronizer=synchronizer,
                path=path,
                zarr_version=zarr_version,
                zarr_format=zarr_format,
                meta_array=meta_array,
                attributes=attributes,
                storage_options=storage_options,
            )
        )
    )


def open_group(
    store: StoreLike | None = None,
    *,
    mode: AccessModeLiteral = "a",
    cache_attrs: bool | None = None,  # default changed, not used in async api
    synchronizer: Any = None,  # not used in async api
    path: str | None = None,
    chunk_store: StoreLike | None = None,  # not used in async api
    storage_options: dict[str, Any] | None = None,  # not used in async api
    zarr_version: ZarrFormat | None = None,  # deprecated
    zarr_format: ZarrFormat | None = None,
    meta_array: Any | None = None,  # not used in async api
    attributes: dict[str, JSON] | None = None,
    use_consolidated: bool | str | None = None,
) -> Group:
    """Open a group using file-mode-like semantics.

    Parameters
    ----------
    store : StoreLike or None, default=None
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    mode : {'r', 'r+', 'a', 'w', 'w-'}, optional
        Persistence mode: 'r' means read only (must exist); 'r+' means
        read/write (must exist); 'a' means read/write (create if doesn't
        exist); 'w' means create (overwrite if exists); 'w-' means create
        (fail if exists).
    cache_attrs : bool, optional
        If True (default), user attributes will be cached for attribute read
        operations. If False, user attributes are reloaded from the store prior
        to all attribute read operations.
    synchronizer : object, optional
        Array synchronizer.
    path : str, optional
        Group path within store.
    chunk_store : StoreLike or None, default=None
        Separate storage for chunks. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    meta_array : array-like, optional
        An array instance to use for determining arrays to create and return
        to users. Use `numpy.empty(())` by default.
    attributes : dict
        A dictionary of JSON-serializable values with user-defined attributes.
    use_consolidated : bool or str, default None
        Whether to use consolidated metadata.

        By default, consolidated metadata is used if it's present in the
        store (in the ``zarr.json`` for Zarr format 3 and in the ``.zmetadata`` file
        for Zarr format 2).

        To explicitly require consolidated metadata, set ``use_consolidated=True``,
        which will raise an exception if consolidated metadata is not found.

        To explicitly *not* use consolidated metadata, set ``use_consolidated=False``,
        which will fall back to using the regular, non consolidated metadata.

        Zarr format 2 allowed configuring the key storing the consolidated metadata
        (``.zmetadata`` by default). Specify the custom key as ``use_consolidated``
        to load consolidated metadata from a non-default key.

    Returns
    -------
    g : Group
        The new group.
    """
    return Group(
        sync(
            async_api.open_group(
                store=store,
                mode=mode,
                cache_attrs=cache_attrs,
                synchronizer=synchronizer,
                path=path,
                chunk_store=chunk_store,
                storage_options=storage_options,
                zarr_version=zarr_version,
                zarr_format=zarr_format,
                meta_array=meta_array,
                attributes=attributes,
                use_consolidated=use_consolidated,
            )
        )
    )


def create_group(
    store: StoreLike,
    *,
    path: str | None = None,
    zarr_format: ZarrFormat | None = None,
    overwrite: bool = False,
    attributes: dict[str, Any] | None = None,
    storage_options: dict[str, Any] | None = None,
) -> Group:
    """Create a group.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    path : str, optional
        Group path within store.
    overwrite : bool, optional
        If True, pre-existing data at ``path`` will be deleted before
        creating the group.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
        If no ``zarr_format`` is provided, the default format will be used.
        This default can be changed by modifying the value of ``default_zarr_format``
        in [`zarr.config`][zarr.config].
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.

    Returns
    -------
    Group
        The new group.
    """
    return Group(
        sync(
            async_api.create_group(
                store=store,
                path=path,
                overwrite=overwrite,
                storage_options=storage_options,
                zarr_format=zarr_format,
                attributes=attributes,
            )
        )
    )


# TODO: add type annotations for kwargs
def create(
    shape: tuple[int, ...] | int,
    *,  # Note: this is a change from v2
    chunks: tuple[int, ...] | int | bool | None = None,
    dtype: ZDTypeLike | None = None,
    compressor: CompressorLike = "auto",
    fill_value: Any | None = DEFAULT_FILL_VALUE,  # TODO: need type
    order: MemoryOrder | None = None,
    store: StoreLike | None = None,
    synchronizer: Any | None = None,
    overwrite: bool = False,
    path: PathLike | None = None,
    chunk_store: StoreLike | None = None,
    filters: Iterable[dict[str, JSON] | Numcodec] | None = None,
    cache_metadata: bool | None = None,
    cache_attrs: bool | None = None,
    read_only: bool | None = None,
    object_codec: Codec | None = None,  # TODO: type has changed
    dimension_separator: Literal[".", "/"] | None = None,
    write_empty_chunks: bool | None = None,  # TODO: default has changed
    zarr_version: ZarrFormat | None = None,  # deprecated
    zarr_format: ZarrFormat | None = None,
    meta_array: Any | None = None,  # TODO: need type
    attributes: dict[str, JSON] | None = None,
    # v3 only
    chunk_shape: tuple[int, ...] | int | None = None,
    chunk_key_encoding: (
        ChunkKeyEncoding
        | tuple[Literal["default"], Literal[".", "/"]]
        | tuple[Literal["v2"], Literal[".", "/"]]
        | None
    ) = None,
    codecs: Iterable[Codec | dict[str, JSON]] | None = None,
    dimension_names: DimensionNames = None,
    storage_options: dict[str, Any] | None = None,
    config: ArrayConfigLike | None = None,
    **kwargs: Any,
) -> AnyArray:
    """Create an array.

    Parameters
    ----------
    shape : int or tuple of ints
        Array shape.
    chunks : int or tuple of ints, optional
        Chunk shape. If True, will be guessed from ``shape`` and ``dtype``. If
        False, will be set to ``shape``, i.e., single chunk for the whole array.
        If an int, the chunk size in each dimension will be given by the value
        of ``chunks``. Default is True.
    dtype : str or dtype, optional
        NumPy dtype.
    compressor : Codec, optional
        Primary compressor to compress chunk data.
        Zarr format 2 only. Zarr format 3 arrays should use ``codecs`` instead.

        If neither ``compressor`` nor ``filters`` are provided, the default compressor
        [`zarr.codecs.ZstdCodec`][] is used.

        If ``compressor`` is set to ``None``, no compression is used.
    fill_value : Any, optional
        Fill value for the array.
    order : {'C', 'F'}, optional
        Deprecated in favor of the ``config`` keyword argument.
        Pass ``{'order': <value>}`` to ``create`` instead of using this parameter.
        Memory layout to be used within each chunk.
        If not specified, the ``array.order`` parameter in the global config will be used.
    store : StoreLike or None, default=None
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    synchronizer : object, optional
        Array synchronizer.
    overwrite : bool, optional
        If True, delete all pre-existing data in ``store`` at ``path`` before
        creating the array.
    path : str, optional
        Path under which array is stored.
    chunk_store : StoreLike or None, default=None
        Separate storage for chunks. If not provided, ``store`` will be used
        for storage of both chunks and metadata.
    filters : Iterable[Codec] | Literal["auto"], optional
        Iterable of filters to apply to each chunk of the array, in order, before serializing that
        chunk to bytes.

        For Zarr format 3, a "filter" is a codec that takes an array and returns an array,
        and these values must be instances of [`zarr.abc.codec.ArrayArrayCodec`][], or a
        dict representations of [`zarr.abc.codec.ArrayArrayCodec`][].

        For Zarr format 2, a "filter" can be any numcodecs codec; you should ensure that the
        the order if your filters is consistent with the behavior of each filter.

        The default value of ``"auto"`` instructs Zarr to use a default used based on the data
        type of the array and the Zarr format specified. For all data types in Zarr V3, and most
        data types in Zarr V2, the default filters are empty. The only cases where default filters
        are not empty is when the Zarr format is 2, and the data type is a variable-length data type like
        [`zarr.dtype.VariableLengthUTF8`][] or [`zarr.dtype.VariableLengthUTF8`][]. In these cases,
        the default filters contains a single element which is a codec specific to that particular data type.

        To create an array with no filters, provide an empty iterable or the value ``None``.
    cache_metadata : bool, optional
        If True, array configuration metadata will be cached for the
        lifetime of the object. If False, array metadata will be reloaded
        prior to all data access and modification operations (may incur
        overhead depending on storage and data access pattern).
    cache_attrs : bool, optional
        If True (default), user attributes will be cached for attribute read
        operations. If False, user attributes are reloaded from the store prior
        to all attribute read operations.
    read_only : bool, optional
        True if array should be protected against modification.
    object_codec : Codec, optional
        A codec to encode object arrays, only needed if dtype=object.
    dimension_separator : {'.', '/'}, optional
        Separator placed between the dimensions of a chunk.
        Zarr format 2 only. Zarr format 3 arrays should use ``chunk_key_encoding`` instead.
    write_empty_chunks : bool, optional
        Deprecated in favor of the ``config`` keyword argument.
        Pass ``{'write_empty_chunks': <value>}`` to ``create`` instead of using this parameter.
        If True, all chunks will be stored regardless of their
        contents. If False, each chunk is compared to the array's fill value
        prior to storing. If a chunk is uniformly equal to the fill value, then
        that chunk is not be stored, and the store entry for that chunk's key
        is deleted.
    zarr_format : {2, 3, None}, optional
        The Zarr format to use when creating an array. The default is ``None``,
        which instructs Zarr to choose the default Zarr format value defined in the
        runtime configuration.
    meta_array : array-like, optional
        Not implemented.
    attributes : dict[str, JSON], optional
        A dictionary of user attributes to store with the array.
    chunk_shape : int or tuple of ints, optional
        The shape of the Array's chunks (default is None).
        Zarr format 3 only. Zarr format 2 arrays should use `chunks` instead.
    chunk_key_encoding : ChunkKeyEncoding, optional
        A specification of how the chunk keys are represented in storage.
        Zarr format 3 only. Zarr format 2 arrays should use `dimension_separator` instead.
        Default is ``("default", "/")``.
    codecs : Sequence of Codecs or dicts, optional
        An iterable of Codec or dict serializations of Codecs. Zarr V3 only.

        The elements of ``codecs`` specify the transformation from array values to stored bytes.
        Zarr format 3 only. Zarr format 2 arrays should use ``filters`` and ``compressor`` instead.

        If no codecs are provided, default codecs will be used based on the data type of the array.
        For most data types, the default codecs are the tuple ``(BytesCodec(), ZstdCodec())``;
        data types that require a special [`zarr.abc.codec.ArrayBytesCodec`][], like variable-length strings or bytes,
        will use the [`zarr.abc.codec.ArrayBytesCodec`][] required for the data type instead of [`zarr.codecs.BytesCodec`][].
    dimension_names : Iterable[str | None] | None = None
        An iterable of dimension names. Zarr format 3 only.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    config : ArrayConfigLike, optional
        Runtime configuration of the array. If provided, will override the
        default values from `zarr.config.array`.

    Returns
    -------
    z : Array
        The array.
    """
    return Array(
        sync(
            async_api.create(
                shape=shape,
                chunks=chunks,
                dtype=dtype,
                compressor=compressor,
                fill_value=fill_value,
                order=order,
                store=store,
                synchronizer=synchronizer,
                overwrite=overwrite,
                path=path,
                chunk_store=chunk_store,
                filters=filters,
                cache_metadata=cache_metadata,
                cache_attrs=cache_attrs,
                read_only=read_only,
                object_codec=object_codec,
                dimension_separator=dimension_separator,
                write_empty_chunks=write_empty_chunks,
                zarr_version=zarr_version,
                zarr_format=zarr_format,
                meta_array=meta_array,
                attributes=attributes,
                chunk_shape=chunk_shape,
                chunk_key_encoding=chunk_key_encoding,
                codecs=codecs,
                dimension_names=dimension_names,
                storage_options=storage_options,
                config=config,
                **kwargs,
            )
        )
    )


def create_array(
    store: StoreLike,
    *,
    name: str | None = None,
    shape: ShapeLike | None = None,
    dtype: ZDTypeLike | None = None,
    data: np.ndarray[Any, np.dtype[Any]] | None = None,
    chunks: tuple[int, ...] | Literal["auto"] = "auto",
    shards: ShardsLike | None = None,
    filters: FiltersLike = "auto",
    compressors: CompressorsLike = "auto",
    serializer: SerializerLike = "auto",
    fill_value: Any | None = DEFAULT_FILL_VALUE,
    order: MemoryOrder | None = None,
    zarr_format: ZarrFormat | None = 3,
    attributes: dict[str, JSON] | None = None,
    chunk_key_encoding: ChunkKeyEncodingLike | None = None,
    dimension_names: DimensionNames = None,
    storage_options: dict[str, Any] | None = None,
    overwrite: bool = False,
    config: ArrayConfigLike | None = None,
    write_data: bool = True,
) -> AnyArray:
    """Create an array.

    This function wraps [zarr.core.array.create_array][].

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    name : str or None, optional
        The name of the array within the store. If ``name`` is ``None``, the array will be located
        at the root of the store.
    shape : ShapeLike, optional
        Shape of the array. Must be ``None`` if ``data`` is provided.
    dtype : ZDTypeLike | None
        Data type of the array. Must be ``None`` if ``data`` is provided.
    data : np.ndarray, optional
        Array-like data to use for initializing the array. If this parameter is provided, the
        ``shape`` and ``dtype`` parameters must be ``None``.
    chunks : tuple[int, ...] | Literal["auto"], default="auto"
        Chunk shape of the array.
        If chunks is "auto", a chunk shape is guessed based on the shape of the array and the dtype.
    shards : tuple[int, ...], optional
        Shard shape of the array. The default value of ``None`` results in no sharding at all.
    filters : Iterable[Codec] | Literal["auto"], optional
        Iterable of filters to apply to each chunk of the array, in order, before serializing that
        chunk to bytes.

        For Zarr format 3, a "filter" is a codec that takes an array and returns an array,

        and these values must be instances of [`zarr.abc.codec.ArrayArrayCodec`][], or a
        dict representations of [`zarr.abc.codec.ArrayArrayCodec`][].

        For Zarr format 2, a "filter" can be any numcodecs codec; you should ensure that the
        the order if your filters is consistent with the behavior of each filter.

        The default value of ``"auto"`` instructs Zarr to use a default used based on the data
        type of the array and the Zarr format specified. For all data types in Zarr V3, and most
        data types in Zarr V2, the default filters are empty. The only cases where default filters
        are not empty is when the Zarr format is 2, and the data type is a variable-length data type like
        [`zarr.dtype.VariableLengthUTF8`][] or [`zarr.dtype.VariableLengthUTF8`][]. In these cases,
        the default filters contains a single element which is a codec specific to that particular data type.

        To create an array with no filters, provide an empty iterable or the value ``None``.
    compressors : Iterable[Codec], optional
        List of compressors to apply to the array. Compressors are applied in order, and after any
        filters are applied (if any are specified) and the data is serialized into bytes.

        For Zarr format 3, a "compressor" is a codec that takes a bytestream, and
        returns another bytestream. Multiple compressors my be provided for Zarr format 3.
        If no ``compressors`` are provided, a default set of compressors will be used.
        These defaults can be changed by modifying the value of ``array.v3_default_compressors``
        in [`zarr.config`][zarr.config].
        Use ``None`` to omit default compressors.

        For Zarr format 2, a "compressor" can be any numcodecs codec. Only a single compressor may
        be provided for Zarr format 2.
        If no ``compressor`` is provided, a default compressor will be used.
        in [`zarr.config`][zarr.config].
        Use ``None`` to omit the default compressor.
    serializer : dict[str, JSON] | ArrayBytesCodec, optional
        Array-to-bytes codec to use for encoding the array data.
        Zarr format 3 only. Zarr format 2 arrays use implicit array-to-bytes conversion.
        If no ``serializer`` is provided, a default serializer will be used.
        These defaults can be changed by modifying the value of ``array.v3_default_serializer``
        in [`zarr.config`][zarr.config].
    fill_value : Any, optional
        Fill value for the array.
    order : {"C", "F"}, optional
        The memory of the array (default is "C").
        For Zarr format 2, this parameter sets the memory order of the array.
        For Zarr format 3, this parameter is deprecated, because memory order
        is a runtime parameter for Zarr format 3 arrays. The recommended way to specify the memory
        order for Zarr format 3 arrays is via the ``config`` parameter, e.g. ``{'config': 'C'}``.
        If no ``order`` is provided, a default order will be used.
        This default can be changed by modifying the value of ``array.order`` in [`zarr.config`][zarr.config].
    zarr_format : {2, 3}, optional
        The zarr format to use when saving.
    attributes : dict, optional
        Attributes for the array.
    chunk_key_encoding : ChunkKeyEncodingLike, optional
        A specification of how the chunk keys are represented in storage.
        For Zarr format 3, the default is ``{"name": "default", "separator": "/"}}``.
        For Zarr format 2, the default is ``{"name": "v2", "separator": "."}}``.
    dimension_names : Iterable[str], optional
        The names of the dimensions (default is None).
        Zarr format 3 only. Zarr format 2 arrays should not use this parameter.
    storage_options : dict, optional
        If using an fsspec URL to create the store, these will be passed to the backend implementation.
        Ignored otherwise.
    overwrite : bool, default False
        Whether to overwrite an array with the same name in the store, if one exists.
        If ``True``, all existing paths in the store will be deleted.
    config : ArrayConfigLike, optional
        Runtime configuration for the array.
    write_data : bool
        If a pre-existing array-like object was provided to this function via the ``data`` parameter
        then ``write_data`` determines whether the values in that array-like object should be
        written to the Zarr array created by this function. If ``write_data`` is ``False``, then the
        array will be left empty.

    Returns
    -------
    Array
        The array.

    Examples
    --------
    ```python
    import zarr
    store = zarr.storage.MemoryStore()
    arr = zarr.create_array(
        store=store,
        shape=(100,100),
        chunks=(10,10),
        dtype='i4',
        fill_value=0)
    # <Array memory://... shape=(100, 100) dtype=int32>
    ```
    """
    return Array(
        sync(
            zarr.core.array.create_array(
                store,
                name=name,
                shape=shape,
                dtype=dtype,
                data=data,
                chunks=chunks,
                shards=shards,
                filters=filters,
                compressors=compressors,
                serializer=serializer,
                fill_value=fill_value,
                order=order,
                zarr_format=zarr_format,
                attributes=attributes,
                chunk_key_encoding=chunk_key_encoding,
                dimension_names=dimension_names,
                storage_options=storage_options,
                overwrite=overwrite,
                config=config,
                write_data=write_data,
            )
        )
    )


def from_array(
    store: StoreLike,
    *,
    data: AnyArray | npt.ArrayLike,
    write_data: bool = True,
    name: str | None = None,
    chunks: Literal["auto", "keep"] | tuple[int, ...] = "keep",
    shards: ShardsLike | None | Literal["keep"] = "keep",
    filters: FiltersLike | Literal["keep"] = "keep",
    compressors: CompressorsLike | Literal["keep"] = "keep",
    serializer: SerializerLike | Literal["keep"] = "keep",
    fill_value: Any | None = DEFAULT_FILL_VALUE,
    order: MemoryOrder | None = None,
    zarr_format: ZarrFormat | None = None,
    attributes: dict[str, JSON] | None = None,
    chunk_key_encoding: ChunkKeyEncodingLike | None = None,
    dimension_names: DimensionNames = None,
    storage_options: dict[str, Any] | None = None,
    overwrite: bool = False,
    config: ArrayConfigLike | None = None,
) -> AnyArray:
    """Create an array from an existing array or array-like.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    data : Array | array-like
        The array to copy.
    write_data : bool, default True
        Whether to copy the data from the input array to the new array.
        If ``write_data`` is ``False``, the new array will be created with the same metadata as the
        input array, but without any data.
    name : str or None, optional
        The name of the array within the store. If ``name`` is ``None``, the array will be located
        at the root of the store.
    chunks : tuple[int, ...] or "auto" or "keep", optional
        Chunk shape of the array.
        Following values are supported:

        - "auto": Automatically determine the chunk shape based on the array's shape and dtype.
        - "keep": Retain the chunk shape of the data array if it is a zarr Array.
        - tuple[int, ...]: A tuple of integers representing the chunk shape.

        If not specified, defaults to "keep" if data is a zarr Array, otherwise "auto".
    shards : tuple[int, ...], optional
        Shard shape of the array.
        Following values are supported:

        - "auto": Automatically determine the shard shape based on the array's shape and chunk shape.
        - "keep": Retain the shard shape of the data array if it is a zarr Array.
        - tuple[int, ...]: A tuple of integers representing the shard shape.
        - None: No sharding.

        If not specified, defaults to "keep" if data is a zarr Array, otherwise None.
    filters : Iterable[Codec] | Literal["auto", "keep"], optional
        Iterable of filters to apply to each chunk of the array, in order, before serializing that
        chunk to bytes.

        For Zarr format 3, a "filter" is a codec that takes an array and returns an array,
        and these values must be instances of [`zarr.abc.codec.ArrayArrayCodec`][], or a
        dict representations of [`zarr.abc.codec.ArrayArrayCodec`][].

        For Zarr format 2, a "filter" can be any numcodecs codec; you should ensure that the
        the order if your filters is consistent with the behavior of each filter.

        The default value of ``"keep"`` instructs Zarr to infer ``filters`` from ``data``.
        If that inference is not possible, Zarr will fall back to the behavior specified by ``"auto"``,
        which is to choose default filters based on the data type of the array and the Zarr format specified.
        For all data types in Zarr V3, and most data types in Zarr V2, the default filters are the empty tuple ``()``.
        The only cases where default filters are not empty is when the Zarr format is 2, and the
        data type is a variable-length data type like [`zarr.dtype.VariableLengthUTF8`][] or
        [`zarr.dtype.VariableLengthUTF8`][]. In these cases, the default filters is a tuple with a
        single element which is a codec specific to that particular data type.

        To create an array with no filters, provide an empty iterable or the value ``None``.
    compressors : Iterable[Codec] or "auto" or "keep", optional
        List of compressors to apply to the array. Compressors are applied in order, and after any
        filters are applied (if any are specified) and the data is serialized into bytes.

        For Zarr format 3, a "compressor" is a codec that takes a bytestream, and
        returns another bytestream. Multiple compressors my be provided for Zarr format 3.

        For Zarr format 2, a "compressor" can be any numcodecs codec. Only a single compressor may
        be provided for Zarr format 2.

        Following values are supported:

        - Iterable[Codec]: List of compressors to apply to the array.
        - "auto": Automatically determine the compressors based on the array's dtype.
        - "keep": Retain the compressors of the input array if it is a zarr Array.

        If no ``compressors`` are provided, defaults to "keep" if data is a zarr Array, otherwise "auto".
    serializer : dict[str, JSON] | ArrayBytesCodec or "auto" or "keep", optional
        Array-to-bytes codec to use for encoding the array data.
        Zarr format 3 only. Zarr format 2 arrays use implicit array-to-bytes conversion.

        Following values are supported:

        - dict[str, JSON]: A dict representation of an ``ArrayBytesCodec``.
        - ArrayBytesCodec: An instance of ``ArrayBytesCodec``.
        - "auto": a default serializer will be used. These defaults can be changed by modifying the value of
          ``array.v3_default_serializer`` in [`zarr.config`][zarr.config].
        - "keep": Retain the serializer of the input array if it is a zarr Array.

    fill_value : Any, optional
        Fill value for the array.
        If not specified, defaults to the fill value of the data array.
    order : {"C", "F"}, optional
        The memory of the array (default is "C").
        For Zarr format 2, this parameter sets the memory order of the array.
        For Zarr format 3, this parameter is deprecated, because memory order
        is a runtime parameter for Zarr format 3 arrays. The recommended way to specify the memory
        order for Zarr format 3 arrays is via the ``config`` parameter, e.g. ``{'config': 'C'}``.
        If not specified, defaults to the memory order of the data array.
    zarr_format : {2, 3}, optional
        The zarr format to use when saving.
        If not specified, defaults to the zarr format of the data array.
    attributes : dict, optional
        Attributes for the array.
        If not specified, defaults to the attributes of the data array.
    chunk_key_encoding : ChunkKeyEncoding, optional
        A specification of how the chunk keys are represented in storage.
        For Zarr format 3, the default is ``{"name": "default", "separator": "/"}}``.
        For Zarr format 2, the default is ``{"name": "v2", "separator": "."}}``.
        If not specified and the data array has the same zarr format as the target array,
        the chunk key encoding of the data array is used.
    dimension_names : Iterable[str | None] | None
        The names of the dimensions (default is None).
        Zarr format 3 only. Zarr format 2 arrays should not use this parameter.
        If not specified, defaults to the dimension names of the data array.
    storage_options : dict, optional
        If using an fsspec URL to create the store, these will be passed to the backend implementation.
        Ignored otherwise.
    overwrite : bool, default False
        Whether to overwrite an array with the same name in the store, if one exists.
    config : ArrayConfig or ArrayConfigLike, optional
        Runtime configuration for the array.

    Returns
    -------
    Array
        The array.

    Examples
    --------
    Create an array from an existing Array:

    ```python
    import zarr
    store = zarr.storage.MemoryStore()
    store2 = zarr.storage.LocalStore('example_from_array.zarr')
    arr = zarr.create_array(
        store=store,
        shape=(100,100),
        chunks=(10,10),
        dtype='int32',
        fill_value=0)
    arr2 = zarr.from_array(store2, data=arr, overwrite=True)
    # <Array file://example_from_array.zarr shape=(100, 100) dtype=int32>
    ```

    Create an array from an existing NumPy array:

    ```python
    import zarr
    import numpy as np
    arr3 = zarr.from_array(
            zarr.storage.MemoryStore(),
        data=np.arange(10000, dtype='i4').reshape(100, 100),
    )
    # <Array memory://... shape=(100, 100) dtype=int32>
    ```

    Create an array from any array-like object:

    ```python
    import zarr
    arr4 = zarr.from_array(
        zarr.storage.MemoryStore(),
        data=[[1, 2], [3, 4]],
    )
    # <Array memory://... shape=(2, 2) dtype=int64>
    arr4[...]
    # array([[1, 2],[3, 4]])
    ```

    Create an array from an existing Array without copying the data:

    ```python
    import zarr
    arr4 = zarr.from_array(
        zarr.storage.MemoryStore(),
        data=[[1, 2], [3, 4]],
    )
    arr5 = zarr.from_array(
        zarr.storage.MemoryStore(),
        data=arr4,
        write_data=False,
    )
    # <Array memory://... shape=(2, 2) dtype=int64>
    arr5[...]
    # array([[0, 0],[0, 0]])
    ```
    """
    return Array(
        sync(
            zarr.core.array.from_array(
                store,
                data=data,
                write_data=write_data,
                name=name,
                chunks=chunks,
                shards=shards,
                filters=filters,
                compressors=compressors,
                serializer=serializer,
                fill_value=fill_value,
                order=order,
                zarr_format=zarr_format,
                attributes=attributes,
                chunk_key_encoding=chunk_key_encoding,
                dimension_names=dimension_names,
                storage_options=storage_options,
                overwrite=overwrite,
                config=config,
            )
        )
    )


# TODO: add type annotations for kwargs
def empty(shape: tuple[int, ...], **kwargs: Any) -> AnyArray:
    """Create an empty array with the specified shape. The contents will be filled with the
    array's fill value or zeros if no fill value is provided.

    Parameters
    ----------
    shape : int or tuple of int
        Shape of the empty array.
    **kwargs
        Keyword arguments passed to [`create`][zarr.api.asynchronous.create].

    Returns
    -------
    Array
        The new array.

    Notes
    -----
    The contents of an empty Zarr array are not defined. On attempting to
    retrieve data from an empty Zarr array, any values may be returned,
    and these are not guaranteed to be stable from one access to the next.
    """
    return Array(sync(async_api.empty(shape, **kwargs)))


# TODO: move ArrayLike to common module
# TODO: add type annotations for kwargs
def empty_like(a: ArrayLike, **kwargs: Any) -> AnyArray:
    """Create an empty array like another array. The contents will be filled with the
    array's fill value or zeros if no fill value is provided.

    Parameters
    ----------
    a : array-like
        The array to create an empty array like.
    **kwargs
        Keyword arguments passed to [`create`][zarr.api.asynchronous.create].

    Returns
    -------
    Array
        The new array.

    Notes
    -----
    The contents of an empty Zarr array are not defined. On attempting to
    retrieve data from an empty Zarr array, any values may be returned,
    and these are not guaranteed to be stable from one access to the next.
    """
    return Array(sync(async_api.empty_like(a, **kwargs)))


# TODO: add type annotations for kwargs and fill_value
def full(shape: tuple[int, ...], fill_value: Any, **kwargs: Any) -> AnyArray:
    """Create an array with a default fill value.

    Parameters
    ----------
    shape : int or tuple of int
        Shape of the empty array.
    fill_value : scalar
        Fill value.
    **kwargs
        Keyword arguments passed to [`create`][zarr.api.asynchronous.create].

    Returns
    -------
    Array
        The new array.
    """
    return Array(sync(async_api.full(shape=shape, fill_value=fill_value, **kwargs)))


# TODO: move ArrayLike to common module
# TODO: add type annotations for kwargs
def full_like(a: ArrayLike, **kwargs: Any) -> AnyArray:
    """Create a filled array like another array.

    Parameters
    ----------
    a : array-like
        The array to create an empty array like.
    **kwargs
        Keyword arguments passed to [`zarr.api.asynchronous.create`][].

    Returns
    -------
    Array
        The new array.
    """
    return Array(sync(async_api.full_like(a, **kwargs)))


# TODO: add type annotations for kwargs
def ones(shape: tuple[int, ...], **kwargs: Any) -> AnyArray:
    """Create an array with a fill value of one.

    Parameters
    ----------
    shape : int or tuple of int
        Shape of the empty array.
    **kwargs
        Keyword arguments passed to [`zarr.api.asynchronous.create`][].

    Returns
    -------
    Array
        The new array.
    """
    return Array(sync(async_api.ones(shape, **kwargs)))


# TODO: add type annotations for kwargs
def ones_like(a: ArrayLike, **kwargs: Any) -> AnyArray:
    """Create an array of ones like another array.

    Parameters
    ----------
    a : array-like
        The array to create an empty array like.
    **kwargs
        Keyword arguments passed to [`zarr.api.asynchronous.create`][].

    Returns
    -------
    Array
        The new array.
    """
    return Array(sync(async_api.ones_like(a, **kwargs)))


# TODO: update this once async_api.open_array is fully implemented
def open_array(
    store: StoreLike | None = None,
    *,
    zarr_version: ZarrFormat | None = None,
    zarr_format: ZarrFormat | None = None,
    path: PathLike = "",
    storage_options: dict[str, Any] | None = None,
    **kwargs: Any,
) -> AnyArray:
    """Open an array using file-mode-like semantics.

    Parameters
    ----------
    store : StoreLike
        StoreLike object to open. See the
        [storage documentation in the user guide][user-guide-store-like]
        for a description of all valid StoreLike values.
    zarr_version : {2, 3, None}, optional
        The zarr format to use when saving. Deprecated in favor of zarr_format.
    zarr_format : {2, 3, None}, optional
        The zarr format to use when saving.
    path : str, optional
        Path in store to array.
    storage_options : dict
        If using an fsspec URL to create the store, these will be passed to
        the backend implementation. Ignored otherwise.
    **kwargs
        Any keyword arguments to pass to [`create`][zarr.api.asynchronous.create].


    Returns
    -------
    AsyncArray
        The opened array.
    """
    return Array(
        sync(
            async_api.open_array(
                store=store,
                zarr_version=zarr_version,
                zarr_format=zarr_format,
                path=path,
                storage_options=storage_options,
                **kwargs,
            )
        )
    )


# TODO: add type annotations for kwargs
def open_like(a: ArrayLike, path: str, **kwargs: Any) -> AnyArray:
    """Open a persistent array like another array.

    Parameters
    ----------
    a : Array
        The shape and data-type of a define these same attributes of the returned array.
    path : str
        The path to the new array.
    **kwargs
        Any keyword arguments to pass to the array constructor.

    Returns
    -------
    AsyncArray
        The opened array.
    """
    return Array(sync(async_api.open_like(a, path=path, **kwargs)))


# TODO: add type annotations for kwargs
def zeros(shape: tuple[int, ...], **kwargs: Any) -> AnyArray:
    """Create an array with a fill value of zero.

    Parameters
    ----------
    shape : int or tuple of int
        Shape of the empty array.
    **kwargs
        Keyword arguments passed to [`zarr.api.asynchronous.create`][].

    Returns
    -------
    Array
        The new array.
    """
    return Array(sync(async_api.zeros(shape=shape, **kwargs)))


# TODO: add type annotations for kwargs
def zeros_like(a: ArrayLike, **kwargs: Any) -> AnyArray:
    """Create an array of zeros like another array.

    Parameters
    ----------
    a : array-like
        The array to create an empty array like.
    **kwargs
        Keyword arguments passed to [`create`][zarr.api.asynchronous.create].

    Returns
    -------
    Array
        The new array.
    """
    return Array(sync(async_api.zeros_like(a, **kwargs)))
