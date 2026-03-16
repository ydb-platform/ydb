import math
import sys
from collections.abc import Callable, Mapping
from typing import Any, Literal

import hypothesis.extra.numpy as npst
import hypothesis.strategies as st
import numpy as np
import numpy.typing as npt
from hypothesis import event
from hypothesis.strategies import SearchStrategy

import zarr
from zarr.abc.store import RangeByteRequest, Store
from zarr.codecs.bytes import BytesCodec
from zarr.core.array import Array
from zarr.core.chunk_grids import RegularChunkGrid
from zarr.core.chunk_key_encodings import DefaultChunkKeyEncoding
from zarr.core.common import JSON, ZarrFormat
from zarr.core.dtype import get_data_type_from_native_dtype
from zarr.core.metadata import ArrayV2Metadata, ArrayV3Metadata
from zarr.core.sync import sync
from zarr.storage import MemoryStore, StoreLike
from zarr.storage._common import _dereference_path
from zarr.storage._utils import normalize_path
from zarr.types import AnyArray

# Copied from Xarray
_attr_keys = st.text(st.characters(), min_size=1)
_attr_values = st.recursive(
    st.none() | st.booleans() | st.text(st.characters(), max_size=5),
    lambda children: st.lists(children) | st.dictionaries(_attr_keys, children),
    max_leaves=3,
)


@st.composite
def keys(draw: st.DrawFn, *, max_num_nodes: int | None = None) -> str:
    return draw(st.lists(node_names, min_size=1, max_size=max_num_nodes).map("/".join))


@st.composite
def paths(draw: st.DrawFn, *, max_num_nodes: int | None = None) -> str:
    return draw(st.just("/") | keys(max_num_nodes=max_num_nodes))


def dtypes() -> st.SearchStrategy[np.dtype[Any]]:
    return (
        npst.boolean_dtypes()
        | npst.integer_dtypes(endianness="=")
        | npst.unsigned_integer_dtypes(endianness="=")
        | npst.floating_dtypes(endianness="=")
        | npst.complex_number_dtypes(endianness="=")
        | npst.byte_string_dtypes(endianness="=")
        | npst.unicode_string_dtypes(endianness="=")
        | npst.datetime64_dtypes(endianness="=")
        | npst.timedelta64_dtypes(endianness="=")
    )


def v3_dtypes() -> st.SearchStrategy[np.dtype[Any]]:
    return dtypes()


def v2_dtypes() -> st.SearchStrategy[np.dtype[Any]]:
    return dtypes()


def safe_unicode_for_dtype(dtype: np.dtype[np.str_]) -> st.SearchStrategy[str]:
    """Generate UTF-8-safe text constrained to max_len of dtype."""
    # account for utf-32 encoding (i.e. 4 bytes/character)
    max_len = max(1, dtype.itemsize // 4)

    return st.text(
        alphabet=st.characters(
            exclude_categories=["Cs"],  # Avoid *technically allowed* surrogates
            min_codepoint=32,
        ),
        min_size=1,
        max_size=max_len,
    )


def clear_store(x: Store) -> Store:
    sync(x.clear())
    return x


# From https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#node-names
# 1. must not be the empty string ("")
# 2. must not include the character "/"
# 3. must not be a string composed only of period characters, e.g. "." or ".."
# 4. must not start with the reserved prefix "__"
zarr_key_chars = st.sampled_from(
    ".-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
)
node_names = (
    st.text(zarr_key_chars, min_size=1)
    .filter(lambda t: t not in (".", "..") and not t.startswith("__"))
    .filter(lambda name: name.lower() != "zarr.json")
)
short_node_names = (
    st.text(zarr_key_chars, max_size=3, min_size=1)
    .filter(lambda t: t not in (".", "..") and not t.startswith("__"))
    .filter(lambda name: name.lower() != "zarr.json")
)
array_names = node_names
attrs: st.SearchStrategy[Mapping[str, JSON] | None] = st.none() | st.dictionaries(
    _attr_keys, _attr_values
)
# st.builds will only call a new store constructor for different keyword arguments
# i.e. stores.examples() will always return the same object per Store class.
# So we map a clear to reset the store.
stores = st.builds(MemoryStore, st.just({})).map(clear_store)
compressors = st.sampled_from([None, "default"])
zarr_formats: st.SearchStrategy[ZarrFormat] = st.sampled_from([3, 2])
# We de-prioritize arrays having dim sizes 0, 1, 2
array_shapes = npst.array_shapes(max_dims=4, min_side=3, max_side=5) | npst.array_shapes(
    max_dims=4, min_side=0
)


@st.composite
def dimension_names(draw: st.DrawFn, *, ndim: int | None = None) -> list[None | str] | None:
    simple_text = st.text(zarr_key_chars, min_size=0)
    return draw(st.none() | st.lists(st.none() | simple_text, min_size=ndim, max_size=ndim))  # type: ignore[arg-type]


@st.composite
def array_metadata(
    draw: st.DrawFn,
    *,
    array_shapes: Callable[..., st.SearchStrategy[tuple[int, ...]]] = npst.array_shapes,
    zarr_formats: st.SearchStrategy[Literal[2, 3]] = zarr_formats,
    attributes: SearchStrategy[Mapping[str, JSON] | None] = attrs,
) -> ArrayV2Metadata | ArrayV3Metadata:
    zarr_format = draw(zarr_formats)
    # separator = draw(st.sampled_from(['/', '\\']))
    shape = draw(array_shapes())
    ndim = len(shape)
    chunk_shape = draw(array_shapes(min_dims=ndim, max_dims=ndim))
    np_dtype = draw(dtypes())
    dtype = get_data_type_from_native_dtype(np_dtype)
    fill_value = draw(npst.from_dtype(np_dtype))
    if zarr_format == 2:
        return ArrayV2Metadata(
            shape=shape,
            chunks=chunk_shape,
            dtype=dtype,
            fill_value=fill_value,
            order=draw(st.sampled_from(["C", "F"])),
            attributes=draw(attributes),  # type: ignore[arg-type]
            dimension_separator=draw(st.sampled_from([".", "/"])),
            filters=None,
            compressor=None,
        )
    else:
        return ArrayV3Metadata(
            shape=shape,
            data_type=dtype,
            chunk_grid=RegularChunkGrid(chunk_shape=chunk_shape),
            fill_value=fill_value,
            attributes=draw(attributes),  # type: ignore[arg-type]
            dimension_names=draw(dimension_names(ndim=ndim)),
            chunk_key_encoding=DefaultChunkKeyEncoding(separator="/"),  # FIXME
            codecs=[BytesCodec()],
            storage_transformers=(),
        )


@st.composite
def numpy_arrays(
    draw: st.DrawFn,
    *,
    shapes: st.SearchStrategy[tuple[int, ...]] = array_shapes,
    dtype: np.dtype[Any] | None = None,
) -> npt.NDArray[Any]:
    """
    Generate numpy arrays that can be saved in the provided Zarr format.
    """
    if dtype is None:
        dtype = draw(dtypes())
    if np.issubdtype(dtype, np.str_):
        safe_unicode_strings = safe_unicode_for_dtype(dtype)
        return draw(npst.arrays(dtype=dtype, shape=shapes, elements=safe_unicode_strings))

    return draw(npst.arrays(dtype=dtype, shape=shapes))


@st.composite
def chunk_shapes(draw: st.DrawFn, *, shape: tuple[int, ...]) -> tuple[int, ...]:
    # We want this strategy to shrink towards arrays with smaller number of chunks
    # 1. st.integers() shrinks towards smaller values. So we use that to generate number of chunks
    numchunks = draw(
        st.tuples(*[st.integers(min_value=0 if size == 0 else 1, max_value=size) for size in shape])
    )
    # 2. and now generate the chunks tuple
    chunks = tuple(
        size // nchunks if nchunks > 0 else 0
        for size, nchunks in zip(shape, numchunks, strict=True)
    )

    for c in chunks:
        event("chunk size", c)

    if any((c != 0 and s % c != 0) for s, c in zip(shape, chunks, strict=True)):
        event("smaller last chunk")

    return chunks


@st.composite
def shard_shapes(
    draw: st.DrawFn, *, shape: tuple[int, ...], chunk_shape: tuple[int, ...]
) -> tuple[int, ...]:
    # We want this strategy to shrink towards arrays with smaller number of shards
    # shards must be an integral number of chunks
    assert all(c != 0 for c in chunk_shape)
    numchunks = tuple(s // c for s, c in zip(shape, chunk_shape, strict=True))
    multiples = tuple(draw(st.integers(min_value=1, max_value=nc)) for nc in numchunks)
    return tuple(m * c for m, c in zip(multiples, chunk_shape, strict=True))


@st.composite
def np_array_and_chunks(
    draw: st.DrawFn,
    *,
    arrays: st.SearchStrategy[npt.NDArray[Any]] = numpy_arrays(),  # noqa: B008
) -> tuple[np.ndarray, tuple[int, ...]]:  # type: ignore[type-arg]
    """A hypothesis strategy to generate small sized random arrays.

    Returns: a tuple of the array and a suitable random chunking for it.
    """
    array = draw(arrays)
    return (array, draw(chunk_shapes(shape=array.shape)))


@st.composite
def arrays(
    draw: st.DrawFn,
    *,
    shapes: st.SearchStrategy[tuple[int, ...]] = array_shapes,
    compressors: st.SearchStrategy = compressors,
    stores: st.SearchStrategy[StoreLike] = stores,
    paths: st.SearchStrategy[str] = paths(),  # noqa: B008
    array_names: st.SearchStrategy = array_names,
    arrays: st.SearchStrategy | None = None,
    attrs: st.SearchStrategy = attrs,
    zarr_formats: st.SearchStrategy = zarr_formats,
) -> AnyArray:
    store = draw(stores, label="store")
    path = draw(paths, label="array parent")
    name = draw(array_names, label="array name")
    attributes = draw(attrs, label="attributes")
    zarr_format = draw(zarr_formats, label="zarr format")
    if arrays is None:
        arrays = numpy_arrays(shapes=shapes)
    nparray = draw(arrays, label="array data")
    chunk_shape = draw(chunk_shapes(shape=nparray.shape), label="chunk shape")
    dim_names: None | list[str | None] = None
    if zarr_format == 3 and all(c > 0 for c in chunk_shape):
        shard_shape = draw(
            st.none() | shard_shapes(shape=nparray.shape, chunk_shape=chunk_shape),
            label="shard shape",
        )
        dim_names = draw(dimension_names(ndim=nparray.ndim), label="dimension names")
    else:
        shard_shape = None
    # test that None works too.
    fill_value = draw(st.one_of([st.none(), npst.from_dtype(nparray.dtype)]))
    # compressor = draw(compressors)

    expected_attrs = {} if attributes is None else attributes

    array_path = _dereference_path(path, name)
    root = zarr.open_group(store, mode="w", zarr_format=zarr_format)

    a = root.create_array(
        array_path,
        shape=nparray.shape,
        chunks=chunk_shape,
        shards=shard_shape,
        dtype=nparray.dtype,
        attributes=attributes,
        # compressor=compressor,  # FIXME
        fill_value=fill_value,
        dimension_names=dim_names,
    )

    assert isinstance(a, Array)
    if a.metadata.zarr_format == 3:
        assert a.fill_value is not None
    assert a.name is not None
    assert a.path == normalize_path(array_path)
    assert a.name == "/" + a.path
    assert isinstance(root[array_path], Array)
    assert nparray.shape == a.shape
    assert chunk_shape == a.chunks
    assert shard_shape == a.shards
    assert a.basename == name, (a.basename, name)
    assert dict(a.attrs) == expected_attrs

    a[:] = nparray

    return a


@st.composite
def simple_arrays(
    draw: st.DrawFn,
    *,
    shapes: st.SearchStrategy[tuple[int, ...]] = array_shapes,
) -> Any:
    return draw(
        arrays(
            shapes=shapes,
            paths=paths(max_num_nodes=2),
            array_names=short_node_names,
            attrs=st.none(),
            compressors=st.sampled_from([None, "default"]),
        )
    )


def is_negative_slice(idx: Any) -> bool:
    return isinstance(idx, slice) and idx.step is not None and idx.step < 0


@st.composite
def end_slices(draw: st.DrawFn, *, shape: tuple[int, ...]) -> Any:
    """
    A strategy that slices ranges that include the last chunk.
    This is intended to stress-test handling of a possibly smaller last chunk.
    """
    slicers = []
    for size in shape:
        start = draw(st.integers(min_value=size // 2, max_value=size - 1))
        length = draw(st.integers(min_value=0, max_value=size - start))
        slicers.append(slice(start, start + length))
    event("drawing end slice")
    return tuple(slicers)


@st.composite
def basic_indices(
    draw: st.DrawFn,
    *,
    shape: tuple[int, ...],
    min_dims: int = 0,
    max_dims: int | None = None,
    allow_newaxis: bool = False,
    allow_ellipsis: bool = True,
) -> Any:
    """Basic indices without unsupported negative slices."""
    strategy = npst.basic_indices(
        shape=shape,
        min_dims=min_dims,
        max_dims=max_dims,
        allow_newaxis=allow_newaxis,
        allow_ellipsis=allow_ellipsis,
    ).filter(
        lambda idxr: (
            not (
                is_negative_slice(idxr)
                or (isinstance(idxr, tuple) and any(is_negative_slice(idx) for idx in idxr))  # type: ignore[redundant-expr]
            )
        )
    )
    if math.prod(shape) >= 3:
        strategy = end_slices(shape=shape) | strategy
    return draw(strategy)


@st.composite
def orthogonal_indices(
    draw: st.DrawFn, *, shape: tuple[int, ...]
) -> tuple[tuple[np.ndarray[Any, Any], ...], tuple[np.ndarray[Any, Any], ...]]:
    """
    Strategy that returns
    (1) a tuple of integer arrays used for orthogonal indexing of Zarr arrays.
    (2) a tuple of integer arrays that can be used for equivalent indexing of numpy arrays
    """
    zindexer = []
    npindexer = []
    ndim = len(shape)
    for axis, size in enumerate(shape):
        if size != 0:
            strategy = npst.integer_array_indices(
                shape=(size,), result_shape=npst.array_shapes(min_side=1, max_side=size, max_dims=1)
            ) | basic_indices(min_dims=1, shape=(size,), allow_ellipsis=False)
        else:
            strategy = basic_indices(min_dims=1, shape=(size,), allow_ellipsis=False)

        val = draw(
            strategy
            # bare ints, slices
            .map(lambda x: (x,) if not isinstance(x, tuple) else x)
            # skip empty tuple
            .filter(bool)
        )
        (idxr,) = val
        if isinstance(idxr, int):
            idxr = np.array([idxr])
        zindexer.append(idxr)
        if isinstance(idxr, slice):
            idxr = np.arange(*idxr.indices(size))
        elif isinstance(idxr, (tuple, int)):
            idxr = np.array(idxr)
        newshape = [1] * ndim
        newshape[axis] = idxr.size
        npindexer.append(idxr.reshape(newshape))

    # casting the output of broadcast_arrays is needed for numpy < 2
    return tuple(zindexer), tuple(np.broadcast_arrays(*npindexer))


def key_ranges(
    keys: SearchStrategy[str] = node_names, max_size: int = sys.maxsize
) -> SearchStrategy[list[tuple[str, RangeByteRequest]]]:
    """
    Function to generate key_ranges strategy for get_partial_values()
    returns list strategy w/ form::

        [(key, (range_start, range_end)),
         (key, (range_start, range_end)),...]
    """

    def make_request(start: int, length: int) -> RangeByteRequest:
        return RangeByteRequest(start, end=min(start + length, max_size))

    byte_ranges = st.builds(
        make_request,
        start=st.integers(min_value=0, max_value=max_size),
        length=st.integers(min_value=0, max_value=max_size),
    )
    key_tuple = st.tuples(keys, byte_ranges)
    return st.lists(key_tuple, min_size=1, max_size=10)


@st.composite
def chunk_paths(draw: st.DrawFn, ndim: int, numblocks: tuple[int, ...], subset: bool = True) -> str:
    blockidx = draw(
        st.tuples(*tuple(st.integers(min_value=0, max_value=max(0, b - 1)) for b in numblocks))
    )
    subset_slicer = slice(draw(st.integers(min_value=0, max_value=ndim))) if subset else slice(None)
    return "/".join(map(str, blockidx[subset_slicer]))
