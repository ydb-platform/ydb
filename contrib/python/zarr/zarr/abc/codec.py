from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping
from typing import TYPE_CHECKING, Generic, TypeGuard, TypeVar

from typing_extensions import ReadOnly, TypedDict

from zarr.abc.metadata import Metadata
from zarr.core.buffer import Buffer, NDBuffer
from zarr.core.common import NamedConfig, concurrent_map
from zarr.core.config import config

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable
    from typing import Self

    from zarr.abc.store import ByteGetter, ByteSetter, Store
    from zarr.core.array_spec import ArraySpec
    from zarr.core.chunk_grids import ChunkGrid
    from zarr.core.dtype.wrapper import TBaseDType, TBaseScalar, ZDType
    from zarr.core.indexing import SelectorTuple
    from zarr.core.metadata import ArrayMetadata

__all__ = [
    "ArrayArrayCodec",
    "ArrayBytesCodec",
    "ArrayBytesCodecPartialDecodeMixin",
    "ArrayBytesCodecPartialEncodeMixin",
    "BaseCodec",
    "BytesBytesCodec",
    "CodecInput",
    "CodecOutput",
    "CodecPipeline",
]

CodecInput = TypeVar("CodecInput", bound=NDBuffer | Buffer)
CodecOutput = TypeVar("CodecOutput", bound=NDBuffer | Buffer)

TName = TypeVar("TName", bound=str, covariant=True)


class CodecJSON_V2(TypedDict, Generic[TName]):
    """The JSON representation of a codec for Zarr V2"""

    id: ReadOnly[TName]


def _check_codecjson_v2(data: object) -> TypeGuard[CodecJSON_V2[str]]:
    return isinstance(data, Mapping) and "id" in data and isinstance(data["id"], str)


CodecJSON_V3 = str | NamedConfig[str, Mapping[str, object]]
"""The JSON representation of a codec for Zarr V3."""

# The widest type we will *accept* for a codec JSON
# This covers v2 and v3
CodecJSON = str | Mapping[str, object]
"""The widest type of JSON-like input that could specify a codec."""


class BaseCodec(Metadata, Generic[CodecInput, CodecOutput]):
    """Generic base class for codecs.

    Codecs can be registered via zarr.codecs.registry.

    Warnings
    --------
    This class is not intended to be directly, please use
    ArrayArrayCodec, ArrayBytesCodec or BytesBytesCodec for subclassing.
    """

    is_fixed_size: bool

    @abstractmethod
    def compute_encoded_size(self, input_byte_length: int, chunk_spec: ArraySpec) -> int:
        """Given an input byte length, this method returns the output byte length.
        Raises a NotImplementedError for codecs with variable-sized outputs (e.g. compressors).

        Parameters
        ----------
        input_byte_length : int
        chunk_spec : ArraySpec

        Returns
        -------
        int
        """
        ...

    def resolve_metadata(self, chunk_spec: ArraySpec) -> ArraySpec:
        """Computed the spec of the chunk after it has been encoded by the codec.
        This is important for codecs that change the shape, data type or fill value of a chunk.
        The spec will then be used for subsequent codecs in the pipeline.

        Parameters
        ----------
        chunk_spec : ArraySpec

        Returns
        -------
        ArraySpec
        """
        return chunk_spec

    def evolve_from_array_spec(self, array_spec: ArraySpec) -> Self:
        """Fills in codec configuration parameters that can be automatically
        inferred from the array metadata.

        Parameters
        ----------
        array_spec : ArraySpec

        Returns
        -------
        Self
        """
        return self

    def validate(
        self,
        *,
        shape: tuple[int, ...],
        dtype: ZDType[TBaseDType, TBaseScalar],
        chunk_grid: ChunkGrid,
    ) -> None:
        """Validates that the codec configuration is compatible with the array metadata.
        Raises errors when the codec configuration is not compatible.

        Parameters
        ----------
        shape : tuple[int, ...]
            The array shape
        dtype : np.dtype[Any]
            The array data type
        chunk_grid : ChunkGrid
            The array chunk grid
        """

    async def _decode_single(self, chunk_data: CodecOutput, chunk_spec: ArraySpec) -> CodecInput:
        raise NotImplementedError  # pragma: no cover

    async def decode(
        self,
        chunks_and_specs: Iterable[tuple[CodecOutput | None, ArraySpec]],
    ) -> Iterable[CodecInput | None]:
        """Decodes a batch of chunks.
        Chunks can be None in which case they are ignored by the codec.

        Parameters
        ----------
        chunks_and_specs : Iterable[tuple[CodecOutput | None, ArraySpec]]
            Ordered set of encoded chunks with their accompanying chunk spec.

        Returns
        -------
        Iterable[CodecInput | None]
        """
        return await _batching_helper(self._decode_single, chunks_and_specs)

    async def _encode_single(
        self, chunk_data: CodecInput, chunk_spec: ArraySpec
    ) -> CodecOutput | None:
        raise NotImplementedError  # pragma: no cover

    async def encode(
        self,
        chunks_and_specs: Iterable[tuple[CodecInput | None, ArraySpec]],
    ) -> Iterable[CodecOutput | None]:
        """Encodes a batch of chunks.
        Chunks can be None in which case they are ignored by the codec.

        Parameters
        ----------
        chunks_and_specs : Iterable[tuple[CodecInput | None, ArraySpec]]
            Ordered set of to-be-encoded chunks with their accompanying chunk spec.

        Returns
        -------
        Iterable[CodecOutput | None]
        """
        return await _batching_helper(self._encode_single, chunks_and_specs)


class ArrayArrayCodec(BaseCodec[NDBuffer, NDBuffer]):
    """Base class for array-to-array codecs."""


class ArrayBytesCodec(BaseCodec[NDBuffer, Buffer]):
    """Base class for array-to-bytes codecs."""


class BytesBytesCodec(BaseCodec[Buffer, Buffer]):
    """Base class for bytes-to-bytes codecs."""


Codec = ArrayArrayCodec | ArrayBytesCodec | BytesBytesCodec


class ArrayBytesCodecPartialDecodeMixin:
    """Mixin for array-to-bytes codecs that implement partial decoding."""

    async def _decode_partial_single(
        self, byte_getter: ByteGetter, selection: SelectorTuple, chunk_spec: ArraySpec
    ) -> NDBuffer | None:
        raise NotImplementedError

    async def decode_partial(
        self,
        batch_info: Iterable[tuple[ByteGetter, SelectorTuple, ArraySpec]],
    ) -> Iterable[NDBuffer | None]:
        """Partially decodes a batch of chunks.
        This method determines parts of a chunk from the slice selection,
        fetches these parts from the store (via ByteGetter) and decodes them.

        Parameters
        ----------
        batch_info : Iterable[tuple[ByteGetter, SelectorTuple, ArraySpec]]
            Ordered set of information about slices of encoded chunks.
            The slice selection determines which parts of the chunk will be fetched.
            The ByteGetter is used to fetch the necessary bytes.
            The chunk spec contains information about the construction of an array from the bytes.

        Returns
        -------
        Iterable[NDBuffer | None]
        """
        return await concurrent_map(
            list(batch_info),
            self._decode_partial_single,
            config.get("async.concurrency"),
        )


class ArrayBytesCodecPartialEncodeMixin:
    """Mixin for array-to-bytes codecs that implement partial encoding."""

    async def _encode_partial_single(
        self,
        byte_setter: ByteSetter,
        chunk_array: NDBuffer,
        selection: SelectorTuple,
        chunk_spec: ArraySpec,
    ) -> None:
        raise NotImplementedError  # pragma: no cover

    async def encode_partial(
        self,
        batch_info: Iterable[tuple[ByteSetter, NDBuffer, SelectorTuple, ArraySpec]],
    ) -> None:
        """Partially encodes a batch of chunks.
        This method determines parts of a chunk from the slice selection, encodes them and
        writes these parts to the store (via ByteSetter).
        If merging with existing chunk data in the store is necessary, this method will
        read from the store first and perform the merge.

        Parameters
        ----------
        batch_info : Iterable[tuple[ByteSetter, NDBuffer, SelectorTuple, ArraySpec]]
            Ordered set of information about slices of to-be-encoded chunks.
            The slice selection determines which parts of the chunk will be encoded.
            The ByteSetter is used to write the necessary bytes and fetch bytes for existing chunk data.
            The chunk spec contains information about the chunk.
        """
        await concurrent_map(
            list(batch_info),
            self._encode_partial_single,
            config.get("async.concurrency"),
        )


class CodecPipeline:
    """Base class for implementing CodecPipeline.
    A CodecPipeline implements the read and write paths for chunk data.
    On the read path, it is responsible for fetching chunks from a store (via ByteGetter),
    decoding them and assembling an output array. On the write path, it encodes the chunks
    and writes them to a store (via ByteSetter)."""

    @abstractmethod
    def evolve_from_array_spec(self, array_spec: ArraySpec) -> Self:
        """Fills in codec configuration parameters that can be automatically
        inferred from the array metadata.

        Parameters
        ----------
        array_spec : ArraySpec

        Returns
        -------
        Self
        """
        ...

    @classmethod
    @abstractmethod
    def from_codecs(cls, codecs: Iterable[Codec]) -> Self:
        """Creates a codec pipeline from an iterable of codecs.

        Parameters
        ----------
        codecs : Iterable[Codec]

        Returns
        -------
        Self
        """
        ...

    @classmethod
    def from_array_metadata_and_store(cls, array_metadata: ArrayMetadata, store: Store) -> Self:
        """Creates a codec pipeline from array metadata and a store path.

        Raises NotImplementedError by default, indicating the CodecPipeline must be created with from_codecs instead.

        Parameters
        ----------
        array_metadata : ArrayMetadata
        store : Store

        Returns
        -------
        Self
        """
        raise NotImplementedError(
            f"'{type(cls).__name__}' does not implement CodecPipeline.from_array_metadata_and_store."
        )

    @property
    @abstractmethod
    def supports_partial_decode(self) -> bool: ...

    @property
    @abstractmethod
    def supports_partial_encode(self) -> bool: ...

    @abstractmethod
    def validate(
        self,
        *,
        shape: tuple[int, ...],
        dtype: ZDType[TBaseDType, TBaseScalar],
        chunk_grid: ChunkGrid,
    ) -> None:
        """Validates that all codec configurations are compatible with the array metadata.
        Raises errors when a codec configuration is not compatible.

        Parameters
        ----------
        shape : tuple[int, ...]
            The array shape
        dtype : np.dtype[Any]
            The array data type
        chunk_grid : ChunkGrid
            The array chunk grid
        """
        ...

    @abstractmethod
    def compute_encoded_size(self, byte_length: int, array_spec: ArraySpec) -> int:
        """Given an input byte length, this method returns the output byte length.
        Raises a NotImplementedError for codecs with variable-sized outputs (e.g. compressors).

        Parameters
        ----------
        byte_length : int
        array_spec : ArraySpec

        Returns
        -------
        int
        """
        ...

    @abstractmethod
    async def decode(
        self,
        chunk_bytes_and_specs: Iterable[tuple[Buffer | None, ArraySpec]],
    ) -> Iterable[NDBuffer | None]:
        """Decodes a batch of chunks.
        Chunks can be None in which case they are ignored by the codec.

        Parameters
        ----------
        chunk_bytes_and_specs : Iterable[tuple[Buffer | None, ArraySpec]]
            Ordered set of encoded chunks with their accompanying chunk spec.

        Returns
        -------
        Iterable[NDBuffer | None]
        """
        ...

    @abstractmethod
    async def encode(
        self,
        chunk_arrays_and_specs: Iterable[tuple[NDBuffer | None, ArraySpec]],
    ) -> Iterable[Buffer | None]:
        """Encodes a batch of chunks.
        Chunks can be None in which case they are ignored by the codec.

        Parameters
        ----------
        chunk_arrays_and_specs : Iterable[tuple[NDBuffer | None, ArraySpec]]
            Ordered set of to-be-encoded chunks with their accompanying chunk spec.

        Returns
        -------
        Iterable[Buffer | None]
        """
        ...

    @abstractmethod
    async def read(
        self,
        batch_info: Iterable[tuple[ByteGetter, ArraySpec, SelectorTuple, SelectorTuple, bool]],
        out: NDBuffer,
        drop_axes: tuple[int, ...] = (),
    ) -> None:
        """Reads chunk data from the store, decodes it and writes it into an output array.
        Partial decoding may be utilized if the codecs and stores support it.

        Parameters
        ----------
        batch_info : Iterable[tuple[ByteGetter, ArraySpec, SelectorTuple, SelectorTuple]]
            Ordered set of information about the chunks.
            The first slice selection determines which parts of the chunk will be fetched.
            The second slice selection determines where in the output array the chunk data will be written.
            The ByteGetter is used to fetch the necessary bytes.
            The chunk spec contains information about the construction of an array from the bytes.

            If the Store returns ``None`` for a chunk, then the chunk was not
            written and the implementation must set the values of that chunk (or
            ``out``) to the fill value for the array.

        out : NDBuffer
        """
        ...

    @abstractmethod
    async def write(
        self,
        batch_info: Iterable[tuple[ByteSetter, ArraySpec, SelectorTuple, SelectorTuple, bool]],
        value: NDBuffer,
        drop_axes: tuple[int, ...] = (),
    ) -> None:
        """Encodes chunk data and writes it to the store.
        Merges with existing chunk data by reading first, if necessary.
        Partial encoding may be utilized if the codecs and stores support it.

        Parameters
        ----------
        batch_info : Iterable[tuple[ByteSetter, ArraySpec, SelectorTuple, SelectorTuple]]
            Ordered set of information about the chunks.
            The first slice selection determines which parts of the chunk will be encoded.
            The second slice selection determines where in the value array the chunk data is located.
            The ByteSetter is used to fetch and write the necessary bytes.
            The chunk spec contains information about the chunk.
        value : NDBuffer
        """
        ...


async def _batching_helper(
    func: Callable[[CodecInput, ArraySpec], Awaitable[CodecOutput | None]],
    batch_info: Iterable[tuple[CodecInput | None, ArraySpec]],
) -> list[CodecOutput | None]:
    return await concurrent_map(
        list(batch_info),
        _noop_for_none(func),
        config.get("async.concurrency"),
    )


def _noop_for_none(
    func: Callable[[CodecInput, ArraySpec], Awaitable[CodecOutput | None]],
) -> Callable[[CodecInput | None, ArraySpec], Awaitable[CodecOutput | None]]:
    async def wrap(chunk: CodecInput | None, chunk_spec: ArraySpec) -> CodecOutput | None:
        if chunk is None:
            return None
        return await func(chunk, chunk_spec)

    return wrap
