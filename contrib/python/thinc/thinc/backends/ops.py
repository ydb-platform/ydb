import itertools
import math
from typing import (
    Any,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

import numpy

from ..types import (
    Array1d,
    Array2d,
    Array3d,
    Array4d,
    ArrayXd,
    Batchable,
    DeviceTypes,
    DTypes,
    DTypesFloat,
    DTypesInt,
    Floats1d,
    Floats2d,
    Floats3d,
    Floats4d,
    FloatsXd,
    FloatsXdT,
    Generator,
    Ints1d,
    Ints2d,
    Ints3d,
    Ints4d,
    IntsXd,
    List2d,
    ListXd,
    Padded,
    Shape,
    SizedGenerator,
    Xp,
    _Floats,
)
from ..util import get_array_module, is_xp_array, to_numpy
from .cblas import CBlas

ArrayT = TypeVar("ArrayT", bound=ArrayXd)
FloatsT = TypeVar("FloatsT", bound=_Floats)
SQRT2PI = math.sqrt(2.0 / math.pi)
INV_SQRT2 = 1.0 / math.sqrt(2.0)
INV_SQRT_2PI = 1.0 / math.sqrt(2.0 * math.pi)


class Ops:
    name: str = "base"
    xp: Xp = numpy

    def __init__(
        self, device_type: DeviceTypes = "cpu", device_id: int = -1, **kwargs
    ) -> None:
        self.device_type = device_type
        self.device_id = device_id

    def cblas(self) -> CBlas:
        """Return C BLAS function table."""
        err = f"{type(self).__name__} does not provide C BLAS functions"
        raise NotImplementedError(err)

    def to_numpy(self, data, *, byte_order=None):  # pragma: no cover
        if isinstance(data, numpy.ndarray):
            if byte_order:
                dtype = data.dtype.newbyteorder(byte_order)
                data = numpy.asarray(data, dtype=dtype)
            return data
        else:
            raise ValueError("Cannot convert non-numpy from base Ops class")

    def minibatch(
        self,
        size: Union[int, Generator],
        sequence: Batchable,
        *,
        shuffle: bool = False,
        buffer: int = 1,
    ) -> SizedGenerator:
        """Iterate slices from a sequence, optionally shuffled. Slices
        may be either views or copies of the underlying data.

        The `size` argument may be either an integer, or a sequence of integers.
        If a sequence, a new size is drawn before every output.

        If shuffle is True, shuffled batches are produced by first generating
        an index array, shuffling it, and then using it to slice into the
        sequence.

        An internal queue of `buffer` items is accumulated before being each
        output. Buffering is useful for some devices, to allow the
        network to run asynchronously without blocking on every batch.
        """
        if not hasattr(sequence, "__len__"):
            err = f"Can't minibatch data. Expected sequence, got {type(sequence)}"
            raise ValueError(err)
        sizes = self._get_batch_sizes(
            len(sequence), itertools.repeat(size) if isinstance(size, int) else size
        )
        indices = numpy.arange(len(sequence))

        # This is a bit convoluted, but it's a time where convenience makes
        # trickery worthwhile: instead of being an actual generator, we
        # return our SizedGenerator object, which provides a __len__.
        def _iter_items():
            if shuffle:
                numpy.random.shuffle(indices)
            queue = []
            i = 0
            for size in sizes:
                size = int(size)
                queue.append(self._get_batch(sequence, indices[i : i + size]))
                if len(queue) >= buffer:
                    yield from queue
                    queue = []
                i += size
            yield from queue

        return SizedGenerator(_iter_items, len(sizes))

    def multibatch(
        self,
        size: Union[int, Generator],
        sequence: Batchable,
        *others: Batchable,
        shuffle: bool = False,
        buffer: int = 1,
    ) -> SizedGenerator:
        """Minibatch one or more sequences of data, and yield
        lists with one batch per sequence. See ops.minibatch.
        """
        # You'd think we could just do this by calling into minibatch and zip...
        # But the shuffling makes it really hard.
        sequences = (sequence,) + tuple(others)
        if not all(hasattr(seq, "__len__") for seq in sequences):
            values = ", ".join([f"{type(seq)}" for seq in sequences])
            err = f"Can't multibatch data. Expected sequences, got {values}"
            raise ValueError(err)
        sizes = self._get_batch_sizes(
            len(sequence), itertools.repeat(size) if isinstance(size, int) else size
        )
        indices = numpy.arange(len(sequence))

        def _iter_items():
            if shuffle:
                numpy.random.shuffle(indices)
            queue = []
            i = 0
            for size in sizes:
                size = int(size)
                idx_batch = indices[i : i + size]
                queue.append([])
                for sequence in sequences:
                    queue[-1].append(self._get_batch(sequence, idx_batch))
                if len(queue) >= buffer:
                    yield from queue
                    queue = []
                i += size
            yield from queue

        return SizedGenerator(_iter_items, len(sizes))

    def _get_batch(self, sequence, indices):
        if isinstance(sequence, list):
            subseq = [sequence[i] for i in indices]
        elif isinstance(sequence, tuple):
            subseq = tuple(sequence[i] for i in indices)
        else:
            subseq = sequence[indices]
        if is_xp_array(subseq):
            subseq = self.as_contig(self.xp.asarray(subseq))
        return subseq

    def _get_batch_sizes(self, length: int, sizes: Iterator[int]):
        output = []
        i = 0
        while i < length:
            output.append(next(sizes))
            i += output[-1]
        return output

    def seq2col(
        self, seq: Floats2d, nW: int, *, lengths: Optional[Ints1d] = None
    ) -> Floats2d:
        """Given an (M, N) sequence of vectors, return an (M, N*(nW*2+1))
        sequence. The new sequence is constructed by concatenating nW preceding
        and succeeding vectors onto each column in the sequence, to extract a
        window of features.
        """
        # This is a test implementation that only supports nW=1 and lengths=None
        assert nW == 1
        assert lengths == None
        B = seq.shape[0]
        I = seq.shape[1]
        cols = self.alloc3f(B, (nW * 2 + 1), I)
        # Copy left contexts. The last words aren't the left-context for anything.
        cols[nW:, :nW] = self.reshape3f(seq[:-nW], -1, nW, I)
        cols[:, nW] = seq
        cols[:-nW, nW + 1 :] = self.reshape3f(seq[nW:], -1, nW, I)
        return self.reshape2f(cols, B, I * (2 * nW + 1))

    def backprop_seq2col(
        self, dY: Floats2d, nW: int, *, lengths: Optional[Ints1d] = None
    ) -> Floats2d:
        """The reverse/backward operation of the `seq2col` function: calculate
        the gradient of the original `(M, N)` sequence, as a function of the
        gradient of the output `(M, N*(nW*2+1))` sequence.
        """
        # This is a test implementation that only supports nW=1 and lengths=None
        assert nW == 1
        assert lengths == None
        nF = nW * 2 + 1
        B = dY.shape[0]
        I = dY.shape[1] // nF
        # Having trouble getting the kernel to work...
        dX = self.alloc2f(B, I)
        dY3d = self.reshape3f(dY, B, nF, I)
        dX[:-nW] += self.reshape2f(dY3d[nW:, :nW], -1, I)
        dX += dY3d[:, nW]
        dX[nW:] += self.reshape2f(dY3d[:-nW, nW + 1 :], -1, I)
        return dX

    def gemm(
        self,
        x: Floats2d,
        y: Floats2d,
        out: Optional[Floats2d] = None,
        trans1: bool = False,
        trans2: bool = False,
    ) -> Floats2d:
        """Perform General Matrix Multiplication (GeMM) and optionally store
        the result in the specified output variable.
        """
        if trans1:
            x = x.T
        if trans2:
            y = y.T
        if out is None:
            return self.xp.dot(x, y)
        else:
            self.xp.dot(x, y, out=out)
            return out

    def tile(self, X: Floats2d, reps: int) -> Floats2d:
        return self.xp.tile(X, reps)

    def affine(self, X: Floats2d, W: Floats2d, b: Floats1d) -> Floats2d:
        """Apply a weights layer and a bias to some inputs, i.e.
        Y = X @ W.T + b
        """
        Y = self.gemm(X, W, trans2=True)
        Y += b
        return Y

    @overload
    def flatten(
        self,
        X: List[Floats2d],
        dtype: Optional[DTypes] = None,
        pad: int = 0,
        ndim_if_empty: int = 2,
    ) -> Floats2d:
        ...

    @overload
    def flatten(
        self,
        X: List[Ints1d],
        dtype: Optional[DTypes] = None,
        pad: int = 0,
        ndim_if_empty: int = 2,
    ) -> Ints1d:
        ...

    @overload
    def flatten(
        self,
        X: List2d,
        dtype: Optional[DTypes] = None,
        pad: int = 0,
        ndim_if_empty: int = 2,
    ) -> Array2d:
        ...

    # further specific typed signatures can be added as necessary

    @overload
    def flatten(
        self,
        X: ListXd,
        dtype: Optional[DTypes] = None,
        pad: int = 0,
        ndim_if_empty: int = 2,
    ) -> ArrayXd:
        ...

    @overload
    def flatten(
        self,
        X: Sequence[ArrayXd],
        dtype: Optional[DTypes] = None,
        pad: int = 0,
        ndim_if_empty: int = 2,
    ) -> ArrayXd:
        ...

    def flatten(
        self,
        X: Sequence[ArrayXd],
        dtype: Optional[DTypes] = None,
        pad: int = 0,
        ndim_if_empty: int = 2,
    ) -> ArrayXd:
        """Flatten a list of arrays into one large array."""
        if X is None or len(X) == 0:
            return self.alloc((0,) * ndim_if_empty, dtype=dtype or "f")
        xp = get_array_module(X[0])
        shape_if_empty = X[0].shape
        X = [x for x in X if x.size != 0]
        if len(X) == 0:
            return self.alloc(shape_if_empty, dtype=dtype or "f")
        if int(pad) >= 1:
            padded = []
            for x in X:
                padded.append(xp.zeros((pad,) + x.shape[1:], dtype=x.dtype))
                padded.append(x)
            padded.append(xp.zeros((pad,) + x.shape[1:], dtype=x.dtype))
            X = padded
        result = xp.concatenate(X)
        if dtype is not None:
            result = xp.asarray(result, dtype=dtype)
        return result

    @overload
    def unflatten(self, X: Floats2d, lengths: Ints1d, pad: int = 0) -> List[Floats2d]:
        ...

    @overload
    def unflatten(self, X: Ints1d, lengths: Ints1d, pad: int = 0) -> List[Ints1d]:
        ...

    @overload
    def unflatten(self, X: Array2d, lengths: Ints1d, pad: int = 0) -> List2d:
        ...

    # further specific typed signatures can be added as necessary

    @overload
    def unflatten(self, X: ArrayXd, lengths: Ints1d, pad: int = 0) -> ListXd:
        ...

    def unflatten(self, X: ArrayXd, lengths: Ints1d, pad: int = 0) -> ListXd:
        """The reverse/backward operation of the `flatten` function: unflatten
        a large array into a list of arrays according to the given lengths.
        """
        # cupy.split requires lengths to be in CPU memory.
        lengths = to_numpy(lengths)

        if pad > 0:
            lengths = numpy.where(lengths > 0, lengths + pad, 0)  # type: ignore
        unflat = self.xp.split(X, numpy.cumsum(lengths))[:-1]  # type: ignore
        if pad > 0:
            unflat = [a[pad:] for a in unflat]

        assert len(unflat) == len(lengths)

        return unflat

    @overload
    def pad(self, seqs: List[Ints2d], round_to=1) -> Ints3d:
        ...

    @overload  # noqa: F811
    def pad(self, seqs: List[Floats2d], round_to=1) -> Floats3d:
        ...

    def pad(  # noqa: F811
        self, seqs: Union[List[Ints2d], List[Floats2d]], round_to=1
    ) -> Array3d:
        """Perform padding on a list of arrays so that they each have the same
        length, by taking the maximum dimension across each axis. This only
        works on non-empty sequences with the same `ndim` and `dtype`.
        """
        if round_to < 1:
            raise ValueError(
                f"Rounding for padding must at least be 1, was: {round_to}"
            )

        # TODO: This should be generalized to handle different ranks
        if not seqs:
            raise ValueError("Cannot pad empty sequence")
        if len(set(seq.ndim for seq in seqs)) != 1:
            raise ValueError("Cannot pad sequences with different ndims")
        if len(set(seq.dtype for seq in seqs)) != 1:
            raise ValueError("Cannot pad sequences with different dtypes")
        if len(set(seq.shape[1:] for seq in seqs)) != 1:
            raise ValueError("Cannot pad sequences that differ on other dimensions")
        # Find the maximum dimension along each axis. That's what we'll pad to.
        max_seq_len = max(len(seq) for seq in seqs)
        # Round the length to nearest bucket -- helps on GPU, to make similar
        # array sizes.
        max_seq_len += -max_seq_len % round_to
        final_shape = (len(seqs), max_seq_len) + seqs[0].shape[1:]
        output: Array3d = cast(Array3d, self.alloc(final_shape, dtype=seqs[0].dtype))
        for i, arr in enumerate(seqs):
            # It's difficult to convince this that the dtypes will match.
            output[i, : arr.shape[0]] = arr  # type: ignore[assignment, call-overload]
        return output

    def unpad(self, padded: Array3d, lengths: List[int]) -> List2d:
        """The reverse/backward operation of the `pad` function: transform an
        array back into a list of arrays, each with their original length.
        """
        output = []
        for i, length in enumerate(lengths):
            output.append(padded[i, :length])
        return cast(List2d, output)

    def list2padded(self, seqs: List2d) -> Padded:
        """Pack a sequence of 2d arrays into a Padded datatype."""
        if not seqs:
            return Padded(
                self.alloc3f(0, 0, 0), self.alloc1i(0), self.alloc1i(0), self.alloc1i(0)
            )
        elif len(seqs) == 1:
            data = self.reshape3(seqs[0], seqs[0].shape[0], 1, seqs[0].shape[1])
            size_at_t = self.asarray1i([1] * data.shape[0])
            lengths = self.asarray1i([data.shape[0]])
            indices = self.asarray1i([0])
            return Padded(data, size_at_t, lengths, indices)
        lengths_indices = [(len(seq), i) for i, seq in enumerate(seqs)]
        lengths_indices.sort(reverse=True)
        indices_ = [i for length, i in lengths_indices]
        lengths_ = [length for length, i in lengths_indices]
        nS = max([seq.shape[0] for seq in seqs])
        nB = len(seqs)
        nO = seqs[0].shape[1]
        # Reorder the sequences, by length. This looks the same in either
        # direction: you're swapping elements between their original and sorted
        # position.
        seqs = cast(List2d, [seqs[i] for i in indices_])
        arr: Array3d = self.pad(seqs)
        assert arr.shape == (nB, nS, nO), (nB, nS, nO)
        arr = self.as_contig(arr.transpose((1, 0, 2)))
        assert arr.shape == (nS, nB, nO)
        # Build a lookup table so we can find how big the batch is at point t.
        batch_size_at_t_ = [0 for _ in range(nS)]
        current_size = len(lengths_)
        for t in range(nS):
            while current_size and t >= lengths_[current_size - 1]:
                current_size -= 1
            batch_size_at_t_[t] = current_size
        assert sum(lengths_) == sum(batch_size_at_t_)
        return Padded(
            arr,
            self.asarray1i(batch_size_at_t_),
            self.asarray1i(lengths_),
            self.asarray1i(indices_),
        )

    def padded2list(self, padded: Padded) -> List2d:
        """Unpack a Padded datatype to a list of 2-dimensional arrays."""
        data = padded.data
        indices = to_numpy(padded.indices)
        lengths = to_numpy(padded.lengths)
        unpadded: List[Optional[Array2d]] = [None] * len(lengths)
        # Transpose from (length, batch, data) to (batch, length, data)
        data = self.as_contig(data.transpose((1, 0, 2)))
        for i in range(data.shape[0]):
            unpadded[indices[i]] = data[i, : int(lengths[i])]
        return cast(List2d, unpadded)

    def get_dropout_mask(self, shape: Shape, drop: Optional[float]) -> FloatsXd:
        """Create a random mask for applying dropout, with a certain percent of
        the mask (defined by `drop`) will contain zeros. The neurons at those
        positions will be deactivated during training, resulting in a more
        robust network and less overfitting.
        """
        if drop is None or drop <= 0:
            return self.xp.ones(shape, dtype="f")
        elif drop >= 1.0:
            return self.alloc_f(shape)
        coinflips = self.xp.random.uniform(0.0, 1.0, shape)
        mask = (coinflips >= drop) / (1.0 - drop)
        return cast(FloatsXd, self.asarray(mask, dtype="float32"))

    def alloc1f(
        self,
        d0: int,
        *,
        dtype: Optional[DTypesFloat] = "float32",
        zeros: bool = True,
    ) -> Floats1d:
        return cast(Floats1d, self.alloc((d0,), dtype=dtype, zeros=zeros))

    def alloc2f(
        self,
        d0: int,
        d1: int,
        *,
        dtype: Optional[DTypesFloat] = "float32",
        zeros: bool = True,
    ) -> Floats2d:
        return cast(Floats2d, self.alloc((d0, d1), dtype=dtype, zeros=zeros))

    def alloc3f(
        self,
        d0: int,
        d1: int,
        d2: int,
        *,
        dtype: Optional[DTypesFloat] = "float32",
        zeros: bool = True,
    ) -> Floats3d:
        return cast(Floats3d, self.alloc((d0, d1, d2), dtype=dtype, zeros=zeros))

    def alloc4f(
        self,
        d0: int,
        d1: int,
        d2: int,
        d3: int,
        *,
        dtype: Optional[DTypesFloat] = "float32",
        zeros: bool = True,
    ) -> Floats4d:
        return cast(Floats4d, self.alloc((d0, d1, d2, d3), dtype=dtype, zeros=zeros))

    def alloc_f(
        self,
        shape: Shape,
        *,
        dtype: Optional[DTypesFloat] = "float32",
        zeros: bool = True,
    ) -> FloatsXd:
        return cast(FloatsXd, self.alloc(shape, dtype=dtype, zeros=zeros))

    def alloc1i(
        self,
        d0: int,
        *,
        dtype: Optional[DTypesInt] = "int32",
        zeros: bool = True,
    ) -> Ints1d:
        return cast(Ints1d, self.alloc((d0,), dtype=dtype, zeros=zeros))

    def alloc2i(
        self,
        d0: int,
        d1: int,
        *,
        dtype: Optional[DTypesInt] = "int32",
        zeros: bool = True,
    ) -> Ints2d:
        return cast(Ints2d, self.alloc((d0, d1), dtype=dtype, zeros=zeros))

    def alloc3i(
        self,
        d0: int,
        d1: int,
        d2: int,
        *,
        dtype: Optional[DTypesInt] = "int32",
        zeros: bool = True,
    ) -> Ints3d:
        return cast(Ints3d, self.alloc((d0, d1, d2), dtype=dtype, zeros=zeros))

    def alloc4i(
        self,
        d0: int,
        d1: int,
        d2: int,
        d3: int,
        *,
        dtype: Optional[DTypesInt] = "int32",
        zeros: bool = True,
    ) -> Ints4d:
        return cast(Ints4d, self.alloc((d0, d1, d2, d3), dtype=dtype, zeros=zeros))

    def alloc_i(
        self,
        shape: Shape,
        *,
        dtype: Optional[DTypesInt] = "int32",
        zeros: bool = True,
    ) -> IntsXd:
        return cast(IntsXd, self.alloc(shape, dtype=dtype, zeros=zeros))

    def alloc(
        self,
        shape: Shape,
        *,
        dtype: Optional[DTypes] = "float32",
        zeros: bool = True,
    ) -> Any:
        """Allocate an array of a certain shape."""
        if isinstance(shape, int):
            shape = (shape,)

        if zeros:
            return self.xp.zeros(shape, dtype=dtype)
        else:
            return self.xp.empty(shape, dtype=dtype)

    def reshape1(self, array: ArrayXd, d0: int) -> Array1d:
        return cast(Array1d, self.reshape(array, (d0,)))

    def reshape2(self, array: ArrayXd, d0: int, d1: int) -> Array2d:
        return cast(Array2d, self.reshape(array, (d0, d1)))

    def reshape3(self, array: ArrayXd, d0: int, d1: int, d2: int) -> Array3d:
        return cast(Array3d, self.reshape(array, (d0, d1, d2)))

    def reshape4(self, array: ArrayXd, d0: int, d1: int, d2: int, d3: int) -> Array4d:
        return cast(Array4d, self.reshape(array, (d0, d1, d2, d3)))

    def reshape1f(self, array: FloatsXd, d0: int) -> Floats1d:
        return cast(Floats1d, self.reshape(array, (d0,)))

    def reshape2f(self, array: FloatsXd, d0: int, d1: int) -> Floats2d:
        return cast(Floats2d, self.reshape(array, (d0, d1)))

    def reshape3f(self, array: FloatsXd, d0: int, d1: int, d2: int) -> Floats3d:
        return cast(Floats3d, self.reshape(array, (d0, d1, d2)))

    def reshape4f(
        self, array: FloatsXd, d0: int, d1: int, d2: int, d3: int
    ) -> Floats4d:
        return cast(Floats4d, self.reshape(array, (d0, d1, d2, d3)))

    def reshape_f(self, array: FloatsXd, shape: Shape) -> FloatsXd:
        return self.reshape(array, shape)

    def reshape1i(self, array: IntsXd, d0: int) -> Ints1d:
        return cast(Ints1d, self.reshape(array, (d0,)))

    def reshape2i(self, array: IntsXd, d0: int, d1: int) -> Ints2d:
        return cast(Ints2d, self.reshape(array, (d0, d1)))

    def reshape3i(self, array: IntsXd, d0: int, d1: int, d2: int) -> Ints3d:
        return cast(Ints3d, self.reshape(array, (d0, d1, d2)))

    def reshape4i(self, array: IntsXd, d0: int, d1: int, d2: int, d3: int) -> Ints4d:
        return cast(Ints4d, self.reshape(array, (d0, d1, d2, d3)))

    def reshape_i(self, array: IntsXd, shape: Shape) -> IntsXd:
        return self.reshape(array, shape)

    def reshape(self, array: ArrayT, shape: Shape) -> ArrayT:
        """Reshape an array."""
        if isinstance(shape, int):
            shape = (shape,)
        return cast(ArrayT, array.reshape(shape))

    def asarray4f(
        self,
        data: Union[Floats4d, Sequence[Sequence[Sequence[Sequence[float]]]]],
        *,
        dtype: Optional[DTypes] = "float32",
    ) -> Floats4d:
        return cast(Floats4d, self.asarray(data, dtype=dtype))

    def asarray3f(
        self,
        data: Union[Floats3d, Sequence[Sequence[Sequence[float]]]],
        *,
        dtype: Optional[DTypes] = "float32",
    ) -> Floats3d:
        return cast(Floats3d, self.asarray(data, dtype=dtype))

    def asarray2f(
        self,
        data: Union[Floats2d, Sequence[Sequence[float]]],
        *,
        dtype: Optional[DTypes] = "float32",
    ) -> Floats2d:
        return cast(Floats2d, self.asarray(data, dtype=dtype))

    def asarray1f(
        self,
        data: Union[Floats1d, Sequence[float]],
        *,
        dtype: Optional[DTypes] = "float32",
    ) -> Floats1d:
        return cast(Floats1d, self.asarray(data, dtype=dtype))

    def asarray_f(
        self,
        data: Union[FloatsXd, Sequence[Any]],
        *,
        dtype: Optional[DTypes] = "float32",
    ) -> FloatsXd:
        return cast(FloatsXd, self.asarray(data, dtype=dtype))

    def asarray1i(
        self, data: Union[Ints1d, Sequence[int]], *, dtype: Optional[DTypes] = "int32"
    ) -> Ints1d:
        return cast(Ints1d, self.asarray(data, dtype=dtype))

    def asarray2i(
        self,
        data: Union[Ints2d, Sequence[Sequence[int]]],
        *,
        dtype: Optional[DTypes] = "int32",
    ) -> Ints2d:
        return cast(Ints2d, self.asarray(data, dtype=dtype))

    def asarray3i(
        self,
        data: Union[Ints3d, Sequence[Sequence[Sequence[int]]]],
        *,
        dtype: Optional[DTypes] = "int32",
    ) -> Ints3d:
        return cast(Ints3d, self.asarray(data, dtype=dtype))

    def asarray4i(
        self,
        data: Union[Ints4d, Sequence[Sequence[Sequence[Sequence[int]]]]],
        *,
        dtype: Optional[DTypes] = "int32",
    ) -> Ints4d:
        return cast(Ints4d, self.asarray(data, dtype=dtype))

    def asarray_i(
        self, data: Union[IntsXd, Sequence[Any]], *, dtype: Optional[DTypes] = "int32"
    ) -> IntsXd:
        return cast(IntsXd, self.asarray(data, dtype=dtype))

    def asarray(
        self,
        data: Union[ArrayXd, Sequence[ArrayXd], Sequence[Any]],
        *,
        dtype: Optional[DTypes] = None,
    ) -> ArrayXd:
        """Ensure a given array is of the correct type."""
        if isinstance(data, self.xp.ndarray):
            if dtype is None:
                return data
            elif data.dtype == dtype:
                return data
            else:
                return self.xp.asarray(data, dtype=dtype)
        elif hasattr(data, "numpy"):
            # Handles PyTorch Tensor
            return data.numpy()  # type: ignore[union-attr]
        elif dtype is not None:
            return self.xp.array(data, dtype=dtype)
        else:
            return self.xp.array(data)

    def as_contig(self, data: ArrayT, dtype: Optional[DTypes] = None) -> ArrayT:
        """Allow the backend to make a contiguous copy of an array.
        Implementations of `Ops` do not have to make a copy or make it
        contiguous if that would not improve efficiency for the execution engine.
        """
        if data.flags["C_CONTIGUOUS"] and dtype in (None, data.dtype):
            return data
        kwargs = {"dtype": dtype} if dtype is not None else {}
        return self.xp.ascontiguousarray(data, **kwargs)

    def sigmoid(self, X: FloatsXdT, *, inplace: bool = False) -> FloatsXdT:
        if inplace:
            # To prevent overflows and help with regularization/numerical stability
            X = self.xp.clip(X, -20.0, 20.0, out=X)
            self.xp.exp(-X, out=X)
            X += 1.0
            X **= -1.0
            return X
        else:
            X = self.xp.clip(X, -20.0, 20.0)
            return 1.0 / (1.0 + self.xp.exp(-X))

    def backprop_sigmoid(
        self, dY: FloatsXdT, Y: FloatsXdT, *, inplace: bool = False
    ) -> FloatsXdT:
        if inplace:
            self.dsigmoid(Y, inplace=True)
            Y *= dY
            return Y
        else:
            return dY * self.dsigmoid(Y, inplace=inplace)

    def dsigmoid(self, Y: FloatsXdT, *, inplace: bool = False) -> FloatsXdT:
        if inplace:
            Y *= 1 - Y
            return Y
        else:
            return Y * (1.0 - Y)

    def dtanh(self, Y: FloatsT, *, inplace: bool = False) -> FloatsT:
        if inplace:
            Y **= 2
            Y *= -1.0
            Y += 1.0
            return Y
        else:
            return 1 - Y**2

    def softmax(
        self,
        x: FloatsT,
        *,
        inplace: bool = False,
        axis: int = -1,
        temperature: float = 1.0,
    ) -> FloatsT:
        if temperature != 1.0:
            x = x / temperature
        maxes = self.xp.max(x, axis=axis, keepdims=True)
        shifted = x - maxes
        new_x = self.xp.exp(shifted)
        new_x /= new_x.sum(axis=axis, keepdims=True)
        return new_x

    def softmax_sequences(
        self, Xs: Floats2d, lengths: Ints1d, *, inplace: bool = False, axis: int = -1
    ) -> Floats2d:
        if Xs.ndim >= 3:
            err = f"Softmax currently only supports 2d. Got: {Xs.ndim}"
            raise NotImplementedError(err)
        # This loses almost no fidelity, and helps the numerical stability.
        Xs = self.xp.clip(Xs, -20.0, 20.0)
        new_x = self.xp.exp(Xs)
        summed = self.backprop_reduce_sum(self.reduce_sum(new_x, lengths), lengths)
        new_x /= summed
        return new_x

    def backprop_softmax(
        self, Y: FloatsT, dY: FloatsT, *, axis: int = -1, temperature: float = 1.0
    ) -> FloatsT:
        if temperature != 1.0:
            dY = dY / temperature

        dX = Y * dY
        dX -= Y * dX.sum(axis=axis, keepdims=True)
        return dX

    def backprop_softmax_sequences(
        self, dY: Floats2d, Y: Floats2d, lengths: Ints1d
    ) -> Floats2d:
        dX = Y * dY
        sum_dX = self.backprop_reduce_sum(self.reduce_sum(dX, lengths), lengths)
        dX -= Y * sum_dX
        return dX

    def lstm_forward_training(
        self,
        params: Floats1d,
        H0: Floats3d,
        C0: Floats3d,
        X: Floats2d,
        size_at_t: Ints1d,
    ) -> Tuple[Floats2d, Tuple]:
        assert H0.shape == C0.shape
        assert H0.shape[1] == C0.shape[1]
        Y, fwd_state = lstm_forward_training(params, H0, C0, X, size_at_t)
        return Y, fwd_state

    def lstm_forward_inference(
        self,
        params: Floats1d,
        H0: Floats3d,
        C0: Floats3d,
        X: Floats2d,
        size_at_t: Ints1d,
    ) -> Floats2d:
        Y, _ = lstm_forward_training(params, H0, C0, X, size_at_t)
        return Y

    def backprop_lstm(
        self, dY: Floats2d, lengths: Ints1d, params: Floats1d, fwd_state: Tuple
    ) -> Tuple[Floats2d, Floats1d]:
        dX, d_params = backprop_lstm(dY, lengths, params, fwd_state)
        return dX, d_params

    def maxout(self, X: Floats3d) -> Tuple[Floats2d, Ints2d]:
        which = X.argmax(axis=-1)
        return X.max(axis=-1), which

    def backprop_maxout(self, dY: Floats2d, which: Ints2d, P: int) -> Floats3d:
        dX = self.alloc3f(dY.shape[0], dY.shape[1], P, dtype=dY.dtype)
        for b in range(dY.shape[0]):
            for o in range(dY.shape[1]):
                dX[b, o, which[b, o]] = dY[b, o]
        return dX

    def relu(self, X: Floats2d, inplace: bool = False) -> Floats2d:
        if not inplace:
            return X * (X > 0)
        else:
            X *= X > 0
            return X

    def backprop_relu(
        self, dY: Floats2d, Y: Floats2d, inplace: bool = False
    ) -> Floats2d:
        if not inplace:
            return dY * (Y > 0)
        dY *= Y > 0
        return dY

    def clipped_linear(
        self,
        X: FloatsXdT,
        slope: float = 1.0,
        offset: float = 0.0,
        min_val: float = 0.0,
        max_val: float = 1.0,
        inplace: bool = False,
    ) -> FloatsXdT:
        if inplace:
            X *= slope
            X += offset
            return self.xp.clip(X, min_val, max_val, out=X)
        out = X * slope + offset
        return self.xp.clip(out, min_val, max_val)

    def backprop_clipped_linear(
        self,
        dY: FloatsXdT,
        X: FloatsXdT,
        slope: float = 1.0,
        offset: float = 0.0,
        min_val: float = 0.0,
        max_val: float = 1.0,
        inplace: bool = False,
    ) -> FloatsXdT:
        low = (min_val - offset) / slope
        high = (max_val - offset) / slope
        slope = self.xp.float64(slope).astype(X.dtype)
        zero = self.xp.float64(0.0).astype(X.dtype)
        dX = self.xp.where((low < X) & (X < high), slope, zero)
        if inplace:
            dY *= dX
            return dY
        return dY * dX

    def relu_k(self, X: FloatsXdT, n: float = 6.0, inplace: bool = False) -> FloatsXdT:
        return self.clipped_linear(X, max_val=n, inplace=inplace)

    def backprop_relu_k(
        self, dY: FloatsXdT, X: FloatsXdT, n: float = 6.0, inplace: bool = False
    ) -> FloatsXdT:
        return self.backprop_clipped_linear(dY, X, max_val=n, inplace=inplace)

    def hard_sigmoid(self, X: FloatsXdT, inplace: bool = False) -> FloatsXdT:
        return self.clipped_linear(X, slope=0.2, offset=0.5, inplace=inplace)

    def backprop_hard_sigmoid(
        self, dY: FloatsXdT, X: FloatsXdT, inplace: bool = False
    ) -> FloatsXdT:
        return self.backprop_clipped_linear(dY, X, slope=0.2, offset=0.5)

    def hard_tanh(self, X: FloatsXdT, inplace: bool = False) -> FloatsXdT:
        return self.clipped_linear(X, min_val=-1.0, max_val=1.0, inplace=inplace)

    def backprop_hard_tanh(
        self, dY: FloatsXdT, X: FloatsXdT, inplace: bool = False
    ) -> FloatsXdT:
        return self.backprop_clipped_linear(dY, X, min_val=-1.0, max_val=1.0)

    def swish(self, X: FloatsXdT, inplace: bool = False) -> FloatsXdT:
        if inplace:
            X *= self.sigmoid(X)
            return X
        out = X * self.sigmoid(X)
        return out

    def backprop_swish(
        self, dY: FloatsXdT, X: FloatsXdT, Y: FloatsXdT, inplace: bool = False
    ) -> FloatsXdT:
        Y = Y + self.sigmoid(X) * (1 - Y)
        if inplace:
            dY *= Y
            return dY
        out = dY * Y
        return out

    # Following https://www.scitepress.org/Papers/2019/74696/74696.pdf
    def hard_swish(self, X: FloatsXdT, inplace: bool = False) -> FloatsXdT:
        if inplace:
            X *= self.hard_sigmoid(X)
            return X
        out = X * self.hard_sigmoid(X)
        return out

    def backprop_hard_swish(
        self, dY: FloatsXdT, X: FloatsXdT, inplace: bool = False
    ) -> FloatsXdT:
        dX = X * 0.4 + 0.5
        dX[X > 2.5] = 1.0
        dX[X < -2.5] = 0
        if inplace:
            dY *= dX
            return dY
        return dY * dX

    # From https://arxiv.org/pdf/1905.02244v5.pdf
    def hard_swish_mobilenet(self, X: FloatsXdT, inplace: bool = False) -> FloatsXdT:
        if inplace:
            X *= self.relu_k(X + 3) / 6
            return X
        return X * (self.relu_k(X + 3) / 6)

    def backprop_hard_swish_mobilenet(
        self, dY: FloatsXdT, X: FloatsXdT, inplace: bool = False
    ) -> FloatsXdT:
        dX = (1 / 6) * (X * 2.0 + 3.0)
        dX[X > 3.0] = 1.0
        dX[X < -3.0] = 0
        if inplace:
            dY *= dX
            return dY
        return dX * dY

    def dish(self, X: FloatsXdT, inplace: bool = False) -> FloatsXdT:
        tmp = self.xp.square(X)
        tmp += 1.0
        self.xp.sqrt(tmp, out=tmp)
        tmp = X / tmp
        tmp += 1
        tmp *= 0.5
        if inplace:
            X *= tmp
            return X
        else:
            return X * tmp

    def backprop_dish(
        self, dY: FloatsXdT, X: FloatsXdT, inplace: bool = False
    ) -> FloatsXdT:
        x_sq = self.xp.square(X)
        x_sq_plus_one = x_sq + 1.0
        deriv = X / self.xp.sqrt(x_sq_plus_one)
        second = 0.5 * X * x_sq
        second /= x_sq_plus_one**1.5
        deriv -= second
        deriv += 0.5
        if inplace:
            dY *= deriv
            return dY
        else:
            return dY * deriv

    # Code snippet taken from:
    # https://www.johndcook.com/blog/2009/01/19/stand-alone-error-function-erf/
    def erf(self, X: FloatsXdT) -> FloatsXdT:
        # save the sign of x
        sign = self.xp.sign(X)
        X = self.xp.abs(X)

        a1 = 0.254829592
        a2 = -0.284496736
        a3 = 1.421413741
        a4 = -1.453152027
        a5 = 1.061405429
        p = 0.3275911

        t = 1.0 / (1.0 + p * X)
        y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * self.xp.exp(
            -X * X
        )
        out = sign * y
        out = out.astype(X.dtype)
        return out

    def sechsq(self, X: FloatsXdT) -> FloatsXdT:
        # Avoid overflow in cosh. Clipping at |20| has an error of 1.7e-17.
        X = self.xp.clip(X, -20.0, 20.0)
        return (1 / self.xp.cosh(X)) ** 2

    def gelu_approx(self, X: FloatsXdT, inplace: bool = False) -> FloatsXdT:
        tmp = 1.0 + self.xp.tanh(SQRT2PI * (X + 0.044715 * self.xp.power(X, 3)))
        tmp *= 0.5
        tmp = tmp.astype(X.dtype)
        if inplace:
            X *= tmp
            return X
        else:
            Y = self.xp.array(X)
            Y *= tmp
            return Y

    def backprop_gelu_approx(
        self, dY: FloatsXdT, X: FloatsXdT, inplace: bool = False
    ) -> FloatsXdT:
        dX = cast(FloatsXdT, self.alloc_f(X.shape))
        Xp3 = self.xp.power(X, 3)
        tmp = 0.5 * self.xp.tanh(0.0356774 * Xp3 + 0.797885 * X)
        tmp += (0.0535161 * Xp3 + 0.398942 * X) * self.sechsq(
            0.0356774 * Xp3 + 0.797885 * X
        )
        tmp += 0.5
        dX += tmp
        if inplace:
            dY *= dX
            return dY
        return dY * dX

    def gelu(self, X: FloatsXdT, inplace: bool = False) -> FloatsXdT:
        # GELU(x) = x · Φ(x)
        cdf = gaussian_cdf(self, X)
        if inplace:
            X *= cdf
            return X
        return X * cdf

    def backprop_gelu(
        self, dY: FloatsXdT, X: FloatsXdT, inplace: bool = False
    ) -> FloatsXdT:
        # GELU'(x) = Φ(x) + x · PDF(x)
        dX = gaussian_cdf(self, X) + X * gaussian_pdf(self, X)
        if inplace:
            dY *= dX
            return dY
        return dY * dX

    def mish(
        self, X: FloatsXdT, threshold: float = 20.0, inplace: bool = False
    ) -> FloatsXdT:
        tmp = X * self.xp.tanh(self.xp.log(1.0 + self.xp.exp(X)))
        Y = self.xp.where(X >= threshold, X, tmp)
        if inplace:
            X[:] = Y
            return X
        else:
            return Y

    def backprop_mish(
        self,
        dY: FloatsXdT,
        X: Floats2d,
        threshold: float = 20.0,
        inplace: bool = False,
    ) -> FloatsXdT:
        if dY.shape != X.shape:
            msg = f"arrays have incompatible shapes: {dY.shape} and {X.shape}"
            raise ValueError(msg)

        xp = get_array_module(X)
        indices = X < threshold
        Xsub = X[indices]
        dYsub = dY[indices]
        omega = 4.0 * (Xsub + 1.0)
        omega += 4.0 * xp.exp(2.0 * Xsub)
        omega += xp.exp(3.0 * Xsub)
        omega += xp.exp(Xsub) * ((4.0 * Xsub) + 6.0)
        delta = xp.exp(Xsub) + 1.0
        delta *= delta
        delta += 1.0
        dXsub = dYsub * ((xp.exp(Xsub) * omega) / (delta**2))
        # Gradient when above threshold will ignore softplus.
        if inplace:
            out = dY
        else:
            out = xp.copy(dY)
        out[indices] = dXsub
        return out

    def update_averages(
        self, ema: FloatsT, weights: FloatsT, t: int, max_decay: float = 0.9999
    ) -> None:
        # Internals for optimizer
        decay = (1.0 + t) / (10.0 + t)
        if decay > max_decay:
            decay = max_decay
        ema -= (1 - decay) * (ema - weights)

    def adam(
        self,
        weights: Floats1d,
        gradient: Floats1d,
        mom1: Floats1d,
        mom2: Floats1d,
        beta1: float,
        beta2: float,
        eps: float,
        learn_rate: float,
        mod_rate: float = 1.0,
    ) -> Tuple[Floats1d, Floats1d, Floats1d, Floats1d]:
        _check_compatible_shape(weights, gradient)
        _check_compatible_shape(weights, mom1)
        _check_compatible_shape(weights, mom2)

        # Internals for optimizer
        mom1 *= beta1
        mom2 *= beta2
        mom1 += gradient * (1.0 - beta1)
        mom2 += gradient * gradient * (1.0 - beta2)
        # Here we assume learn rate is calculated by the caller.
        # cdef weight_t a_t = learn_rate * sqrt(1-beta2**hp.t) / (1-beta1**hp.t);
        weights -= learn_rate * (mom1 / (mod_rate * self.xp.sqrt(mom2) + eps))
        return weights, gradient, mom1, mom2

    def clip_gradient(self, gradient: FloatsT, threshold: float) -> FloatsT:
        # Internals for optimizer
        xp = get_array_module(gradient)
        grad_norm = xp.linalg.norm(gradient)
        if grad_norm >= threshold:
            gradient *= threshold / grad_norm
        return gradient

    def logloss(self, y_true: FloatsT, y_pred: FloatsT) -> float:
        # Currently not used
        log_yp = self.xp.log(y_pred + 1e-8)
        loss = (y_true * log_yp) + (1 - y_true) * self.xp.log((1 - y_pred) + 1e-8)
        return -loss

    def reduce_sum(self, X: Floats2d, lengths: Ints1d) -> Floats2d:
        Y = self.alloc2f(lengths.shape[0], X.shape[1], zeros=False)
        start = 0
        for i, length in enumerate(lengths):
            if length < 0:
                raise ValueError(f"all sequence lengths must be >= 0, got {length}")
            elif start + length > X.shape[0]:
                raise IndexError("lengths must sum up to the number of rows")
            elif length:
                Y[i] = X[start : start + length].sum(axis=0)
                start += length
            else:
                Y[i] = 0.0
        return Y

    def reduce_first(self, X: Floats2d, lengths: Ints1d) -> Tuple[Floats2d, Ints1d]:
        if lengths.size == 0:
            return self.alloc2f(0, X.shape[1]), lengths
        if not self.xp.all(lengths > 0):
            raise ValueError(f"all sequence lengths must be > 0")
        starts_ends = self.alloc1i(lengths.shape[0] + 1, zeros=False)
        starts_ends[0] = 0
        starts_ends[1:] = lengths.cumsum()
        if starts_ends[-1] != X.shape[0]:
            raise IndexError("lengths must sum up to the number of rows")

        return X[starts_ends[:-1]], starts_ends

    def reduce_last(self, X: Floats2d, lengths: Ints1d) -> Tuple[Floats2d, Ints1d]:
        if lengths.size == 0:
            return self.alloc2f(0, X.shape[1]), lengths
        if not self.xp.all(lengths > 0):
            raise ValueError(f"all sequence lengths must be > 0")
        lasts = lengths.cumsum() - 1
        if lasts[-1] + 1 != X.shape[0]:
            raise IndexError("lengths must sum up to the number of rows")
        return X[lasts], lasts

    def reduce_mean(self, X: Floats2d, lengths: Ints1d) -> Floats2d:
        Y = self.alloc2f(lengths.shape[0], X.shape[1], zeros=False)
        start = 0
        for i, length in enumerate(lengths):
            if length < 0:
                raise ValueError(f"all sequence lengths must be >= 0, got {length}")
            elif start + length > X.shape[0]:
                raise IndexError("lengths must sum up to the number of rows")
            elif length:
                Y[i] = X[start : start + length].mean(axis=0)
            else:
                Y[i] = 0.0
            start += length
        return Y

    def reduce_max(self, X: Floats2d, lengths: Ints1d) -> Tuple[Floats2d, Ints2d]:
        Y = self.alloc2f(lengths.shape[0], X.shape[1], dtype=X.dtype, zeros=False)
        which = self.alloc2i(lengths.shape[0], X.shape[1], zeros=False)
        start = 0
        for i, length in enumerate(lengths):
            if length <= 0:
                raise ValueError(f"all sequence lengths must be > 0, got {length}")
            elif start + length > X.shape[0]:
                raise IndexError("lengths must sum up to the number of rows")
            elif length:
                which[i] = X[start : start + length].argmax(axis=0)
                Y[i] = X[start : start + length].max(axis=0)
            start += length
        return Y, which

    def backprop_reduce_first(
        self, d_firsts: Floats2d, starts_ends: Ints1d
    ) -> Floats2d:
        if starts_ends.size == 0:
            return self.alloc2f(0, d_firsts.shape[1], dtype=d_firsts.dtype, zeros=True)
        elif starts_ends.size == 1:
            raise ValueError(f"starts_ends must not have size 1")
        dX = self.alloc2f(
            int(starts_ends[-1]), d_firsts.shape[1], dtype=d_firsts.dtype, zeros=True
        )
        dX[starts_ends[:-1]] = d_firsts
        return dX

    def backprop_reduce_last(self, d_lasts: Floats2d, lasts: Ints1d) -> Floats2d:
        if lasts.size == 0:
            return self.alloc2f(0, d_lasts.shape[1], dtype=d_lasts.dtype, zeros=True)
        dX = self.alloc2f(
            int(lasts[-1]) + 1, d_lasts.shape[1], dtype=d_lasts.dtype, zeros=True
        )
        dX[lasts] = d_lasts
        return dX

    def backprop_reduce_sum(self, d_sums: Floats2d, lengths: Ints1d) -> Floats2d:
        dX = self.alloc2f(
            lengths.sum(), d_sums.shape[1], dtype=d_sums.dtype, zeros=False
        )
        start = 0
        for i, length in enumerate(lengths):
            if length < 0:
                raise ValueError(f"all sequence lengths must be >= 0, got {length}")
            dX[start : start + length] = d_sums[i]
            start += length
        return dX

    def backprop_reduce_mean(self, d_means: Floats2d, lengths: Ints1d) -> Floats2d:
        dX = self.alloc2f(
            lengths.sum(), d_means.shape[1], dtype=d_means.dtype, zeros=False
        )
        start = 0
        for i, length in enumerate(lengths):
            if length < 0:
                raise ValueError(f"all sequence lengths must be >= 0, got {length}")
            dX[start : start + length] = d_means[i] / length
            start += length
        return dX

    def backprop_reduce_max(
        self, d_maxes: Floats2d, which: Ints2d, lengths: Ints1d
    ) -> Floats2d:
        dX = self.alloc2f(lengths.sum(), d_maxes.shape[1], dtype=d_maxes.dtype)
        start = 0
        for i, length in enumerate(lengths):
            if length <= 0:
                raise ValueError(f"all sequence lengths must be > 0, got {length}")

            self.xp.put_along_axis(
                dX[start : start + length], which[i].reshape((1, -1)), d_maxes[i], 0
            )
            start += length
        return dX

    def hash(self, ids: Ints1d, seed: int) -> Ints2d:
        """Hash a sequence of 64-bit keys into a table with 4 32-bit keys, using
        murmurhash3.
        """
        from .numpy_ops import NumpyOps

        numpy_ops = NumpyOps()
        return self.asarray2i(
            numpy_ops.hash(numpy_ops.asarray(ids, dtype="uint64"), seed)
        )

    def ngrams(self, n: int, keys: Ints1d) -> Ints1d:
        from .numpy_ops import NumpyOps

        numpy_ops = NumpyOps()
        return self.asarray1i(
            numpy_ops.ngrams(n, numpy_ops.asarray(keys, dtype="uint64"))
        )

    def position_encode(
        self, N: int, D: int, period: int = 10000, out: Optional[Floats2d] = None
    ) -> Floats2d:
        # Currently internals only
        from .numpy_ops import NumpyOps

        numpy_ops = NumpyOps()
        return self.asarray2f(numpy_ops.position_encode(N, D, period, out))

    def gather_add(self, table: Floats2d, indices: Ints2d) -> Floats2d:
        return table[indices].sum(axis=1)  # type: ignore[return-value]

    def scatter_add(
        self, table: FloatsXd, indices: IntsXd, values: FloatsXd
    ) -> FloatsXd:
        return self.xp.add.at(table, indices, values)

    def insert_into(self, shape, Xs):
        """Maybe don't need this? Just a quicky to get Jax working."""
        output = self.alloc(shape, dtype=Xs[0].dtype)
        for i, x in enumerate(Xs):
            output[i, : x.shape[0]] = x
        return output


"""
LSTM Notation (kind of involved, but made it a lot easier to write)

X: Inputs
Y: Outputs (aka hiddens)
C: Cells
G: Gates (Output of non-linearity, i.e. lstm_gates(X @ W.T)
A: Activations (X @ W.T, before non-linearity)

Imagine we have the input:
batch = [
    ["apple", "banana", "cantaloupe", "date", "elderberry"],
    ["aardvark", "bat", "capybara", "dingo", "elephant"]
]

The input variable X will have one vector per word, so X[0, 1] will be banana's
vector, X[0, 1, 0] will be a float, the first element of that vector.

We're computing an output variable Y of shape (nL, nB, nO), so that Y[0, 1] is
the output variable of banana.

A problem with variables for RNNs is keeping the timesteps straight. It's hard
to distinguish the current, previous, and next timesteps. To solve this problem,
we follow the convention that **we are at timestep 3**.

Additionally, the variables for Y and C are offset by one, as the 0th elements
have the initial hiddens and initial cells. So:

    t=3
    Xt3: The input vectors for 'dingo' and 'date', i.e. X[t]
    Yt3: The output vectors for 'dingo' and 'date', i.e. Y[t+1] (Y is offset.)
    Ct2: The cells calculated at 'c...', that are the input for 'd...'
    Ct3: The cells calculated at 'd...', that are the input for 'e...'
    At3: The activations at 'd...'
    Gt3: The gates at 'd...'
"""


def lstm_forward_training(
    params: Floats1d, c_init: Floats3d, h_init: Floats3d, X: Floats2d, lengths: Ints1d
) -> Tuple[Floats2d, Tuple]:
    xp = get_array_module(params)
    depth, dirs, nO = c_init.shape
    N, nI = X.shape
    batch_size = lengths[0]
    # Preallocate these so we can pass them through for loop.
    G = cast(Floats4d, xp.zeros((depth, dirs, X.shape[0], nO * 4), dtype="f"))
    Y = cast(Floats4d, xp.zeros((depth, dirs, X.shape[0], nO), dtype="f"))
    C = cast(Floats4d, xp.zeros((depth, dirs, X.shape[0], nO), dtype="f"))
    Yt2 = cast(Floats2d, xp.zeros((batch_size, nO), dtype="f"))
    Ct2 = cast(Floats2d, xp.zeros((batch_size, nO), dtype="f"))
    # Compute the start and end indices first.
    indices = []
    start = 0
    for batch_size in lengths:
        indices.append((start, start + batch_size))
        start += batch_size
    params_i = 0
    orig_X = X
    for i in range(depth):
        nI = X.shape[1]
        for d in range(dirs):
            # The inits are shaped (depth, dirs, nO). We add the internal dimension
            # to make them set correctly.
            Yt2 = h_init[i, d].reshape((1, nO))  # type: ignore[assignment]
            Ct2 = c_init[i, d].reshape((1, nO))  # type: ignore[assignment]
            layer_params, params_i = _split_weights(params, i, nO, nI, params_i)
            Wx, Wh, bias = _transpose_weights(layer_params)
            G[i, d] += xp.dot(X, Wx.T)
            G[i, d] += bias
            for start, end in indices if d == 0 else reversed(indices):
                # When we iterate left-to-right, t2 might be longer than t3.
                Yt2 = Yt2[: end - start]
                Ct2 = Ct2[: end - start]
                # But in right-to-left, it's the opposite: t3 can be longer.
                Gt3 = G[i, d, start:end]
                Gt3 = Gt3[: Yt2.shape[0]]
                Gt3 += xp.dot(Yt2, Wh.T)
                Gt3_ = cast(Floats3d, Gt3.reshape((-1, nO, 4)))
                hf = sigmoid(Gt3_[:, :, 0])
                hi = sigmoid(Gt3_[:, :, 1])
                ho = sigmoid(Gt3_[:, :, 2])
                hc = xp.tanh(Gt3_[:, :, 3])
                Ct3 = hf * Ct2
                Ct3 += hi * hc
                # Store results
                Gt3 = (
                    xp.hstack((hf, hi, ho, hc))
                    .reshape((-1, 4, nO))
                    .transpose((0, 2, 1))
                    .reshape((-1, nO * 4))
                )
                # Fix the endpoint to account for shorter slices when iterating
                # reversed. Not 100% sure this is right. If there's a bug, look
                # here?
                end = min(end, start + ho.shape[0])
                Y[i, d, start:end] = xp.tanh(Ct3) * ho
                G[i, d, start:end] = Gt3
                C[i, d, start:end] = Ct3
                # Set the t2 variables to the current t3 variables.
                Ct2 = Ct3
                Yt2 = Y[i, d, start:end]
        H = cast(Floats2d, Y[i].transpose((1, 0, 2)).reshape((N, -1)))
        if dirs == 2:
            H = xp.ascontiguousarray(H)
        X = H
    return H, (Y, G, C, orig_X)


def backprop_lstm(dY: Floats2d, lengths: Ints1d, params: Floats1d, fwd_state: Tuple):
    xp = get_array_module(params)

    Y: Floats4d
    G: Floats4d
    C: Floats4d
    X: Floats2d
    Wx: Floats2d
    Wh: Floats2d
    bias: Floats1d
    dWx: Floats2d
    dWh: Floats2d
    d_bias: Floats1d
    Y, G, C, X = fwd_state
    depth, dirs, N, nO = C.shape
    nI = X.shape[1]
    batch_size = lengths[0]
    # We don't need to store all the cells for all the layers.
    dC = cast(Floats2d, xp.zeros((N, nO), dtype=C.dtype))
    dG = cast(Floats2d, xp.zeros((N, nO * 4), dtype=C.dtype))
    d_params = cast(Floats1d, xp.zeros((params.shape[0],), dtype=params.dtype))
    # Collect the params and slices. It makes it a bit easier to get the indexing
    # right, when we're iterating backwards.
    params_i = 0
    all_layer_params: List[List[Tuple[Tuple[Floats2d, Floats2d, Floats1d], int]]] = []
    for i in range(depth):
        all_layer_params.append([])
        n_inputs = nI if i == 0 else (nO * dirs)
        for d in range(dirs):
            layer_params, params_i = _split_weights(params, i, nO, n_inputs, params_i)
            layer_params = _transpose_weights(layer_params)
            all_layer_params[-1].append((layer_params, params_i))
    params_i = 0
    all_layer_grads: List[List[Tuple[Tuple[Floats2d, Floats2d, Floats1d], int]]] = []
    for i in range(depth):
        all_layer_grads.append([])
        n_inputs = nI if i == 0 else (nO * dirs)
        for d in range(dirs):
            layer_grads, params_i = _split_weights(d_params, i, nO, n_inputs, params_i)
            layer_grads = _transpose_weights(layer_grads)
            all_layer_grads[-1].append((layer_grads, params_i))
    # Similarly, we want to compute the indices first
    indices = []
    start = 0
    for batch_size in lengths:
        indices.append((start, start + batch_size))
        start += batch_size

    Xs = [X] + [
        cast(Floats2d, Y[i].transpose((1, 0, 2)).reshape((N, -1)))
        for i in range(depth - 1)
    ]
    dXs = [xp.zeros((X.shape[0], X.shape[1]), dtype=X.dtype) for X in Xs]
    # Okay, now do the actual looping
    for i in reversed(range(depth)):
        dY3d = cast(Floats3d, dY.reshape((N, dirs, nO)).transpose((1, 0, 2)))
        dX = dXs[i]
        X = Xs[i]
        if dirs >= 2:
            dY3d = xp.ascontiguousarray(dY3d)
        for d in range(dirs):
            Wx, Wh, bias = all_layer_params[i][d][0]
            dWx, dWh, d_bias = all_layer_grads[i][d][0]
            if d == 0:
                start_t3, end_t3 = indices[-1]
                layer_indices = indices[:-1]
                layer_indices.reverse()
            else:
                start_t3, end_t3 = indices[0]
                layer_indices = indices[1:]
            for start_t2, end_t2 in layer_indices:
                size = min(end_t2 - start_t2, end_t3 - start_t3)
                dGt3, dCt2 = backprop_lstm_gates(
                    dY3d[d, start_t3 : start_t3 + size],
                    dC[start_t3 : start_t3 + size],
                    G[i, d, start_t3 : start_t3 + size],
                    C[i, d, start_t3 : start_t3 + size],
                    C[i, d, start_t2 : start_t2 + size],
                )
                # Backprop hidden-to-hidden w.r.t. hidden.
                dY3d[d, start_t2 : start_t2 + size] += dGt3 @ Wh
                # Update iteration variables
                dC[start_t2 : start_t2 + size] = dCt2
                start_t3 = start_t2
                end_t3 = end_t2
            # Backprop input-to-hidden w.r.t. weights.
            dWx += dG.T @ X
            # Backprop hidden-to-hidden w.r.t. weights.
            dWh += dG.T @ Y[i, d]
            # Backprop bias
            d_bias += dG.sum(axis=0)
            # Backprop input-to-hidden w.r.t. input
            dX += dG @ Wx
        dY = dX
    assert dX.shape[1] == X.shape[1]
    grad_parts = []
    for layer_grads in all_layer_grads:
        for dir_grads, _ in layer_grads:
            grad_parts.append(_untranspose_unsplit_weights(dir_grads))
    return dX, xp.concatenate(grad_parts)


def _split_weights(params: Floats1d, i: int, nO: int, nI: int, params_i: int):
    Wx_size = 4 * nO * nI
    bx_size = 4 * nO
    Wh_size = 4 * nO * nO
    bh_size = 4 * nO
    Wx = params[params_i : params_i + Wx_size].reshape((4 * nO, nI))
    params_i += Wx_size
    bx = params[params_i : params_i + bx_size].reshape((4 * nO,))
    params_i += bx_size
    Wh = params[params_i : params_i + Wh_size].reshape((4 * nO, nO))
    params_i += Wh_size
    bh = params[params_i : params_i + bh_size].reshape((4 * nO,))
    params_i += bh_size
    return ((Wx, bx), (Wh, bh)), params_i


def _transpose_weights(params):
    # Transpose the parameters so that the gates are the last dimension. This
    # makes it easier to fuse.
    (Wx, bx), (Wh, bh) = params
    xp = get_array_module(Wx)
    Wx = Wx.reshape((4, -1, Wx.shape[-1]))
    Wx = Wx.transpose((1, 0, 2)).reshape((-1, Wx.shape[-1]))
    bx = bx.reshape((4, -1)).transpose((1, 0)).reshape((-1,))
    Wh = Wh.reshape((4, -1, Wh.shape[-1]))
    Wh = Wh.transpose((1, 0, 2)).reshape((-1, Wh.shape[-1]))
    bh = bh.reshape((4, -1)).transpose((1, 0)).reshape((-1,))
    ascontig = xp.ascontiguousarray
    Wx = ascontig(Wx)
    Wh = ascontig(Wh)
    bias = ascontig(bx) + bh
    return Wx, Wh, bias


def _untranspose_unsplit_weights(params):
    Wx, Wh, bias = params
    xp = get_array_module(Wx)
    nO = Wh.shape[1]
    nI = Wx.shape[1]
    Wx = Wx.reshape((-1, 4, nI)).transpose((1, 0, 2)).reshape((-1, nI))
    Wh = Wh.reshape((-1, 4, nO)).transpose((1, 0, 2)).reshape((-1, nO))
    bias = bias.reshape((-1, 4)).transpose((1, 0)).reshape((-1,))
    zeros = xp.zeros(bias.shape, dtype="f")
    return xp.concatenate((Wx.ravel(), bias, Wh.ravel(), zeros))


def backprop_lstm_gates(
    dYt3: Floats2d, dCt3: Floats2d, Gt3: Floats2d, Ct3: Floats2d, Ct2: Floats2d
) -> Tuple[Floats2d, Floats2d]:
    # See above for notation. Step numbering refers to forward_lstm_gates
    xp = get_array_module(dYt3)
    hf, hi, ho, hc = xp.split(Gt3, 4, axis=-1)
    assert hf.shape[0] == hi.shape[0] == ho.shape[0] == hc.shape[0]
    assert hf.shape[0] == dYt3.shape[0] == dCt3.shape[0] == Ct3.shape[0] == Ct2.shape[0]
    tanhCt3 = xp.tanh(Ct3)
    # 3b: Yt3 = tanhCt3 * ho
    d_ho = dYt3 * tanhCt3
    d_tanhCt3 = dYt3 * ho
    # 3a: tanhCt3 = tanh(Ct3)
    dCt3 += d_tanhCt3 * dtanh(tanhCt3)
    # 2b: Ct3 += hi * hc
    d_hi = dCt3 * hc
    d_hc = dCt3 * hi
    # 2a: Ct3 = hf * Ct2
    d_hf = dCt3 * Ct2
    dCt2 = dCt3 * hf
    d_At3_hc = d_hc * dtanh(hc)  # 1d
    d_At3_ho = d_ho * dsigmoid(ho)  # 1c
    d_At3_hi = d_hi * dsigmoid(hi)  # 1b
    d_At3_hf = d_hf * dsigmoid(hf)  # 1a
    dAt3 = xp.concatenate((d_At3_hf, d_At3_hi, d_At3_ho, d_At3_hc), axis=-1)
    return dAt3, dCt2


def sigmoid(X, out=None):
    xp = get_array_module(X)

    # To prevent overflows and help with regularization/numerical stability
    X = xp.clip(X, -20.0, 20.0)
    return 1.0 / (1.0 + xp.exp(-X))


def dsigmoid(Y: ArrayT) -> ArrayT:
    return Y * (1.0 - Y)


def dtanh(Y: ArrayT) -> ArrayT:
    return 1 - Y**2


def gaussian_cdf(ops: Ops, X: FloatsXdT) -> FloatsXdT:
    """Gaussian CDF for distribution with mean 0 and stdev 1."""
    return 0.5 * (1.0 + ops.erf(INV_SQRT2 * X))


def gaussian_pdf(ops: Ops, X: FloatsXdT) -> FloatsXdT:
    """Gaussian PDF for distribution with mean 0 and stdev 1."""
    return INV_SQRT_2PI * ops.xp.exp(-0.5 * X * X)


def _check_compatible_shape(u: FloatsXd, v: FloatsXd):
    if u.shape != v.shape:
        msg = f"arrays have incompatible shapes: {u.shape} and {v.shape}"
        raise ValueError(msg)
