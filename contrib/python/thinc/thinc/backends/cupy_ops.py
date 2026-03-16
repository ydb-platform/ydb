import numpy

from .. import registry
from ..compat import cublas, cupy, cupyx
from ..types import DeviceTypes
from ..util import (
    is_cupy_array,
    is_mxnet_gpu_array,
    is_tensorflow_gpu_array,
    is_torch_cuda_array,
    mxnet2xp,
    tensorflow2xp,
    torch2xp,
)
from . import _custom_kernels
from .numpy_ops import NumpyOps
from .ops import Ops


@registry.ops("CupyOps")
class CupyOps(Ops):
    name = "cupy"
    xp = cupy
    _xp2 = cupyx

    def __init__(
        self, device_type: DeviceTypes = "gpu", device_id: int = 0, **kwargs
    ) -> None:
        self.device_type = device_type
        self.device_id = device_id

    def to_numpy(self, data, *, byte_order=None):
        if not isinstance(data, numpy.ndarray):
            data = data.get()
        if byte_order:
            dtype = data.dtype.newbyteorder(byte_order)
            data = numpy.asarray(data, dtype=dtype)
        return data

    def gather_add(self, table, indices):
        if table.dtype in ("float32", "float64"):
            return _custom_kernels.gather_add(table, indices)
        else:
            return super().gather_add(table, indices)

    def dish(self, X, inplace=False):
        if X.dtype in ("float32", "float64"):
            return _custom_kernels.dish(X, inplace=inplace)
        else:
            return super().dish(X, inplace=inplace)

    def backprop_dish(self, dY, X, inplace=False):
        if X.dtype == dY.dtype and X.dtype in ("float32", "float64"):
            return _custom_kernels.backprop_dish(dY, X, inplace=inplace)
        else:
            return super().backprop_dish(dY, X, inplace=inplace)

    def gelu(self, X, inplace=False):
        if X.dtype in ("float32", "float64"):
            return _custom_kernels.gelu(X, inplace=inplace, threshold=6.0)
        else:
            return super().gelu(X, inplace=inplace)

    def backprop_gelu(self, dY, X, inplace=False):
        if X.dtype == dY.dtype and X.dtype in ("float32", "float64"):
            return _custom_kernels.backprop_gelu(dY, X, inplace=inplace, threshold=6.0)
        else:
            return super().backprop_gelu(dY, X, inplace=inplace)

    def gemm(self, x, y, out=None, trans1=False, trans2=False):
        if isinstance(x, numpy.ndarray) or isinstance(y, numpy.ndarray):
            raise ValueError(
                "Encountered a numpy array when processing with cupy. "
                "Did you call model.ops.asarray on your data?"
            )
        if trans1:
            x = x.T
        if trans2:
            y = y.T
        if out is None:
            return self.xp.dot(x, y)
        else:
            self.xp.dot(x, y, out=out)
            return out

    def asarray(self, data, dtype=None):
        # We'll try to perform a zero-copy conversion if possible.
        if is_cupy_array(data):
            array = self.xp.asarray(data, dtype=dtype)
        elif is_torch_cuda_array(data):
            array = torch2xp(data)
        elif is_tensorflow_gpu_array(data):
            array = tensorflow2xp(data)
        elif is_mxnet_gpu_array(data):
            array = mxnet2xp(data)
        else:
            array = self.xp.array(data, dtype=dtype)

        if dtype is not None:
            array = array.astype(dtype=dtype, copy=False)

        return array

    def pad(self, seqs, round_to=1):
        """Perform padding on a list of arrays so that they each have the same
        length, by taking the maximum dimension across each axis. This only
        works on non-empty sequences with the same `ndim` and `dtype`.
        """
        # TODO: This should be generalized to handle different ranks
        if not seqs:
            raise ValueError("Cannot pad empty sequence")
        if len(set(seq.ndim for seq in seqs)) != 1:
            raise ValueError("Cannot pad sequences with different ndims")
        if len(set(seq.dtype for seq in seqs)) != 1:
            raise ValueError("Cannot pad sequences with different dtypes")
        if len(set(seq.shape[1:] for seq in seqs)) != 1:
            raise ValueError("Cannot pad sequences that differ on other dimensions")

        # Our CUDA kernel can currently only handle C contiguous arrays.
        if not all(seq.flags["C_CONTIGUOUS"] for seq in seqs) or seqs[0].dtype not in (
            "float32",
            "float64",
            "int32",
            "int64",
        ):
            return super().pad(seqs, round_to)

        return _custom_kernels.pad(seqs, round_to)

    def maxout(self, X):
        if X.dtype in ("float32", "float64"):
            return _custom_kernels.maxout(X)
        else:
            return super().maxout(X)

    def backprop_maxout(self, dY, which, P):
        if dY.dtype in ("float32", "float64") and which.dtype == "int32":
            return _custom_kernels.backprop_maxout(dY, which, P)
        else:
            return super().backprop_maxout(dY, which, P)

    def relu(self, X, inplace=False):
        if not inplace:
            return X * (X > 0)
        else:
            X *= X > 0
            return X

    def backprop_relu(self, dY, Y, inplace=False):
        if not inplace:
            return dY * (Y > 0)
        dY *= Y > 0
        return dY

    def clipped_linear(
        self,
        X,
        slope: float = 1.0,
        offset: float = 0.0,
        min_val: float = 0.0,
        max_val: float = 1.0,
        inplace: bool = False,
    ):
        if X.dtype in ("float32", "float64"):
            return _custom_kernels.clipped_linear(
                X,
                inplace=inplace,
                slope=slope,
                offset=offset,
                min_val=min_val,
                max_val=max_val,
            )
        else:
            return super().clipped_linear(
                X,
                inplace=inplace,
                slope=slope,
                offset=offset,
                min_val=min_val,
                max_val=max_val,
            )

    def backprop_clipped_linear(
        self,
        dY,
        X,
        slope: float = 1.0,
        offset: float = 0.0,
        min_val: float = 0.0,
        max_val: float = 1.0,
        inplace: bool = False,
    ):
        if X.dtype == dY.dtype and X.dtype in ("float32", "float64"):
            return _custom_kernels.backprop_clipped_linear(
                dY,
                X,
                slope=slope,
                offset=offset,
                min_val=min_val,
                max_val=max_val,
                inplace=inplace,
            )
        else:
            return super().backprop_clipped_linear(
                dY=dY,
                X=X,
                slope=slope,
                offset=offset,
                min_val=min_val,
                max_val=max_val,
                inplace=inplace,
            )

    def backprop_hard_swish(self, dY, X, inplace: bool = False):
        if X.dtype == dY.dtype and X.dtype in ("float32", "float64"):
            return _custom_kernels.backprop_hard_swish(dY, X, inplace=inplace)
        else:
            return super().backprop_hard_swish(dY, X, inplace=inplace)

    def backprop_hard_swish_mobilenet(self, dY, X, inplace: bool = False):
        if X.dtype == dY.dtype and X.dtype in ("float32", "float64"):
            return _custom_kernels.backprop_hard_swish_mobilenet(dY, X, inplace=inplace)
        else:
            return super().backprop_hard_swish_mobilenet(dY, X, inplace=inplace)

    def mish(self, X, threshold=20.0, inplace=False):
        if X.dtype in ("float32", "float64"):
            return _custom_kernels.mish(X, inplace=inplace, threshold=threshold)
        else:
            return super().mish(X, threshold, inplace)

    def backprop_mish(self, dY, X, threshold=20.0, inplace=False):
        if X.dtype == dY.dtype and X.dtype in ("float32", "float64"):
            return _custom_kernels.backprop_mish(
                dY, X, inplace=inplace, threshold=threshold
            )
        else:
            return super().backprop_mish(dY, X, threshold, inplace)

    def swish(self, X, inplace=False):
        if X.dtype in ("float32", "float64"):
            return _custom_kernels.swish(X, inplace=inplace, threshold=17.0)
        else:
            return super().swish(X, inplace=inplace)

    def backprop_swish(self, dY, X, Y, inplace=False):
        if X.dtype == dY.dtype == Y.dtype and X.dtype in ("float32", "float64"):
            return _custom_kernels.backprop_swish(
                dY, X, Y, inplace=inplace, threshold=17.0
            )
        else:
            return super().backprop_swish(dY, X, Y, inplace=inplace)

    def clip_gradient(self, gradient, threshold):
        # We do not use CuPy's linalg.norm, since it uses scalar reductions
        # using one CUDA block. This is a lot slower than the cuBLAS
        # implementation.
        def frobenius_norm(X):
            X_vec = X.reshape(-1)
            return cublas.nrm2(X_vec)

        grad_norm = cupy.maximum(frobenius_norm(gradient), 1e-12)
        gradient *= cupy.minimum(threshold, grad_norm) / grad_norm
        return gradient

    def seq2col(self, seq, nW, *, lengths=None):
        """Given an (M, N) sequence of vectors, return an (M, N*(nW*2+1)) sequence.
        The new sequence is constructed by concatenating nW preceding and succeeding
        vectors onto each column in the sequence, to extract a window of features.
        """
        if seq.dtype in ("float32", "float64") and (
            lengths is None or lengths.dtype == "int32"
        ):
            return _custom_kernels.seq2col(seq, nW, lengths=lengths)
        else:
            return super().seq2col(seq, nW, lengths=lengths)

    def backprop_seq2col(self, dY, nW, *, lengths=None):
        if dY.dtype in ("float32", "float64") and (
            lengths is None or lengths.dtype == "int32"
        ):
            return _custom_kernels.backprop_seq2col(dY, nW, lengths=lengths)
        else:
            return super().backprop_seq2col(dY, nW, lengths=lengths)

    def reduce_mean(self, X, lengths):
        if X.dtype in ("float32", "float64") and lengths.dtype == "int32":
            return _custom_kernels.reduce_mean(X, lengths=lengths)
        else:
            super().reduce_mean(X, lengths)

    def backprop_reduce_mean(self, d_means, lengths):
        if d_means.dtype in ("float32", "float64") and lengths.dtype == "int32":
            return _custom_kernels.backprop_reduce_mean(d_means, lengths)
        else:
            super().backprop_reduce_mean(d_means, lengths)

    def reduce_max(self, X, lengths):
        if X.dtype in ("float32", "float64") and lengths.dtype == "int32":
            return _custom_kernels.reduce_max(X, lengths)
        else:
            super().reduce_max(X, lengths)

    def backprop_reduce_max(self, d_maxes, which, lengths):
        if (
            d_maxes.dtype in ("float32", "float64")
            and which.dtype == "int32"
            and lengths.dtype == "int32"
        ):
            return _custom_kernels.backprop_reduce_max(d_maxes, which, lengths)
        else:
            super().backprop_reduce_max(d_maxes, which, lengths)

    def reduce_sum(self, X, lengths):
        if X.dtype in ("float32", "float64") and lengths.dtype == "int32":
            return _custom_kernels.reduce_sum(X, lengths)
        else:
            return super().reduce_sum(X, lengths)

    def backprop_reduce_sum(self, d_sums, lengths):
        if d_sums.dtype in ("float32", "float64") and lengths.dtype == "int32":
            return _custom_kernels.backprop_reduce_sum(d_sums, lengths)
        else:
            return super().backprop_reduce_sum(d_sums, lengths)

    def hash(self, ids, seed):
        return _custom_kernels.hash(ids, seed)

    def scatter_add(self, table, indices, values):
        self._xp2.scatter_add(table, indices, values)

    def adam(
        self, weights, gradient, mom1, mom2, beta1, beta2, eps, learn_rate, mod_rate=1.0
    ):
        _check_compatible_shape(weights, gradient)
        _check_compatible_shape(weights, mom1)
        _check_compatible_shape(weights, mom2)

        adam_kernel(
            gradient, learn_rate, 1 - beta1, 1 - beta2, eps, weights, mom1, mom2
        )
        gradient.fill(0)
        return weights, gradient, mom1, mom2

    def position_encode(self, N, D, period=10000, out=None):
        positions = NumpyOps().position_encode(N, D, period=period, out=out)
        return self.asarray(positions)


if cupy is not None:
    adam_kernel = cupy.ElementwiseKernel(
        "T grad, T lr, T one_minus_beta1, T one_minus_beta2, T eps",
        "T param, T m, T v",
        """m += one_minus_beta1 * (grad - m);
        v += one_minus_beta2 * (grad * grad - v);
        param -= lr * m / (sqrt(v) + eps);""",
        "adam",
    )
else:
    adam_kernel = None


def _check_compatible_shape(u, v):
    if u.shape != v.shape:
        msg = f"arrays have incompatible shapes: {u.shape} and {v.shape}"
        raise ValueError(msg)
