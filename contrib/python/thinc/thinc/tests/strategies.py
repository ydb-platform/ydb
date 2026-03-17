import numpy
from hypothesis.extra.numpy import arrays
from hypothesis.strategies import floats, integers, just, tuples

from thinc.api import Linear, NumpyOps


def get_ops():
    return NumpyOps()


def get_model(W_values, b_values):
    model = Linear(W_values.shape[0], W_values.shape[1], ops=NumpyOps())
    model.initialize()
    model.set_param("W", W_values)
    model.set_param("b", b_values)
    return model


def get_output(input_, W_values, b_values):
    return numpy.einsum("oi,bi->bo", W_values, input_) + b_values


def get_input(nr_batch, nr_in):
    ops = NumpyOps()
    return ops.alloc2f(nr_batch, nr_in)


def lengths(lo=1, hi=10):
    return integers(min_value=lo, max_value=hi)


def shapes(min_rows=1, max_rows=100, min_cols=1, max_cols=100):
    return tuples(lengths(lo=min_rows, hi=max_rows), lengths(lo=min_cols, hi=max_cols))


def ndarrays_of_shape(shape, lo=-10.0, hi=10.0, dtype="float32", width=32):
    if dtype.startswith("float"):
        return arrays(
            dtype, shape=shape, elements=floats(min_value=lo, max_value=hi, width=width)
        )
    else:
        return arrays(dtype, shape=shape, elements=integers(min_value=lo, max_value=hi))


def ndarrays(min_len=0, max_len=10, min_val=-10.0, max_val=10.0):
    return lengths(lo=1, hi=2).flatmap(
        lambda n: ndarrays_of_shape(n, lo=min_val, hi=max_val)
    )


def arrays_BI(min_B=1, max_B=10, min_I=1, max_I=100):
    shapes = tuples(lengths(lo=min_B, hi=max_B), lengths(lo=min_I, hi=max_I))
    return shapes.flatmap(ndarrays_of_shape)


def arrays_BOP(min_B=1, max_B=10, min_O=1, max_O=100, min_P=1, max_P=5):
    shapes = tuples(
        lengths(lo=min_B, hi=max_B),
        lengths(lo=min_O, hi=max_O),
        lengths(lo=min_P, hi=max_P),
    )
    return shapes.flatmap(ndarrays_of_shape)


def arrays_BOP_BO(min_B=1, max_B=10, min_O=1, max_O=100, min_P=1, max_P=5):
    shapes = tuples(
        lengths(lo=min_B, hi=max_B),
        lengths(lo=min_O, hi=max_O),
        lengths(lo=min_P, hi=max_P),
    )
    return shapes.flatmap(
        lambda BOP: tuples(ndarrays_of_shape(BOP), ndarrays_of_shape(BOP[:-1]))
    )


def arrays_BI_BO(min_B=1, max_B=10, min_I=1, max_I=100, min_O=1, max_O=100):
    shapes = tuples(
        lengths(lo=min_B, hi=max_B),
        lengths(lo=min_I, hi=max_I),
        lengths(lo=min_O, hi=max_O),
    )
    return shapes.flatmap(
        lambda BIO: tuples(
            ndarrays_of_shape((BIO[0], BIO[1])), ndarrays_of_shape((BIO[0], BIO[2]))
        )
    )


def arrays_OI_O_BI(
    min_batch=1, max_batch=16, min_out=1, max_out=16, min_in=1, max_in=16
):
    shapes = tuples(
        lengths(lo=min_batch, hi=max_batch),
        lengths(lo=min_in, hi=max_out),
        lengths(lo=min_in, hi=max_in),
    )

    def W_b_inputs(shape):
        batch_size, nr_out, nr_in = shape
        W = ndarrays_of_shape((nr_out, nr_in))
        b = ndarrays_of_shape((nr_out,))
        input_ = ndarrays_of_shape((batch_size, nr_in))
        return tuples(W, b, input_)

    return shapes.flatmap(W_b_inputs)


def arrays_OPFI_BI_lengths(max_B=5, max_P=3, max_F=5, max_I=8):
    shapes = tuples(
        lengths(hi=max_B),
        lengths(hi=max_P),
        lengths(hi=max_F),
        lengths(hi=max_I),
        arrays("int32", shape=(5,), elements=integers(min_value=1, max_value=10)),
    )

    strat = shapes.flatmap(
        lambda opfi_lengths: tuples(
            ndarrays_of_shape(opfi_lengths[:-1]),
            ndarrays_of_shape((sum(opfi_lengths[-1]), opfi_lengths[-2])),
            just(opfi_lengths[-1]),
        )
    )
    return strat
