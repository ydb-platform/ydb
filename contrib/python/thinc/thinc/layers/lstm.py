from functools import partial
from typing import Callable, Optional, Tuple, cast

from ..backends import Ops
from ..config import registry
from ..initializers import glorot_uniform_init, zero_init
from ..model import Model
from ..types import Floats1d, Floats2d, Floats4d, Padded, Ragged
from ..util import get_width
from .noop import noop


@registry.layers("LSTM.v1")
def LSTM(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    *,
    bi: bool = False,
    depth: int = 1,
    dropout: float = 0.0,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
) -> Model[Padded, Padded]:
    if depth == 0:
        msg = "LSTM depth must be at least 1. Maybe we should make this a noop?"
        raise ValueError(msg)
    if init_W is None:
        init_W = glorot_uniform_init
    if init_b is None:
        init_b = zero_init

    model: Model[Padded, Padded] = Model(
        "lstm",
        forward,
        dims={"nO": nO, "nI": nI, "depth": depth, "dirs": 1 + int(bi)},
        attrs={"registry_name": "LSTM.v1", "dropout_rate": dropout},
        params={"LSTM": None, "HC0": None},
        init=partial(init, init_W, init_b),
    )
    return model


@registry.layers("PyTorchLSTM.v1")
def PyTorchLSTM(
    nO: int, nI: int, *, bi: bool = False, depth: int = 1, dropout: float = 0.0
) -> Model[Padded, Padded]:
    import torch.nn

    from .pytorchwrapper import PyTorchRNNWrapper
    from .with_padded import with_padded

    if depth == 0:
        return noop()  # type: ignore[misc]
    nH = nO
    if bi:
        nH = nO // 2
    pytorch_rnn = PyTorchRNNWrapper(
        torch.nn.LSTM(nI, nH, depth, bidirectional=bi, dropout=dropout)
    )
    pytorch_rnn.set_dim("nO", nO)
    pytorch_rnn.set_dim("nI", nI)
    return with_padded(pytorch_rnn)


def init(
    init_W: Callable,
    init_b: Callable,
    model: Model,
    X: Optional[Padded] = None,
    Y: Optional[Padded] = None,
) -> None:
    if X is not None:
        model.set_dim("nI", get_width(X))
    if Y is not None:
        model.set_dim("nO", get_width(Y))
    nH = int(model.get_dim("nO") / model.get_dim("dirs"))
    nI = model.get_dim("nI")
    depth = model.get_dim("depth")
    dirs = model.get_dim("dirs")
    # It's easiest to use the initializer if we alloc the weights separately
    # and then stick them all together afterwards. The order matters here:
    # we need to keep the same format that CuDNN expects.
    params = []
    # Convenience
    init_W = partial(init_W, model.ops)
    init_b = partial(init_b, model.ops)
    layer_nI = nI
    for i in range(depth):
        for j in range(dirs):
            # Input-to-gates weights and biases.
            params.append(init_W((nH, layer_nI)))
            params.append(init_W((nH, layer_nI)))
            params.append(init_W((nH, layer_nI)))
            params.append(init_W((nH, layer_nI)))
            params.append(init_b((nH,)))
            params.append(init_b((nH,)))
            params.append(init_b((nH,)))
            params.append(init_b((nH,)))
            # Hidden-to-gates weights and biases
            params.append(init_W((nH, nH)))
            params.append(init_W((nH, nH)))
            params.append(init_W((nH, nH)))
            params.append(init_W((nH, nH)))
            params.append(init_b((nH,)))
            params.append(init_b((nH,)))
            params.append(init_b((nH,)))
            params.append(init_b((nH,)))
        layer_nI = nH * dirs
    model.set_param("LSTM", model.ops.xp.concatenate([p.ravel() for p in params]))
    model.set_param("HC0", zero_init(model.ops, (2, depth, dirs, nH)))
    size = model.get_param("LSTM").size
    expected = 4 * dirs * nH * (nH + nI) + dirs * (8 * nH)
    for _ in range(1, depth):
        expected += 4 * dirs * (nH + nH * dirs) * nH + dirs * (8 * nH)
    assert size == expected, (size, expected)


def forward(
    model: Model[Padded, Padded], Xp: Padded, is_train: bool
) -> Tuple[Padded, Callable]:
    dropout = model.attrs["dropout_rate"]
    Xr = _padded_to_packed(model.ops, Xp)
    LSTM = cast(Floats1d, model.get_param("LSTM"))
    HC0 = cast(Floats4d, model.get_param("HC0"))
    H0 = HC0[0]
    C0 = HC0[1]
    if is_train:
        # Apply dropout over *weights*, not *activations*. RNN activations are
        # heavily correlated, so dropout over the activations is less effective.
        # This trick was explained in Yarin Gal's thesis, and popularised by
        # Smerity in the AWD-LSTM. It also means we can do the dropout outside
        # of the backend, improving compatibility.
        mask = cast(Floats1d, model.ops.get_dropout_mask(LSTM.shape, dropout))
        LSTM = LSTM * mask
        Y, fwd_state = model.ops.lstm_forward_training(
            LSTM, H0, C0, cast(Floats2d, Xr.data), Xr.lengths
        )
    else:
        Y = model.ops.lstm_forward_inference(
            LSTM, H0, C0, cast(Floats2d, Xr.data), Xr.lengths
        )
        fwd_state = tuple()
    assert Y.shape == (Xr.data.shape[0], Y.shape[1]), (Xr.data.shape, Y.shape)
    Yp = _packed_to_padded(model.ops, Ragged(Y, Xr.lengths), Xp)

    def backprop(dYp: Padded) -> Padded:
        assert fwd_state
        dYr = _padded_to_packed(model.ops, dYp)
        dX, dLSTM = model.ops.backprop_lstm(
            cast(Floats2d, dYr.data), dYr.lengths, LSTM, fwd_state
        )
        dLSTM *= mask
        model.inc_grad("LSTM", dLSTM)
        return _packed_to_padded(model.ops, Ragged(dX, dYr.lengths), dYp)

    return Yp, backprop


def _padded_to_packed(ops: Ops, Xp: Padded) -> Ragged:
    """Strip padding from a padded sequence."""
    assert Xp.lengths.sum() == Xp.size_at_t.sum(), (
        Xp.lengths.sum(),
        Xp.size_at_t.sum(),
    )
    Y = ops.alloc2f(Xp.lengths.sum(), Xp.data.shape[2])
    start = 0
    for t in range(Xp.size_at_t.shape[0]):
        batch_size = Xp.size_at_t[t]
        Y[start : start + batch_size] = Xp.data[t, :batch_size]  # type: ignore[assignment]
        start += batch_size
    return Ragged(Y, Xp.size_at_t)


def _packed_to_padded(ops: Ops, Xr: Ragged, Xp: Padded) -> Padded:
    Y = ops.alloc3f(Xp.data.shape[0], Xp.data.shape[1], Xr.data.shape[1])
    X = cast(Floats2d, Xr.data)
    start = 0
    for t in range(Xp.size_at_t.shape[0]):
        batch_size = Xp.size_at_t[t]
        Y[t, :batch_size] = X[start : start + batch_size]
        start += batch_size
    return Padded(Y, size_at_t=Xp.size_at_t, lengths=Xp.lengths, indices=Xp.indices)
