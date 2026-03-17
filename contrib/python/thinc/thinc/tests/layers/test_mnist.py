import pytest

from thinc.api import (
    Adam,
    PyTorchWrapper,
    Relu,
    Softmax,
    TensorFlowWrapper,
    chain,
    clone,
    get_current_ops,
)
from thinc.compat import has_tensorflow, has_torch


@pytest.fixture(scope="module")
def mnist(limit=5000):
    pytest.importorskip("ml_datasets")
    import ml_datasets

    (train_X, train_Y), (dev_X, dev_Y) = ml_datasets.mnist()
    return (train_X[:limit], train_Y[:limit]), (dev_X[:limit], dev_Y[:limit])


def create_relu_softmax(width, dropout, nI, nO):
    return chain(clone(Relu(nO=width, dropout=dropout), 2), Softmax(10, width))


def create_wrapped_pytorch(width, dropout, nI, nO):
    import torch
    import torch.nn
    import torch.nn.functional as F

    class PyTorchModel(torch.nn.Module):
        def __init__(self, width, nO, nI, dropout):
            super(PyTorchModel, self).__init__()
            self.dropout1 = torch.nn.Dropout2d(dropout)
            self.dropout2 = torch.nn.Dropout2d(dropout)
            self.fc1 = torch.nn.Linear(nI, width)
            self.fc2 = torch.nn.Linear(width, nO)

        def forward(self, x):
            x = F.relu(x)
            x = self.dropout1(x)
            x = self.fc1(x)
            x = F.relu(x)
            x = self.dropout2(x)
            x = self.fc2(x)
            output = F.log_softmax(x, dim=1)
            return output

    return PyTorchWrapper(PyTorchModel(width, nO, nI, dropout))


def create_wrapped_tensorflow(width, dropout, nI, nO):
    from tensorflow.keras.layers import Dense, Dropout
    from tensorflow.keras.models import Sequential

    tf_model = Sequential()
    tf_model.add(Dense(width, activation="relu", input_shape=(nI,)))
    tf_model.add(Dropout(dropout))
    tf_model.add(Dense(width, activation="relu"))
    tf_model.add(Dropout(dropout))
    tf_model.add(Dense(nO, activation=None))
    return TensorFlowWrapper(tf_model)


@pytest.fixture(
    # fmt: off
    params=[
        create_relu_softmax,
        pytest.param(create_wrapped_pytorch, marks=pytest.mark.skipif(not has_torch, reason="needs PyTorch")),
        pytest.param(create_wrapped_tensorflow, marks=pytest.mark.skipif(not has_tensorflow, reason="needs TensorFlow"))
    ]
    # fmt: on
)
def create_model(request):
    return request.param


@pytest.mark.slow
@pytest.mark.parametrize(("width", "nb_epoch", "min_score"), [(32, 20, 0.8)])
def test_small_end_to_end(width, nb_epoch, min_score, create_model, mnist):
    batch_size = 128
    dropout = 0.2
    (train_X, train_Y), (dev_X, dev_Y) = mnist
    model = create_model(width, dropout, nI=train_X.shape[1], nO=train_Y.shape[1])
    model.initialize(X=train_X[:5], Y=train_Y[:5])
    optimizer = Adam(0.001)
    losses = []
    scores = []
    ops = get_current_ops()

    for i in range(nb_epoch):
        for X, Y in model.ops.multibatch(batch_size, train_X, train_Y, shuffle=True):
            Yh, backprop = model.begin_update(X)
            # Ensure that the tensor is type-compatible with the current backend.
            Yh = ops.asarray(Yh)

            backprop(Yh - Y)
            model.finish_update(optimizer)
            losses.append(((Yh - Y) ** 2).sum())
        correct = 0
        total = 0
        for X, Y in model.ops.multibatch(batch_size, dev_X, dev_Y):
            Yh = model.predict(X)
            Yh = ops.asarray(Yh)

            correct += (Yh.argmax(axis=1) == Y.argmax(axis=1)).sum()
            total += Yh.shape[0]
        score = correct / total
        scores.append(score)
    assert losses[-1] < losses[0], losses
    if scores[0] < 1.0:
        assert scores[-1] > scores[0], scores
    assert any([score > min_score for score in scores]), scores
