import random

import pytest

from thinc.api import (
    Adam,
    HashEmbed,
    Model,
    Relu,
    Softmax,
    chain,
    expand_window,
    strings2arrays,
    with_array,
)


@pytest.fixture(scope="module")
def ancora():
    pytest.importorskip("ml_datasets")
    import ml_datasets

    return ml_datasets.ud_ancora_pos_tags()


def create_embed_relu_relu_softmax(depth, width, vector_length):
    with Model.define_operators({">>": chain}):
        model = strings2arrays() >> with_array(
            HashEmbed(width, vector_length, column=0)
            >> expand_window(window_size=1)
            >> Relu(width, width * 3)
            >> Relu(width, width)
            >> Softmax(17, width)
        )
    return model


@pytest.fixture(params=[create_embed_relu_relu_softmax])
def create_model(request):
    return request.param


def evaluate_tagger(model, dev_X, dev_Y, batch_size):
    correct = 0.0
    total = 0.0
    for i in range(0, len(dev_X), batch_size):
        Yh = model.predict(dev_X[i : i + batch_size])
        Y = dev_Y[i : i + batch_size]
        for j in range(len(Yh)):
            correct += (Yh[j].argmax(axis=1) == Y[j].argmax(axis=1)).sum()
            total += Yh[j].shape[0]
    return correct / total


def get_shuffled_batches(Xs, Ys, batch_size):
    zipped = list(zip(Xs, Ys))
    random.shuffle(zipped)
    for i in range(0, len(zipped), batch_size):
        batch_X, batch_Y = zip(*zipped[i : i + batch_size])
        yield list(batch_X), list(batch_Y)


@pytest.mark.parametrize(
    ("depth", "width", "vector_width", "nb_epoch"), [(2, 32, 16, 5)]
)
def test_small_end_to_end(depth, width, vector_width, nb_epoch, create_model, ancora):
    (train_X, train_Y), (dev_X, dev_Y) = ancora
    batch_size = 8
    model = create_model(depth, width, vector_width).initialize()
    optimizer = Adam(0.001)
    losses = []
    scores = []
    for _ in range(nb_epoch):
        losses.append(0.0)
        for X, Y in get_shuffled_batches(train_X, train_Y, batch_size):
            Yh, backprop = model.begin_update(X)
            d_loss = []
            for i in range(len(Yh)):
                d_loss.append(Yh[i] - Y[i])
                losses[-1] += ((Yh[i] - Y[i]) ** 2).sum()
            backprop(d_loss)
            model.finish_update(optimizer)
        scores.append(evaluate_tagger(model, dev_X, dev_Y, batch_size))
    assert losses[-1] < losses[0]
    assert scores[-1] > scores[0]
