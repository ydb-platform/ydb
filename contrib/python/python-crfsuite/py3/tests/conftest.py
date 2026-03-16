import pytest


@pytest.fixture()
def xseq():
    return [
        {"walk": 1, "shop": 0.5},
        {"walk": 1},
        {"walk": 1, "clean": 0.5},
        {"shop": 0.5, "clean": 0.5},
        {"walk": 0.5, "clean": 1},
        {"clean": 1, "shop": 0.1},
        {"walk": 1, "shop": 0.5},
        {},
        {"clean": 1},
        {"солнце": "не светит".encode(), "clean": 1},
        {"world": 2},
    ]


@pytest.fixture
def yseq():
    return [
        "sunny",
        "sunny",
        "sunny",
        "rainy",
        "rainy",
        "rainy",
        "sunny",
        "sunny",
        "rainy",
        "rainy",
        "好",
    ]


@pytest.fixture
def model_filename(tmpdir, xseq, yseq):
    from pycrfsuite import Trainer

    trainer = Trainer("lbfgs", verbose=False)
    trainer.append(xseq, yseq)
    model_filename = str(tmpdir.join("model.crfsuite"))
    trainer.train(model_filename)
    return model_filename


@pytest.fixture
def model_bytes(model_filename):
    with open(model_filename, "rb") as f:
        return f.read()
