import inspect
import pickle
from types import GeneratorType
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import catalogue
import numpy
import pytest

try:
    from pydantic.v1 import BaseModel, PositiveInt, StrictBool, StrictFloat, constr
except ImportError:
    from pydantic import BaseModel, PositiveInt, StrictBool, StrictFloat, constr  # type: ignore

import thinc.config
from thinc.api import Config, Model, NumpyOps, RAdam
from thinc.config import ConfigValidationError
from thinc.types import Generator, Ragged
from thinc.util import partial

from .util import make_tempdir

EXAMPLE_CONFIG = """
[optimizer]
@optimizers = "Adam.v1"
beta1 = 0.9
beta2 = 0.999
use_averages = true

[optimizer.learn_rate]
@schedules = "warmup_linear.v1"
initial_rate = 0.1
warmup_steps = 10000
total_steps = 100000

[pipeline]

[pipeline.parser]
name = "parser"
factory = "parser"

[pipeline.parser.model]
@layers = "spacy.ParserModel.v1"
hidden_depth = 1
hidden_width = 64
token_vector_width = 128

[pipeline.parser.model.tok2vec]
@layers = "Tok2Vec.v1"
width = ${pipeline.parser.model:token_vector_width}

[pipeline.parser.model.tok2vec.embed]
@layers = "spacy.MultiFeatureHashEmbed.v1"
width = ${pipeline.parser.model.tok2vec:width}

[pipeline.parser.model.tok2vec.embed.hidden]
@layers = "MLP.v1"
depth = 1
pieces = 3
layer_norm = true
outputs = ${pipeline.parser.model.tok2vec.embed:width}

[pipeline.parser.model.tok2vec.encode]
@layers = "spacy.MaxoutWindowEncoder.v1"
depth = 4
pieces = 3
window_size = 1

[pipeline.parser.model.lower]
@layers = "spacy.ParserLower.v1"

[pipeline.parser.model.upper]
@layers = "thinc.Linear.v1"
"""

OPTIMIZER_CFG = """
[optimizer]
@optimizers = "Adam.v1"
beta1 = 0.9
beta2 = 0.999
use_averages = true

[optimizer.learn_rate]
@schedules = "warmup_linear.v1"
initial_rate = 0.1
warmup_steps = 10000
total_steps = 100000
"""


class my_registry(thinc.config.registry):
    cats = catalogue.create("thinc", "tests", "cats", entry_points=False)


class HelloIntsSchema(BaseModel):
    hello: int
    world: int

    class Config:
        extra = "forbid"


class DefaultsSchema(BaseModel):
    required: int
    optional: str = "default value"

    class Config:
        extra = "forbid"


class ComplexSchema(BaseModel):
    outer_req: int
    outer_opt: str = "default value"

    level2_req: HelloIntsSchema
    level2_opt: DefaultsSchema = DefaultsSchema(required=1)


@my_registry.cats.register("catsie.v1")
def catsie_v1(evil: StrictBool, cute: bool = True) -> str:
    if evil:
        return "scratch!"
    else:
        return "meow"


@my_registry.cats.register("catsie.v2")
def catsie_v2(evil: StrictBool, cute: bool = True, cute_level: int = 1) -> str:
    if evil:
        return "scratch!"
    else:
        if cute_level > 2:
            return "meow <3"
        return "meow"


good_catsie = {"@cats": "catsie.v1", "evil": False, "cute": True}
ok_catsie = {"@cats": "catsie.v1", "evil": False, "cute": False}
bad_catsie = {"@cats": "catsie.v1", "evil": True, "cute": True}
worst_catsie = {"@cats": "catsie.v1", "evil": True, "cute": False}


def test_make_config_positional_args_dicts():
    cfg = {
        "hyper_params": {"n_hidden": 512, "dropout": 0.2, "learn_rate": 0.001},
        "model": {
            "@layers": "chain.v1",
            "*": {
                "relu1": {"@layers": "Relu.v1", "nO": 512, "dropout": 0.2},
                "relu2": {"@layers": "Relu.v1", "nO": 512, "dropout": 0.2},
                "softmax": {"@layers": "Softmax.v1"},
            },
        },
        "optimizer": {"@optimizers": "Adam.v1", "learn_rate": 0.001},
    }
    resolved = my_registry.resolve(cfg)
    model = resolved["model"]
    X = numpy.ones((784, 1), dtype="f")
    model.initialize(X=X, Y=numpy.zeros((784, 1), dtype="f"))
    model.begin_update(X)
    model.finish_update(resolved["optimizer"])


def test_objects_from_config():
    config = {
        "optimizer": {
            "@optimizers": "my_cool_optimizer.v1",
            "beta1": 0.2,
            "learn_rate": {
                "@schedules": "my_cool_repetitive_schedule.v1",
                "base_rate": 0.001,
                "repeat": 4,
            },
        }
    }

    @thinc.registry.optimizers.register("my_cool_optimizer.v1")
    def make_my_optimizer(learn_rate: List[float], beta1: float):
        return RAdam(learn_rate, beta1=beta1)

    @thinc.registry.schedules("my_cool_repetitive_schedule.v1")
    def decaying(base_rate: float, repeat: int) -> List[float]:
        return repeat * [base_rate]

    optimizer = my_registry.resolve(config)["optimizer"]
    assert optimizer.b1 == 0.2
    assert "learn_rate" in optimizer.schedules
    assert optimizer.learn_rate == 0.001


def test_handle_generic_model_type():
    """Test that validation can handle checks against arbitrary generic
    types in function argument annotations."""

    @my_registry.layers("my_transform.v1")
    def my_transform(model: Model[int, int]):
        model.name = "transformed_model"
        return model

    cfg = {"@layers": "my_transform.v1", "model": {"@layers": "Linear.v1"}}
    model = my_registry.resolve({"test": cfg})["test"]
    assert isinstance(model, Model)
    assert model.name == "transformed_model"


def test_arg_order_is_preserved():
    str_cfg = """
    [model]

    [model.chain]
    @layers = "chain.v1"

    [model.chain.*.hashembed]
    @layers = "HashEmbed.v1"
    nO = 8
    nV = 8

    [model.chain.*.expand_window]
    @layers = "expand_window.v1"
    window_size = 1
    """

    cfg = Config().from_str(str_cfg)
    resolved = my_registry.resolve(cfg)
    model = resolved["model"]["chain"]

    # Fails when arguments are sorted, because expand_window
    # is sorted before hashembed.
    assert model.name == "hashembed>>expand_window"
