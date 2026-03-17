from typing import Callable, cast

import numpy

from .backends import Ops
from .config import registry
from .types import FloatsXd, Shape
from .util import partial

# TODO: Harmonize naming with Keras, and fill in missing entries
# https://keras.io/initializers/ We should also have He normal/uniform
# and probably lecun normal/uniform.

# Initialize via numpy, before copying to ops. This makes it easier to work with
# the different backends, because the backend won't affect the randomization.


def lecun_normal_init(ops: Ops, shape: Shape) -> FloatsXd:
    scale = numpy.sqrt(1.0 / shape[1])
    return ops.asarray_f(cast(FloatsXd, numpy.random.normal(0, scale, shape)))


@registry.initializers("lecun_normal_init.v1")
def configure_lecun_normal_init() -> Callable[[Shape], FloatsXd]:
    return partial(lecun_normal_init)


def he_normal_init(ops: Ops, shape: Shape) -> FloatsXd:
    scale = numpy.sqrt(2.0 / shape[1])
    return ops.asarray_f(cast(FloatsXd, numpy.random.normal(0, scale, shape)))


@registry.initializers("he_normal_init.v1")
def configure_he_normal_init() -> Callable[[Shape], FloatsXd]:
    return partial(he_normal_init)


def glorot_normal_init(ops: Ops, shape: Shape) -> FloatsXd:
    scale = numpy.sqrt(2.0 / (shape[1] + shape[0]))
    return ops.asarray_f(cast(FloatsXd, numpy.random.normal(0, scale, shape)))


@registry.initializers("glorot_normal_init.v1")
def configure_glorot_normal_init() -> Callable[[Shape], FloatsXd]:
    return partial(glorot_normal_init)


def he_uniform_init(ops: Ops, shape: Shape) -> FloatsXd:
    scale = numpy.sqrt(6.0 / shape[1])
    return ops.asarray_f(cast(FloatsXd, numpy.random.uniform(-scale, scale, shape)))


@registry.initializers("he_uniform_init.v1")
def configure_he_uniform_init() -> Callable[[Shape], FloatsXd]:
    return partial(he_uniform_init)


def lecun_uniform_init(ops: Ops, shape: Shape) -> FloatsXd:
    scale = numpy.sqrt(3.0 / shape[1])
    return ops.asarray_f(cast(FloatsXd, numpy.random.uniform(-scale, scale, shape)))


@registry.initializers("lecun_uniform_init.v1")
def configure_lecun_uniform_init() -> Callable[[Shape], FloatsXd]:
    return partial(lecun_uniform_init)


def glorot_uniform_init(ops: Ops, shape: Shape) -> FloatsXd:
    scale = numpy.sqrt(6.0 / (shape[0] + shape[1]))
    return ops.asarray_f(cast(FloatsXd, numpy.random.uniform(-scale, scale, shape)))


@registry.initializers("glorot_uniform_init.v1")
def configure_glorot_uniform_init() -> Callable[[Shape], FloatsXd]:
    return partial(glorot_uniform_init)


def zero_init(ops: Ops, shape: Shape) -> FloatsXd:
    return ops.alloc_f(shape)


@registry.initializers("zero_init.v1")
def configure_zero_init() -> Callable[[FloatsXd], FloatsXd]:
    return partial(zero_init)


def uniform_init(
    ops: Ops, shape: Shape, *, lo: float = -0.1, hi: float = 0.1
) -> FloatsXd:
    values = numpy.random.uniform(lo, hi, shape)
    return ops.asarray_f(cast(FloatsXd, values.astype("float32")))


@registry.initializers("uniform_init.v1")
def configure_uniform_init(
    *, lo: float = -0.1, hi: float = 0.1
) -> Callable[[FloatsXd], FloatsXd]:
    return partial(uniform_init, lo=lo, hi=hi)


def normal_init(ops: Ops, shape: Shape, *, mean: float = 0) -> FloatsXd:
    size = int(ops.xp.prod(ops.xp.asarray(shape)))
    inits = cast(FloatsXd, numpy.random.normal(scale=mean, size=size).astype("float32"))
    inits = ops.reshape_f(inits, shape)
    return ops.asarray_f(inits)


@registry.initializers("normal_init.v1")
def configure_normal_init(*, mean: float = 0) -> Callable[[FloatsXd], FloatsXd]:
    return partial(normal_init, mean=mean)


__all__ = [
    "normal_init",
    "uniform_init",
    "glorot_uniform_init",
    "zero_init",
    "lecun_uniform_init",
    "he_uniform_init",
    "glorot_normal_init",
    "he_normal_init",
    "lecun_normal_init",
]
