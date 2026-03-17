"""Generators that provide different rates, schedules, decays or series."""
from typing import Iterable

import numpy

from .config import registry


@registry.schedules("constant_then.v1")
def constant_then(
    rate: float, steps: int, schedule: Iterable[float]
) -> Iterable[float]:
    """Yield a constant rate for N steps, before starting a schedule."""
    for i in range(steps):
        yield rate
    for value in schedule:
        yield value


@registry.schedules("constant.v1")
def constant(rate: float) -> Iterable[float]:
    """Yield a constant rate."""
    while True:
        yield rate


@registry.schedules("decaying.v1")
def decaying(base_rate: float, decay: float, *, t: int = 0) -> Iterable[float]:
    """Yield an infinite series of linearly decaying values,
    following the schedule: base_rate * 1 / (1 + decay * t)

    EXAMPLE:
        >>> learn_rates = decaying(0.001, 1e-4)
        >>> next(learn_rates)
        0.001
        >>> next(learn_rates)
        0.00999
    """
    while True:
        yield base_rate * (1.0 / (1.0 + decay * t))
        t += 1


@registry.schedules("compounding.v1")
def compounding(
    start: float, stop: float, compound: float, *, t: float = 0.0
) -> Iterable[float]:
    """Yield an infinite series of compounding values. Each time the
    generator is called, a value is produced by multiplying the previous
    value by the compound rate.

    EXAMPLE:
        >>> sizes = compounding(1.0, 10.0, 1.5)
        >>> assert next(sizes) == 1.
        >>> assert next(sizes) == 1 * 1.5
        >>> assert next(sizes) == 1.5 * 1.5
    """
    curr = float(start)
    while True:
        yield _clip(curr, start, stop)
        curr *= compound


def _clip(value: float, start: float, stop: float) -> float:
    return max(value, stop) if (start > stop) else min(value, stop)


@registry.schedules("slanted_triangular.v1")
def slanted_triangular(
    max_rate: float,
    num_steps: int,
    *,
    cut_frac: float = 0.1,
    ratio: int = 32,
    decay: float = 1.0,
    t: float = 0.0,
) -> Iterable[float]:
    """Yield an infinite series of values according to Howard and Ruder's
    "slanted triangular learning rate" schedule.
    """
    cut = int(num_steps * cut_frac)
    while True:
        t += 1
        if t < cut:
            p = t / cut
        else:
            p = 1 - ((t - cut) / (cut * (1 / cut_frac - 1)))
        learn_rate = max_rate * (1 + p * (ratio - 1)) * (1 / ratio)
        yield learn_rate


@registry.schedules("warmup_linear.v1")
def warmup_linear(
    initial_rate: float, warmup_steps: int, total_steps: int
) -> Iterable[float]:
    """Generate a series, starting from an initial rate, and then with a warmup
    period, and then a linear decline. Used for learning rates.
    """
    step = 0
    while True:
        if step < warmup_steps:
            factor = step / max(1, warmup_steps)
        else:
            factor = max(
                0.0, (total_steps - step) / max(1.0, total_steps - warmup_steps)
            )
        yield factor * initial_rate
        step += 1


@registry.schedules("cyclic_triangular.v1")
def cyclic_triangular(min_lr: float, max_lr: float, period: int) -> Iterable[float]:
    it = 1
    while True:
        # https://towardsdatascience.com/adaptive-and-cyclical-learning-rates-using-pytorch-2bf904d18dee
        cycle = numpy.floor(1 + it / (2 * period))
        x = numpy.abs(it / period - 2 * cycle + 1)
        relative = max(0, 1 - x)
        yield min_lr + (max_lr - min_lr) * relative
        it += 1


__all__ = [
    "cyclic_triangular",
    "warmup_linear",
    "constant",
    "constant_then",
    "decaying",
    "warmup_linear",
    "slanted_triangular",
    "compounding",
]
