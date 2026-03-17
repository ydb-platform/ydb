from __future__ import annotations

import abc
from datetime import timedelta


class SmoothingAlgorithm(abc.ABC):
    @abc.abstractmethod
    def __init__(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, new_value: float, elapsed: timedelta) -> float:
        """Updates the algorithm with a new value and returns the smoothed
        value.
        """
        raise NotImplementedError


class ExponentialMovingAverage(SmoothingAlgorithm):
    """
    The Exponential Moving Average (EMA) is an exponentially weighted moving
    average that reduces the lag that's typically associated with a simple
    moving average. It's more responsive to recent changes in data.
    """

    def __init__(self, alpha: float = 0.5) -> None:
        self.alpha = alpha
        self.value = 0

    def update(self, new_value: float, elapsed: timedelta) -> float:
        self.value = self.alpha * new_value + (1 - self.alpha) * self.value
        return self.value


class DoubleExponentialMovingAverage(SmoothingAlgorithm):
    """
    The Double Exponential Moving Average (DEMA) is essentially an EMA of an
    EMA, which reduces the lag that's typically associated with a simple EMA.
    It's more responsive to recent changes in data.
    """

    def __init__(self, alpha: float = 0.5) -> None:
        self.alpha = alpha
        self.ema1 = 0
        self.ema2 = 0

    def update(self, new_value: float, elapsed: timedelta) -> float:
        self.ema1 = self.alpha * new_value + (1 - self.alpha) * self.ema1
        self.ema2 = self.alpha * self.ema1 + (1 - self.alpha) * self.ema2
        return 2 * self.ema1 - self.ema2
