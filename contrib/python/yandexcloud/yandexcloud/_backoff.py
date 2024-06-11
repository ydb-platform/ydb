import random
from typing import Callable


def backoff_linear_with_jitter(wait_time: float, jitter: float) -> Callable[[int], float]:
    def func(attempt: int) -> float:  # pylint: disable=unused-argument
        multiplier = jitter * (random.random() * 2 - 1)
        return wait_time * (1 + multiplier)

    return func


def backoff_exponential_with_jitter(base: float, cap: float) -> Callable[[int], float]:
    def func(attempt: int) -> float:  # pylint: disable=unused-argument
        try:
            res = (2**attempt) * base * random.random()
        except OverflowError:
            return cap

        if res > cap:
            return cap

        return res

    return func


def backoff_exponential_jittered_min_interval(base: float = 0.05, cap: float = 60) -> Callable[[int], float]:
    def func(attempt: int) -> float:  # pylint: disable=unused-argument
        try:
            base_interval = (2**attempt) * base
            res = base_interval / 2 + base_interval * random.random()
        except OverflowError:
            return cap

        if res > cap:
            return cap

        return res

    return func


def default_backoff() -> Callable[[int], float]:
    return backoff_exponential_with_jitter(0.05, 60)
