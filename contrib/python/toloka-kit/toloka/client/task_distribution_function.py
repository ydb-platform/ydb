__all__ = ['TaskDistributionFunction']
from enum import unique
from typing import List

from .primitives.base import BaseTolokaObject
from ..util._codegen import attribute
from ..util._extendable_enum import ExtendableStrEnum


class TaskDistributionFunction(BaseTolokaObject):
    """A configuration of selecting tasks.

    It is used:
    - To control the selection of tasks for the selective majority vote checks.
    - To change the frequency of assigning control or training tasks.

    Attributes:
        scope: A way of counting tasks completed by a Toloker:
            * `POOL` — Completed pool tasks are counted.
            * `PROJECT` — All completed project tasks are counted.
        distribution: The distribution of selected tasks within an interval.
            Allowed values: `UNIFORM`.
        window_days: The number of days in which completed tasks are counted.
            Allowed values: from 1 to 365.
        intervals: A list of count intervals with frequency values.
            The maximum number of list items is 10,000.
    """

    @unique
    class Scope(ExtendableStrEnum):
        PROJECT = 'PROJECT'
        POOL = 'POOL'

    @unique
    class Distribution(ExtendableStrEnum):
        UNIFORM = 'UNIFORM'

    class Interval(BaseTolokaObject):
        """A count interval with associated frequency value.

        If the number of tasks is in the interval then the task distribution uses the interval frequency.

        The value of the `frequency` parameter encodes a period in a task sequence.
        For example, if `frequency` is 3, then the 1st, 4th, 7th tasks are selected. And so on.

        Attributes:
            from_: The lower bound of the interval.
                Allowed values: up to 1,000,000.
            to: The upper bound of the interval.
                Allowed values: up to 1,000,000.
            frequency: The frequency of tasks within an interval.
                Allowed values: from 1 to 10,000,000.
        """

        from_: int = attribute(origin='from')
        to: int
        frequency: int

    scope: Scope = attribute(autocast=True)
    distribution: Distribution = attribute(autocast=True)
    window_days: int
    intervals: List[Interval]
