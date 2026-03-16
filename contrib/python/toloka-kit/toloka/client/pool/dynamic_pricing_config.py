__all__ = ['DynamicPricingConfig']
from enum import unique
from typing import List

from ..primitives.base import BaseTolokaObject
from ...util._codegen import attribute
from ...util._extendable_enum import ExtendableStrEnum


class DynamicPricingConfig(BaseTolokaObject, kw_only=False):
    """The dynamic pricing settings.

    A price per task suite can be variable depending on a Toloker's skill.
    If a Toloker is not covered by dynamic pricing settings then the default price is used. It is set in the `reward_per_assignment` pool parameter.

    Attributes:
        type: The dynamic pricing type. Only `SKILL` type is supported now.
        skill_id: The ID of the skill that dynamic pricing is based on.
        intervals: A list of skill intervals and prices.
            The intervals must not overlap.
    """

    @unique
    class Type(ExtendableStrEnum):
        """Dynamic pricing type."""
        SKILL = 'SKILL'

    class Interval(BaseTolokaObject):
        """Skill level interval with the associated price per task suite.

        The lower and upper skill bounds are included in the interval.

        Attributes:
            from_: The lower bound of the interval.
            to: The upper bound of the interval.
            reward_per_assignment: The price per task suite for a Toloker with the specified skill level.
        """

        from_: int = attribute(origin='from')
        to: int
        reward_per_assignment: float

    type: Type = attribute(autocast=True)
    skill_id: str
    intervals: List[Interval]
