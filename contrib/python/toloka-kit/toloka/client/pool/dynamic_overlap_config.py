__all__ = ['DynamicOverlapConfig']
from enum import unique
from typing import List

from ..primitives.base import BaseTolokaObject
from ...util._codegen import attribute
from ...util._extendable_enum import ExtendableStrEnum


class DynamicOverlapConfig(BaseTolokaObject):
    """Dynamic overlap settings.

    Toloka can automatically increase an overlap of tasks if the confidence level of aggregated responses is not high enough.

    Dynamic overlap uses the `BASIC` algorithm.
    Each response is assigned a weight depending on the Toloker's skill value.
    The aggregated response confidence is calculated based on the probability algorithm.
    The task overlap increases until it reaches `max_overlap` or until the confidence of the aggregated response exceeds `min_confidence`.

    Note, that if you use dynamic overlap, then set the `auto_close_after_complete_delay_seconds` pool parameter to a non zero value.

    Learn more about the [Dynamic overlap](https://toloka.ai/docs/guide/dynamic-overlap) in the guide.

    Attributes:
        type: The dynamic overlap algorithm.
        max_overlap: Maximum overlap. The value must be higher than the default overlap value. Allowed range: from 1 to 30,000.
        min_confidence: Minimum required confidence of the aggregated response. Allowed range: from 0 to 1.
        answer_weight_skill_id: A skill that determines the weight of the Toloker's responses.
            For the best results, use a skill calculated as a percentage of correct responses in control tasks.
        fields: A list of output data fields used for aggregating responses.
            For the best results, each field must have a limited number of response options.
            Don't specify fields in the list together if they depend on each other.
    """

    @unique
    class Type(ExtendableStrEnum):
        """The algorithm for dynamic overlap.

        Attributes:
            BASIC: The algorithm based on a Toloker's skill value.
        """

        BASIC = 'BASIC'

    class Field(BaseTolokaObject, kw_only=False):
        """An output data field used for aggregating responses.

        Attributes:
            name: The name of the output field.
        """

        name: str

    type: Type = attribute(autocast=True)
    max_overlap: int
    min_confidence: float
    answer_weight_skill_id: str
    fields: List[Field]
