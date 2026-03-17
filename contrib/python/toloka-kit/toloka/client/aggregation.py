"""Module for aggregating results

This module has methods to aggregate answers and to estimate confidence in aggregated labels.
Use it when each task is assigned to several Tolokers.  Note, that aggregation runs on the Toloka server.

If you need advanced aggregation methods or want to run aggregation algorithms locally on your computer,
try [crowd-kit library](https://toloka.ai/docs/crowd-kit).
"""

__all__ = [
    'AggregatedSolutionType',
    'PoolAggregatedSolutionRequest',
    'TaskAggregatedSolutionRequest',
    'WeightedDynamicOverlapTaskAggregatedSolutionRequest',
    'AggregatedSolution'
]
from enum import unique
from typing import Any, Dict, List

from .primitives.base import BaseTolokaObject
from ..util._codegen import attribute
from ..util._docstrings import inherit_docstrings
from ..util._extendable_enum import ExtendableStrEnum


@unique
class AggregatedSolutionType(ExtendableStrEnum):
    WEIGHTED_DYNAMIC_OVERLAP = 'WEIGHTED_DYNAMIC_OVERLAP'
    DAWID_SKENE = 'DAWID_SKENE'


class PoolAggregatedSolutionRequest(BaseTolokaObject):
    """Parameters for aggregating results in a pool using the [aggregate_solutions_by_pool](toloka.client.TolokaClient.aggregate_solutions_by_pool.md) method.

    Attributes:
        type: Aggregation model:
            * `WEIGHTED_DYNAMIC_OVERLAP` — [Aggregation](https://toloka.ai/docs/guide/result-aggregation#aggr-by-skill) based on Tolokers' skill in a pool with a dynamic overlap.
            * `DAWID_SKENE` — [Dawid-Skene aggregation model](https://toloka.ai/docs/guide/result-aggregation#dawid-skene). It is used in pools without a dynamic overlap.
        pool_id: The ID of the pool.
        answer_weight_skill_id: The ID of the skill that determines the weight of the Toloker's responses.
        fields: Output data fields to aggregate. For the best results, each of these fields should have limited number of response options.
            If the `DAWID_SKENE` aggregation type is selected, you can only specify one value.

    Example:
        >>> aggregation_operation = toloka_client.aggregate_solutions_by_pool(
        >>>     type=toloka.client.aggregation.AggregatedSolutionType.WEIGHTED_DYNAMIC_OVERLAP,
        >>>     pool_id='1080020',
        >>>     answer_weight_skill_id='11294',
        >>>     fields=[toloka.client.aggregation.PoolAggregatedSolutionRequest.Field(name='result')]
        >>> )
        >>> aggregation_operation = toloka_client.wait_operation(aggregation_operation)
        >>> aggregation_results = list(toloka_client.get_aggregated_solutions(aggregation_operation.id))
        ...
    """

    class Field(BaseTolokaObject):
        name: str

    type: AggregatedSolutionType = attribute(autocast=True)
    pool_id: str
    answer_weight_skill_id: str
    fields: List[Field]


class TaskAggregatedSolutionRequest(BaseTolokaObject, spec_field='type', spec_enum=AggregatedSolutionType):
    """Base class with parameters to run aggregation for a single task.

    Attributes:
        task_id: The ID of the task.
        pool_id: The ID of the pool containing the task.
    """

    task_id: str
    pool_id: str


@inherit_docstrings
class WeightedDynamicOverlapTaskAggregatedSolutionRequest(
    TaskAggregatedSolutionRequest,
    spec_value=AggregatedSolutionType.WEIGHTED_DYNAMIC_OVERLAP
):
    """Parameters to run weighted aggregation for a single task with a dynamic overlap.

    Attributes:
        answer_weight_skill_id: The ID of the skill that determines the weight of the Toloker's responses.
        fields: Output data fields to aggregate. For the best results, each of these fields should have limited number of response options.
            If the `DAWID_SKENE` aggregation type is selected, you can only specify one value.

    Example:
        >>> aggregated = toloka_client.aggregate_solutions_by_task(
        >>>     pool_id='1238218',
        >>>     task_id='000012e4ca--62e97a75dbab805456309d81',
        >>>     answer_weight_skill_id='12648',
        >>>     fields=[toloka.client.aggregation.PoolAggregatedSolutionRequest.Field(name='result')]
        >>> )
        >>> print(aggregated.output_values['result'])
        ...
    """

    class Field(BaseTolokaObject):
        name: str

    answer_weight_skill_id: str
    fields: List[Field]


class AggregatedSolution(BaseTolokaObject):
    """An aggregated response to a task.

    Attributes:
        pool_id: The ID of the pool containing the task.
        task_id: The ID of the task.
        confidence: The confidence level for the aggregated response.
        output_values: Output data fields with aggregated responses.

    Example:
        >>> aggregated = toloka_client.aggregate_solutions_by_task(
        >>>     pool_id='1238218',
        >>>     task_id='000012e4ca--62e97a75dbab805456309d81',
        >>>     answer_weight_skill_id='12648',
        >>>     fields=[toloka.client.aggregation.PoolAggregatedSolutionRequest.Field(name='result')]
        >>> )
        >>> print(aggregated.output_values['result'], aggregated.confidence)
        ...
    """

    pool_id: str
    task_id: str
    confidence: float
    output_values: Dict[str, Any]
