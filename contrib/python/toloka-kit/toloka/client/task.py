__all__ = [
    'BaseTask',
    'Task',
    'CreateTaskParameters',
    'CreateTasksParameters',
    'TaskOverlapPatch',
    'TaskPatch'
]
import datetime
from typing import Any, Dict, List

from .primitives.base import BaseTolokaObject
from .primitives.infinite_overlap import InfiniteOverlapParametersMixin
from .primitives.parameter import IdempotentOperationParameters
from ..util._codegen import attribute
from ..util._docstrings import inherit_docstrings


class BaseTask(BaseTolokaObject):
    """A base class for tasks.

    Attributes:
        input_values: A dictionary with input data for a task. Input field names are keys in the dictionary.
        known_solutions: A list of all responses considered correct. It is used with control and training tasks.
                        If there are several output fields, then you must specify all their correct combinations.
        message_on_unknown_solution: A hint used in training tasks.
        id: The ID of a task.
        origin_task_id: The ID of a parent task. This parameter is set if the task was created by copying.
    """

    class KnownSolution(BaseTolokaObject):
        """A correct response for a control or training task.

        Responses have a correctness weight.
        For example, if `correctness_weight` is 0.5,
        then half of the error is counted to the Toloker.

        Attributes:
            output_values: Correct values of output fields.
            correctness_weight: The correctness weight of the response.
        """

        output_values: Dict[str, Any]
        correctness_weight: float

    input_values: Dict[str, Any]
    known_solutions: List[KnownSolution]
    message_on_unknown_solution: str

    # Readonly
    id: str = attribute(readonly=True)
    origin_task_id: str = attribute(readonly=True)


@inherit_docstrings
class Task(InfiniteOverlapParametersMixin, BaseTask):
    """A task that is assigned to Tolokers.

    Tasks are grouped into [TaskSuites](toloka.client.task_suite.TaskSuite.md).

    Attributes:
        pool_id: The ID of the pool that the task belongs to.
        reserved_for: IDs of Tolokers who have access to the task.
        unavailable_for: IDs of Tolokers who don't have access to the task.
        traits_all_of: The task can be assigned to Tolokers who have all of the specified traits.
        traits_any_of: The task can be assigned to Tolokers who have any of the specified traits.
        traits_none_of_any: The task can not be assigned to Tolokers who have any of the specified traits.
        created: The UTC date and time when the task was created.
        baseline_solutions: Preliminary responses for dynamic overlap and aggregation of results by skill. They are used to calculate a confidence level of the first responses from Toloker.
        remaining_overlap: The number of times left for this task to be assigned to Tolokers. Read-only field.

    Example:
        Creating a simple task with one input field.

        >>> task = toloka.client.Task(
        >>>     input_values={'image': 'https://some.url/img0.png'},
        >>>     pool_id='1086170'
        >>> )
        ...

        See more complex example in the description of the [create_tasks](toloka.client.TolokaClient.create_tasks.md) method.
    """

    class BaselineSolution(BaseTolokaObject):
        """ A preliminary response.
        """

        output_values: Dict[str, Any]
        confidence_weight: float

    pool_id: str

    remaining_overlap: int = attribute(readonly=True)
    reserved_for: List[str]
    unavailable_for: List[str]
    traits_all_of: List[str]
    traits_any_of: List[str]
    traits_none_of_any: List[str]
    origin_task_id: str = attribute(readonly=True)
    created: datetime.datetime = attribute(readonly=True)
    baseline_solutions: List[BaselineSolution]


@inherit_docstrings
class CreateTaskParameters(IdempotentOperationParameters):
    """Parameters used with the [create_task](toloka.client.TolokaClient.create_task.md) method.

    Attributes:
        allow_defaults: Active overlap setting:
            * `True` — Use the overlap that is set in the `defaults.default_overlap_for_new_tasks` pool parameter.
            * `False` — Use the overlap that is set in the `overlap` task parameter.

            Default value: `False`.
        open_pool: Open the pool immediately after creating a task suite, if the pool is closed.
            Default value: `False`.
    """
    allow_defaults: bool
    open_pool: bool


@inherit_docstrings
class CreateTasksParameters(CreateTaskParameters):
    """Parameters used with the [create_tasks](toloka.client.TolokaClient.create_tasks.md)
    and [create_tasks_async](toloka.client.TolokaClient.create_tasks_async.md) methods.

    Attributes:
        skip_invalid_items: Task validation option:
            * `True` — All valid tasks are added. If a task does not pass validation, then it is not added to Toloka. All such tasks are listed in the response.
            * `False` — If any task does not pass validation, then the operation is cancelled and no tasks are added to Toloka.

            Default value: `False`.
    """
    skip_invalid_items: bool


class TaskOverlapPatch(BaseTolokaObject):
    """Parameters for changing the overlap of a task.

    Attributes:
        overlap: The new overlap value.
        infinite_overlap:
            * `True` — The task is assigned to all Tolokers. It is usually set for training and control tasks.
            * `False` — An overlap value specified for the task or for the pool is used.

            Default value: `False`.
    """

    overlap: int
    infinite_overlap: bool


@inherit_docstrings
class TaskPatch(TaskOverlapPatch):
    """Parameters for changing a task.

    Attributes:
        baseline_solutions: Preliminary responses for dynamic overlap and aggregation of results by a skill. They are used to calculate a confidence level of the first responses from Tolokers.
        known_solutions: A list of all responses considered correct. It is used with control and training tasks.
            If there are several output fields, then you must specify all their correct combinations.
        message_on_unknown_solution: A hint used in training tasks.
    """

    baseline_solutions: List[Task.BaselineSolution]
    known_solutions: List[Task.KnownSolution]
    message_on_unknown_solution: str
