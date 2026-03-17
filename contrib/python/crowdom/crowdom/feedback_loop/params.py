from dataclasses import dataclass
from typing import Optional

from ..classification import AggregationAlgorithm
from ..classification_loop import Overlap, DynamicOverlap
from ..control import Control
from ..evaluation import AssignmentCheckSample
from ..duration import TaskDurationFunction


@dataclass
class Evaluation:
    aggregation_algorithm: AggregationAlgorithm
    assignment_check_sample: Optional[AssignmentCheckSample]
    overlap: Overlap

    def __post_init__(self):
        check_sample = self.assignment_check_sample
        if check_sample is not None and check_sample.max_tasks_to_check is not None:
            assert check_sample.max_tasks_to_check > 0


@dataclass
class Params:
    assignment_check_sample: Optional[AssignmentCheckSample]
    control: Control
    overlap: DynamicOverlap
    task_duration_function: TaskDurationFunction
