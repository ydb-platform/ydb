from collections import UserDict
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
import logging
from math import floor, ceil
import random
import threading
from typing import List, Dict, Any, Tuple, Optional, Union

import numpy as np
import toloka.client as toloka

from crowdom import base, mapping, objects, task_spec as spec

logger = logging.getLogger(__name__)


class StoppableThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(StoppableThread, self).__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):
        delay = np.random.randint(0, 60)
        self._stop_event.wait(timeout=delay)
        while not self.stopped():
            no_tasks = self._target(*self._args, **self._kwargs)
            if no_tasks:
                self._stop_event.wait(timeout=10 + np.random.randint(0, 10))


EPS = 1e-6


# probs work badly with edge values
def clip(x: float, eps: float = EPS) -> float:
    return np.clip(x, eps, 1 - eps)


def get_task_id(task_spec: spec.PreparedTaskSpec, row: Dict[str, Any]) -> str:
    task_mapping = task_spec.task_mapping
    return task_mapping.task_id(task_mapping.from_task_values(row)).id


class WorkerSpeed(Enum):
    RAND = 'rand'
    POOR = 'poor'
    FAST = 'fast'
    MEDIUM = 'medium'
    SLOW = 'slow'

    def get_bounds(self) -> Tuple[float, float]:
        return {
            WorkerSpeed.RAND: (0.01, 0.05),
            WorkerSpeed.POOR: (0.2, 0.25),
            WorkerSpeed.FAST: (0.5, 0.8),
            WorkerSpeed.MEDIUM: (0.9, 1.1),
            WorkerSpeed.SLOW: (1.2, 1.5),
        }[self]

    # todo - consider tasks count, not only duration hint
    def get_timeout(self, task_duration_hint: timedelta):
        left, right = self.get_bounds()
        duration = int(task_duration_hint.total_seconds())
        return np.random.randint(floor(left * duration), ceil(right * duration))


@dataclass
class TaskInfo:
    solution: base.Object
    complexity: Optional[float] = 0.0


class AnnotationTaskInfo(TaskInfo):
    solution: mapping.Objects


class AnnotationSolutionsDict(UserDict):
    markup_task_mapping: mapping.TaskMapping
    check_task_mapping: mapping.TaskMapping

    # (task, true solution, [given_solution])
    def __init__(self, data, markup_task_mapping: mapping.TaskMapping, check_task_mapping: mapping.TaskMapping):
        super(AnnotationSolutionsDict, self).__init__(data)
        self.markup_task_mapping = markup_task_mapping
        self.check_task_mapping = check_task_mapping

    def __getitem__(
        self,
        task_id: mapping.TaskID,
    ) -> AnnotationTaskInfo:

        if len(task_id.objects) == len(self.check_task_mapping.input_mapping):
            objects = task_id.objects[: len(self.markup_task_mapping.input_mapping)]
            key = mapping.TaskID(objects)
            given_solution = task_id.objects[len(self.markup_task_mapping.input_mapping) :]
        else:
            given_solution = None
            key = task_id
        info = self.data[key]

        return AnnotationTaskInfo(solution=(key.objects, info.solution, given_solution), complexity=info.complexity)


def get_random_classification_option(
    task_mapping: mapping.TaskMapping,
    forbidden_solution: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    assert len(task_mapping.output_mapping) == 1
    label_class = task_mapping.output_mapping[0].obj_meta.type
    assert issubclass(label_class, base.Class)

    options = []

    # todo we can have confusion matrix here
    for possible_result in label_class.possible_instances():
        option = task_mapping.toloka_values((possible_result,), output=True)
        if forbidden_solution is None or option != forbidden_solution:
            options.append(option)

    return np.random.choice(options)


class TaskComplexity(Enum):
    EASY = 0.05
    OK = 0.5
    HARD = 5


def swap_task(task: mapping.Objects, task_spec: spec.PreparedTaskSpec) -> mapping.Objects:
    function = task_spec.function
    assert isinstance(function, base.SbSFunction)
    assert len(task_spec.task_mapping.output_mapping) == 1
    hint_count = len(function.hints)
    hints = task[:hint_count]
    input_objects = task[hint_count:]
    left_objects, right_objects = (input_objects[: len(input_objects) // 2], input_objects[len(input_objects) // 2 :])
    return hints + right_objects + left_objects


# todo we are working with classification functions only
def create_tasks_info_dict(
    task_spec: spec.PreparedTaskSpec,
    solved_tasks: List[mapping.TaskSingleSolution],
    tasks_complexity: Optional[Union[Dict[TaskComplexity, int], TaskComplexity]] = None,
) -> Dict[mapping.TaskID, TaskInfo]:

    if tasks_complexity is None:
        tasks_complexity = {TaskComplexity.EASY: len(solved_tasks)}
    elif isinstance(tasks_complexity, TaskComplexity):
        tasks_complexity = {tasks_complexity: len(solved_tasks)}
    else:
        assert isinstance(tasks_complexity, dict)

    assert sum(tasks_complexity.values()) == len(solved_tasks)
    complexities = [c for c, v in tasks_complexity.items() for _ in range(v)]
    solutions = {}

    if isinstance(task_spec.function, base.ClassificationFunction) or isinstance(task_spec.function, base.SbSFunction):
        for (task, solution), complexity in zip(solved_tasks, complexities):
            assert len(solution) == 1
            choice = solution[0]

            solutions[mapping.TaskID(task)] = TaskInfo(choice, complexity.value)
            if isinstance(task_spec.function, base.SbSFunction):
                solutions[mapping.TaskID(swap_task(task, task_spec))] = TaskInfo(choice.swap(), complexity.value)
        return solutions

    assert isinstance(task_spec, spec.AnnotationTaskSpec)

    for (task, solution), complexity in zip(solved_tasks, complexities):
        solutions[mapping.TaskID(task)] = AnnotationTaskInfo(solution, complexity.value)

    return AnnotationSolutionsDict(solutions, task_spec.task_mapping, task_spec.check.task_mapping)


class ThreadWithReturnValue(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(ThreadWithReturnValue, self).__init__(*args, **kwargs)
        self._return_value: Any = None

    def run(self):
        self._return_value = self._target(*self._args, **self._kwargs)

    def join(self, timeout: Optional[float] = None) -> Any:
        super(ThreadWithReturnValue, self).join(timeout)
        return self._return_value


def get_accuracy(assignment: toloka.Assignment) -> float:
    ok_count, total_count = 0, 0
    for task, solution in zip(assignment.tasks, assignment.solutions):
        if task.known_solutions:
            total_count += 1
            ok_count += int(solution.output_values == task.known_solutions[0].output_values)
    return ok_count / total_count


def content(object: objects.Object) -> str:
    if isinstance(object, objects.Text):
        return object.text
    elif isinstance(object, objects.Audio):
        return object.url
    raise ValueError('Unknown object type')


# just as an example
def mutate(gt: mapping.Objects) -> mapping.Objects:
    result = []
    for obj in gt:
        if isinstance(obj, objects.Text):
            result.append(mutate_text(obj))
        else:
            raise NotImplementedError(f'{gt}')
    return tuple(result)


# todo: maybe reuse Mutator here
def mutate_text(gt: objects.Text) -> objects.Text:
    letters = list(gt.text)
    random.shuffle(letters)
    return objects.Text(''.join(letters))
