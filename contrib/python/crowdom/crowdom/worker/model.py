from dataclasses import dataclass, field
import logging
from typing import List, Callable, Dict

import toloka.client as toloka

from ..mapping import Objects, TaskMapping, TaskID
from .common import Worker

logger = logging.getLogger(__name__)


@dataclass
class Model(Worker):
    name: str
    func: Callable[[List[Objects]], List[Objects]]

    @property
    def id(self) -> str:
        return self.name


@dataclass
class ModelWorkspace:
    model: Model
    task_mapping: TaskMapping
    solutions_cache: Dict[TaskID, Objects] = field(init=False, default_factory=dict)

    def get_solutions(
        self,
        pool_input_objects: List[Objects],
        status: toloka.Assignment.Status = toloka.Assignment.SUBMITTED,
    ) -> toloka.Assignment:
        logger.debug(f'model "{self.model.id}" started solving {len(pool_input_objects)} tasks')

        # TODO(DATAFORGE-75)
        # we have no persistent storage for ML worker answers, we have in-memory cache, in case of labeling restart
        # we will re-answer ML worker in same tasks, so we count on model func determinism

        task_id = self.task_mapping.task_id
        not_cached_input_objects = [
            input_objects for input_objects in pool_input_objects if task_id(input_objects) not in self.solutions_cache
        ]
        not_cached_output_objects = self._apply(not_cached_input_objects)

        self.solutions_cache.update(
            {
                task_id(input_objects): output_objects
                for input_objects, output_objects in zip(not_cached_input_objects, not_cached_output_objects)
            }
        )

        pool_output_objects = [self.solutions_cache[task_id(input_objects)] for input_objects in pool_input_objects]

        logger.debug(
            f'model completed tasks solution '
            f'({len(pool_input_objects) - len(not_cached_input_objects)}/{len(pool_input_objects)} cached solutions)'
        )

        return toloka.Assignment(
            id='',
            tasks=[self.task_mapping.to_task(input_objects) for input_objects in pool_input_objects],
            solutions=[self.task_mapping.to_solution(output_objects) for output_objects in pool_output_objects],
            user_id=self.model.id,
            status=status,
        )

    def _apply(self, pool_input_objects: List[Objects]) -> List[Objects]:
        pool_output_objects = self.model.func(pool_input_objects)

        err_msg_prefix = 'invalid model func:'
        assert len(pool_input_objects) == len(
            pool_output_objects
        ), f'{err_msg_prefix} output list length is not equal to input'
        for output_objects in pool_output_objects:
            assert isinstance(output_objects, tuple), f'{err_msg_prefix} output objects are not tuple'
            assert len(output_objects) == len(
                self.task_mapping.output_mapping
            ), f'{err_msg_prefix} output objects count do not match task function'
            for obj, mapping in zip(output_objects, self.task_mapping.output_mapping):
                assert (
                    type(obj) == mapping.obj_type
                ), f'{err_msg_prefix} object type mismatch, expected {mapping.obj_type}, actual {type(obj)}'

        return pool_output_objects
