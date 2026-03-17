from dataclasses import dataclass
from random import choice
from typing import List, Optional, Tuple

from lzy.api.v1 import op, whiteboard
import toloka.client as toloka

from .classification import Whiteboard as ClassificationWhiteboard
from .. import base, mapping, classification_loop, classification, worker
from ... import pool as pool_config


# In a side-by-side comparison, client has two variants of objects, list of A's and list of B's, and want to know which
# variant, A or B, is better according to his criteria. By default, we place A's to the left, but to avoid workers bias
# of left/right variants of task UI or different exploits, we always randomly swap each pair of variants before creating
# tasks for workers. Overall process looks like:
# - take A's and B's (client input)
# - random swap, create L's and R's for tasks (remember swaps)
# - labeling
# - get results as L's and R's
# - convert L's and R's back to A's and B's (using remembered swaps)
# - return results for each pair in A's and B's terms (client output)
class Loop(classification_loop.ClassificationLoop):
    a_fr: int
    b_fr: int
    b_to: int
    swaps: List[bool] = None

    def __init__(
            self,
            client: toloka.TolokaClient,
            task_mapping: mapping.TaskMapping,
            params: classification_loop.Params,
            lang: str,
            with_control_tasks: bool = True,
            model: Optional[worker.Model] = None,
            task_function: base.SbSFunction = None,
    ):
        super(Loop, self).__init__(client, task_mapping, params, lang, with_control_tasks, model)
        h_cnt = len(task_function.get_hints())
        i_cnt = len(task_function.get_inputs())
        self.a_fr = h_cnt
        self.b_fr = h_cnt + i_cnt
        self.b_to = h_cnt + i_cnt * 2

    def create_pool(
            self,
            control_objects: List[mapping.TaskSingleSolution],
            pool_cfg: pool_config.ClassificationConfig,
    ):
        control_objects_swapped = []
        for control_object in control_objects:
            swap = choice((True, False))
            if swap:
                control_object = self.swap_task_solution(control_object)
            control_objects_swapped.append(control_object)
        return super(Loop, self).create_pool(control_objects_swapped, pool_cfg)

    def add_input_objects(self, pool_id: str, input_objects: List[mapping.Objects]):
        assert self.swaps
        return super(Loop, self).add_input_objects(pool_id, self.swap_input_objects(input_objects))

    def get_results(
            self,
            pool_id: str,
            pool_input_objects: Optional[List[mapping.Objects]],
    ) -> Tuple[classification.Results, Optional[classification.WorkerWeights]]:
        assert self.swaps
        results_swapped, worker_weights = super(Loop, self).get_results(
            pool_id,
            self.swap_input_objects(pool_input_objects),
        )

        results = []
        for (labels_probas, worker_labels), swap in zip(results_swapped, self.swaps):
            assert labels_probas is not None
            if swap:
                labels_probas = {label.swap(): proba for label, proba in labels_probas.items()}
                worker_labels = [(label.swap(), worker) for label, worker in worker_labels]
            results.append((labels_probas, worker_labels))

        return results, worker_weights

    def swap_task(self, task: mapping.Objects) -> mapping.Objects:
        result = list(task)
        a_fr, b_fr, b_to = self.a_fr, self.b_fr, self.b_to
        result[a_fr:b_fr], result[b_fr:b_to] = result[b_fr:b_to], result[a_fr:b_fr]
        return tuple(result)

    def swap_task_solution(self, task_solution: mapping.TaskSingleSolution) -> mapping.TaskSingleSolution:
        task, solution = task_solution
        return self.swap_task(task), (solution[0].swap(),) + solution[1:]

    def swap_input_objects(self, input_objects: List[mapping.Objects]) -> List[mapping.Objects]:
        result = []
        for swap, task_input_objects in zip(self.swaps, input_objects):
            if swap:
                task_input_objects = self.swap_task(task_input_objects)
            result.append(task_input_objects)
        return result

    @op(lazy_arguments=False, cache=True, version='1.0')
    def get_swaps(self, input_objects: List[mapping.Objects]) -> List[bool]:
        return [choice((True, False)) for _ in input_objects]


# WARNING: field name changes are not backward-compatible
@dataclass
@whiteboard(name='sbs')
class Whiteboard(ClassificationWhiteboard):
    swaps: List[bool]
