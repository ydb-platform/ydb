from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
from multiprocessing.pool import ThreadPool
from random import shuffle
from time import sleep
from typing import List, Dict, Type, Optional, Tuple, Any, Callable

import numpy as np
from toloka.client.exceptions import IncorrectActionsApiError, DoesNotExistApiError

from .client import NDATolokaClient, NDARegisterClient
from .util import WorkerSpeed, TaskInfo, StoppableThread, content, swap_task, mutate

from crowdom import base, mapping, task_spec as spec

logger = logging.getLogger(__name__)

true, false = base.BinaryEvaluation(True), base.BinaryEvaluation(False)


def softmax(x: np.array) -> np.array:
    e = np.exp(x)
    s = np.sum(e)
    return e / s


@dataclass
class WorkerConfig:
    speed: WorkerSpeed
    quality: float

    obj_cls: Type[base.Class]
    confusion_matrix: Dict[base.Class, Dict[base.Class, float]] = field(init=False)
    random_vector: Dict[base.Class, float] = field(init=False)

    def __post_init__(self):
        self.confusion_matrix = defaultdict(dict)
        options = self.obj_cls.possible_instances()
        remainder = (1.0 - self.quality) / (len(options) - 1)
        for truth in options:
            for prediction in options:
                self.confusion_matrix[truth][prediction] = self.quality if truth == prediction else remainder

        self.random_vector = {option: 1 / len(options) for option in options}

    def sample(self, gt: Optional[base.Object] = None, temperature: float = 0.5) -> mapping.Objects:
        values = self.confusion_matrix[gt].values() if gt else self.random_vector.values()
        probs = softmax(np.array(list(values)) / temperature)
        return (self.choice(probs),)

    def choice(self, probs: List[float]) -> base.Object:
        return np.random.choice(list(self.confusion_matrix.keys()), p=probs)


@dataclass
class SbSWorkerConfig(WorkerConfig):
    left_bias: float
    treat_bias_as_original_a: bool = False
    obj_cls: Type[base.SbSChoice] = field(init=False, default=base.SbSChoice)

    def __post_init__(self):
        self.confusion_matrix = defaultdict(dict)
        options = self.obj_cls.possible_instances()
        assert len(options) == 2
        a, b = options
        assert a.value == 'a'
        assert b.value == 'b'

        self.confusion_matrix[a][a] = self.quality + (1.0 - self.quality) * self.left_bias
        self.confusion_matrix[a][b] = (1.0 - self.quality) * (1.0 - self.left_bias)

        self.confusion_matrix[b][a] = (1.0 - self.quality) * self.left_bias
        self.confusion_matrix[b][b] = self.quality + (1.0 - self.quality) * (1.0 - self.left_bias)

        self.random_vector = {a: self.left_bias, b: 1.0 - self.left_bias}


@dataclass
class AnnotationWorkerConfig(WorkerConfig):
    obj_cls: Type[base.Class] = base.BinaryEvaluation

    def sample(
        self,
        gt: Optional[Tuple[mapping.Objects, mapping.Objects, Optional[mapping.Objects]]] = None,
        temperature: float = 0.5,
    ) -> mapping.Objects:
        assert gt, 'No ground truth supplied for annotation - impossible to create answer'
        values = self.confusion_matrix[true].values()
        probs = softmax(np.array(list(values)) / temperature)
        ok = self.choice(probs) == true
        task, solution, _ = gt

        if ok:
            return solution

        return mutate(solution)


@dataclass
class AnnotationCheckWorkerConfig(AnnotationWorkerConfig):
    def sample(
        self,
        gt: Optional[Tuple[mapping.Objects, mapping.Objects, Optional[mapping.Objects]]] = None,
        temperature: float = 0.5,
    ) -> mapping.Objects:
        if gt:
            task, solution, given_solution = gt
            answer = base.BinaryEvaluation(solution == given_solution)

            values = self.confusion_matrix[answer].values()
        else:
            values = self.random_vector.values()
        probs = softmax(np.array(list(values)) / temperature)
        return (self.choice(probs),)


@dataclass(init=False)
class Worker:
    client: NDATolokaClient

    config: WorkerConfig

    worker_info: Dict[str, Any]

    # we may want to know how he performs both on annotations and evaluations (in case of annotation func)
    pool_id: str

    task_spec: spec.PreparedTaskSpec

    solutions: Optional[Dict[mapping.TaskID, TaskInfo]] = field(default_factory=dict)

    time_policy: Callable = None

    _thread: Optional[StoppableThread] = field(init=False)

    def __init__(
        self,
        worker_info: Dict[str, Any],
        config: WorkerConfig,
        pool_id: str,
        task_spec: spec.PreparedTaskSpec,
        task_duration_hint: timedelta,
        solutions: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        # todo may be make just __post_init__
        self.client = NDATolokaClient(worker_info['oauth'])
        worker_info['id'] = self.client.resolve_id(worker_info['uid'])['id']
        self.worker_info = worker_info
        self.config = config
        self.pool_id = pool_id
        self.task_spec = task_spec

        self.solutions = solutions
        self.time_policy = lambda task_count: self.config.speed.get_timeout(task_duration_hint * task_count)
        self._thread = None

    @property
    def id(self) -> str:
        return self.worker_info['id']

    @property
    def quality(self) -> float:
        return self.config.quality

    def solve_task(self, task: Dict[str, Any], task_id: mapping.TaskID) -> mapping.Objects:
        if self.solutions is not None:
            info = self.solutions[task_id]
            gt = info.solution
            solution = self.config.sample(gt=gt, temperature=info.complexity)
            logger.debug(f'Created {solution} from gt {gt}')
        else:
            solution = self.config.sample()
            logger.debug(f'Created {solution} from scratch')

        return solution

    def get_target(self) -> Callable:

        # only one loop iteration,
        # loop functionality encased in StoppableThread
        def work() -> bool:

            try:
                assignment = self.client.request_assignment(pool_id=self.pool_id)
                start_time = datetime.now()
                logger.debug(f'Got assignment {assignment} for worker #{id(self)}')
            # not sure what to do in this case
            except IncorrectActionsApiError as e:
                # out of tasks, but maybe they will be uploaded at next loop iteration
                logger.debug(f'Out of tasks for worker #{id(self)}, got {e}')
                return True
            except DoesNotExistApiError as e:
                # out of control tasks for this user, no more work possible
                logger.debug(f'Out of control tasks for worker #{id(self)}, got {e}')
                return True
            except Exception as e:
                logger.debug(f'Got unexpected exception: {e} in worker #{id(self)}')
                return True
            solutions = []
            for task in assignment['tasks']:
                task_id = mapping.TaskID(self.task_spec.task_mapping.from_task_values(task['input_values']))
                logger.debug(f'Got task_id {task_id} for task {task}')

                solutions.append(self.task_spec.task_mapping.toloka_values(self.solve_task(task, task_id), output=True))

            # right bound not included; maybe consider time spent on task solution, if it's big
            timeout = self.time_policy(task_count=len(assignment['tasks']))

            # maybe create event for thread outside and sleep on it here to quit more quickly
            sleep(timeout)

            # TODO: wrap in retry in case - if service is unavailable, than things will break and assignment will hang
            #   wait at least until expiration
            result = self.client.submit_assignment(assignment['id'], solutions)
            logger.debug(f'Submitted {solutions} after {datetime.now() - start_time}')
            assert result['success'], 'Could not submit assignment'
            return False

        return work

    def start(self):
        assert self._thread is None, 'Not configured'
        self._thread = StoppableThread(target=self.get_target())
        self._thread.start()

    def stop(self):
        self._thread.stop()
        self._thread.join()
        self._thread = None


@dataclass(init=False)
class SbSWorker(Worker):
    def __init__(
        self,
        worker_info: Dict[str, Any],
        config: SbSWorkerConfig,
        pool_id: str,
        task_spec: spec.PreparedTaskSpec,
        task_duration_hint: timedelta,
        solutions: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        super(SbSWorker, self).__init__(
            worker_info,
            config,
            pool_id,
            task_spec,
            task_duration_hint,
            solutions,
        )
        self.stats = {'not_swapped': 0, 'swapped': 0}

    def register_task(self, task: Dict[str, Any]) -> bool:
        input_objects = list(
            map(
                content,
                self.task_spec.task_mapping.from_task_values(task['input_values'])[
                    len(self.task_spec.function.hints) :
                ],
            )
        )
        assert input_objects, 'No input objects'
        assert len(input_objects) % 2 == 0, 'Odd number of input objects'
        left_objects, right_objects = input_objects[: len(input_objects) // 2], input_objects[len(input_objects) // 2 :]
        a_left, a_right = 'A' in left_objects[0], 'A' in right_objects[0]
        b_left, b_right = 'B' in left_objects[0], 'B' in right_objects[0]

        assert a_left != a_right
        assert b_left != b_right
        assert a_left == b_right

        for left, right in zip(left_objects, right_objects):
            a_left_loc, a_right_loc = 'A' in left, 'A' in right
            b_left_loc, b_right_loc = 'B' in left, 'B' in right

            assert a_left == a_left_loc
            assert a_right == a_right_loc
            assert b_left == b_left_loc
            assert b_right == b_right_loc

            self.stats['not_swapped'] += int(a_left_loc)
            self.stats['swapped'] += int(a_right_loc)
        return a_right

    def solve_task(self, task: Dict[str, Any], task_id: mapping.TaskID) -> Tuple[base.SbSChoice]:
        swapped = self.register_task(task)
        need_swap = self.config.treat_bias_as_original_a and swapped
        if need_swap:
            swapped_task = swap_task(self.task_spec.task_mapping.from_task_values(task['input_values']), self.task_spec)
            task = self.task_spec.task_mapping.toloka_values(swapped_task)

        answer = super(SbSWorker, self).solve_task(task, task_id)
        if need_swap:
            answer = answer.swap()
        return (answer,)


@dataclass(init=False)
class WorkerGroup:
    workers: List[Worker]

    def __init__(
        self,
        pool_id: str,
        task_spec: spec.PreparedTaskSpec,
        task_duration_hint: timedelta,
        # WorkerConfig(speed, quality), count
        worker_types: List[Tuple[WorkerConfig, int]],
        solutions: Optional[Dict[str, Dict[str, Any]]] = None,
        worker_cls: Type[Worker] = Worker,
    ):
        thread_pool = ThreadPool(8)
        configs = [config for config, count in worker_types for _ in range(count)]
        clients = [NDARegisterClient() for _ in configs]
        workers_info = thread_pool.map(lambda register_client: register_client.create_user(), clients)
        self.workers = [
            worker_cls(
                worker_info=info,
                pool_id=pool_id,
                task_spec=task_spec,
                task_duration_hint=task_duration_hint,
                solutions=solutions,
                config=config,
            )
            for info, config in zip(workers_info, configs)
        ]
        shuffle(self.workers)

    def start(self):
        for worker in self.workers:
            worker.start()

    def stop(self):
        for worker in self.workers:
            worker.stop()
