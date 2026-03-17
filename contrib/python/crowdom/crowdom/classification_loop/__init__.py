from dataclasses import dataclass, field
import logging
from typing import List, Dict, Union, Optional, Tuple

import toloka.client as toloka

from .. import base, classification, control, duration, evaluation, mapping, mos, pool as pool_config, utils, worker

logger = logging.getLogger(__name__)


@dataclass
class Overlap:
    min_overlap: int


@dataclass
class StaticOverlap(Overlap):
    overlap: int
    min_overlap: int = field(init=False, repr=False)

    def __post_init__(self):
        self.min_overlap = self.overlap

    def __hash__(self) -> int:
        return self.overlap

    @property
    def max_overlap(self) -> int:
        return self.overlap


@dataclass
class DynamicOverlap(Overlap):
    min_overlap: int
    max_overlap: int
    confidence: Union[float, classification.TaskLabelsProbas]

    def min_confidence(self, label: base.Label) -> float:
        if isinstance(self.confidence, float):
            return self.confidence
        return self.confidence[label]

    def __hash__(self) -> int:
        return hash(repr(self))


@dataclass
class Params:
    overlap: Overlap
    control: control.Control
    task_duration_function: duration.TaskDurationFunction
    aggregation_algorithm: Optional[classification.AggregationAlgorithm] = None


class ClassificationLoop:
    client: toloka.TolokaClient
    task_mapping: mapping.TaskMapping
    params: Params
    lang: str
    assignment_evaluation_strategy: evaluation.AssignmentAccuracyEvaluationStrategy
    model_ws: Optional[worker.ModelWorkspace]

    def __init__(
        self,
        client: toloka.TolokaClient,
        task_mapping: mapping.TaskMapping,
        params: Params,
        lang: str,
        with_control_tasks: bool = True,
        model: Optional[worker.Model] = None,
    ):
        self.client = client
        self.task_mapping = task_mapping
        self.params = params
        self.lang = lang
        self.assignment_evaluation_strategy = evaluation.ControlTasksAssignmentAccuracyEvaluationStrategy(
            self.task_mapping,
            can_have_zero_checks=not with_control_tasks,
        )
        self.model_ws = None
        if model:
            self.model_ws = worker.ModelWorkspace(model=model, task_mapping=self.task_mapping)

    def create_pool(
        self,
        control_objects: List[mapping.TaskSingleSolution],
        pool_cfg: pool_config.ClassificationConfig,
    ) -> toloka.Pool:
        assert pool_cfg.overlap == self.params.overlap.min_overlap
        pool = self.client.create_pool(pool_config.create_pool_params(pool_cfg))
        control_tasks = []
        for objects in control_objects:
            task = self.task_mapping.to_control_task(objects)
            task.pool_id = pool.id
            control_tasks.append(task)
        logger.debug(f'creating {len(control_tasks)} control tasks')
        if control_tasks:
            # call with empty task list will fail
            self.client.create_tasks(control_tasks, allow_defaults=True, async_mode=True, skip_invalid_items=False)
        return pool

    def add_input_objects(
        self, pool_id: str, input_objects: Union[List[mapping.Objects], List[Tuple[mapping.Objects, worker.Worker]]]
    ):
        tasks = []
        for objects_data in input_objects:
            objects, user_id = objects_data, None
            if len(objects_data) == 2 and isinstance(objects_data[1], worker.Worker):
                objects = objects_data[0]
                if isinstance(objects_data[1], worker.Human):
                    user_id = [objects_data[1].user_id]
            task = self.task_mapping.to_task(objects)
            task.unavailable_for = user_id
            task.pool_id = pool_id
            tasks.append(task)
        logger.debug(f'creating {len(tasks)} tasks')
        self.client.create_tasks(tasks, allow_defaults=True, async_mode=True, skip_invalid_items=False)
        if not self.model_ws:
            # in case of model worker, pool is only needed to store tasks
            self.client.open_pool(pool_id)

    def prior_filtration_is_enabled(self) -> bool:
        return True

    def loop(self, pool_id: str):
        if self.model_ws:
            return

        iteration = 1
        while True:
            logger.debug(f'classification loop iteration #{iteration} is started')
            utils.wait_pool_for_close(self.client, pool_id)

            # TODO: collect stats about how workers answers control tasks and ignore bad control tasks by percentile

            submitted_assignments = self.get_assignments_solutions(
                pool_id, toloka.Assignment.SUBMITTED, with_control_tasks=True
            )

            logger.debug(f'{len(submitted_assignments)} submitted assignments are received')

            if self.prior_filtration_is_enabled():

                filtered_assignments, _ = evaluation.prior_filter_assignments(
                    self.client,
                    submitted_assignments,
                    self.params.control,
                    self.lang,
                    task_duration_function=self.params.task_duration_function,
                )
                logger.debug(f'{len(filtered_assignments)} assignments left after prior filter')

            else:
                filtered_assignments = submitted_assignments
                logger.debug('prior filtration is disabled')

            if isinstance(self.assignment_evaluation_strategy, evaluation.CustomEvaluationStrategy):
                self.assignment_evaluation_strategy.update(submitted_assignments)

            evaluation.evaluate_submitted_assignments_and_apply_rules(
                filtered_assignments,
                self.assignment_evaluation_strategy,
                self.params.control,
                self.client,
                self.lang,
                pool_id,
            )

            tasks_to_rework = rework_not_finalized_tasks(
                self.client,
                self.get_assignments_solutions(pool_id, [toloka.Assignment.ACCEPTED, toloka.Assignment.REJECTED]),
                self.task_mapping,
                # possible cases:
                # I. static overlap - increase until needed min_overlap is reached
                # II. dynamic overlap -
                #   1. increase until needed min_overlap is reached
                #   2. increase after new accepted solution, because not enough confidence accumulated
                #   3. NOT increase after new accepted solution, because enough confidence accumulated already
                #   4. increase after new rejected solution, because not enough confidence accumulated
                #   5. NOT increase after new rejected solution, because enough confidence accumulated due to
                #        confidence recalculation because worker weights are also recalculated
                self.get_task_id_to_overlap_increase(pool_id),
                pool_id,
            )
            if tasks_to_rework == 0:
                return

            iteration += 1

    def calculate_label_probas(
        self,
        assignment_solutions: List[mapping.AssignmentSolutions],
        pool_id: str,
    ) -> Dict[mapping.TaskID, classification.LabelProba]:
        return calculate_label_probas(
            self.client,
            self.task_mapping,
            self.assignment_evaluation_strategy,
            self.params.aggregation_algorithm,
            assignment_solutions,
            pool_id,
        )

    def get_task_id_to_overlap_increase(self, pool_id: str) -> Dict[mapping.TaskID, int]:
        # todo: we maybe should use _all_ assignments in dynamic overlap proba calculation, even rejected ones
        assignments_solutions = get_assignments_solutions(
            self.client,
            self.task_mapping,
            pool_id,
            [toloka.Assignment.ACCEPTED, toloka.Assignment.REJECTED],
            with_control_tasks=True,
        )
        accepted_assignments_solutions = [
            assignment_solution
            for assignment_solution in assignments_solutions
            if assignment_solution[0].status == toloka.Assignment.ACCEPTED
        ]
        task_id_to_current_overlap = evaluation.get_tasks_attempts(accepted_assignments_solutions, self.task_mapping)
        use_dynamic = isinstance(self.params.overlap, DynamicOverlap)
        task_id_to_label_confidence = self.calculate_label_probas(assignments_solutions, pool_id) if use_dynamic else {}
        task_ids = get_task_id_map(self.client, pool_id, self.task_mapping).keys()
        result = {}
        for task_id in task_ids:
            overlap = task_id_to_current_overlap[task_id]
            residual_overlap = self.params.overlap.min_overlap - overlap
            if residual_overlap > 0:
                result[task_id] = residual_overlap
            elif use_dynamic:
                assert isinstance(self.params.overlap, DynamicOverlap)
                label, conf = task_id_to_label_confidence[task_id]
                if overlap < self.params.overlap.max_overlap and conf < self.params.overlap.min_confidence(label):
                    result[task_id] = 1
        return result

    def get_assignments_solutions(
        self,
        pool_id: str,
        status: List[toloka.Assignment.Status],
        with_control_tasks: bool = False,
    ) -> List[mapping.AssignmentSolutions]:
        return get_assignments_solutions(self.client, self.task_mapping, pool_id, status, with_control_tasks)

    def get_results(
        self,
        pool_id: str,
        pool_input_objects: Optional[List[mapping.Objects]] = None,
    ) -> Tuple[classification.Results, Optional[classification.WorkerWeights]]:
        pool_input_objects, accepted_assignments, worker_weights = self.get_assignments_and_worker_weights(
            pool_id, pool_input_objects
        )

        return (
            classification.collect_labels_probas_from_assignments(
                assignments=accepted_assignments,
                task_mapping=self.task_mapping,
                pool_input_objects=pool_input_objects,
                aggregation_algorithm=self.params.aggregation_algorithm,
                worker_weights=worker_weights,
            ),
            worker_weights,
        )

    def get_pool_input_objects(self, pool_id: str) -> List[mapping.Objects]:
        return get_pool_input_objects(self.client, self.task_mapping, pool_id)

    def get_assignments_and_worker_weights(
        self,
        pool_id: str,
        pool_input_objects: Optional[List[mapping.Objects]] = None,
    ) -> Tuple[List[mapping.Objects], List[toloka.Assignment], Optional[classification.WorkerWeights]]:
        if self.model_ws:
            # TODO(DATAFORGE-75): model can only substitute all human workers
            #  mixture of model and human solutions is not implemented yet
            pool_input_objects = pool_input_objects or get_pool_input_objects(self.client, self.task_mapping, pool_id)
            assignment = self.model_ws.get_solutions(pool_input_objects, toloka.Assignment.ACCEPTED)
            return pool_input_objects, [assignment], {self.model_ws.model.id: 1.0}

        return get_assignments_and_worker_weights(
            self.client,
            self.task_mapping,
            pool_id,
            self.params.aggregation_algorithm,
            self.assignment_evaluation_strategy,
            pool_input_objects,
        )


class MOSLoop(ClassificationLoop):
    def __init__(
        self,
        client: toloka.TolokaClient,
        task_mapping: mapping.TaskMapping,
        params: Params,
        lang: str,
        with_control_tasks: bool = False,
        model: Optional[worker.Model] = None,
        inputs_to_metadata: Optional[Dict[mapping.Objects, mos.ObjectsMetadata]] = None,
    ):
        assert not with_control_tasks
        super().__init__(client, task_mapping, params, lang, with_control_tasks, model)
        speed_rules = params.control.filter_rules(
            predicate_type=control.AssignmentDurationPredicate, action_type=control.BlockUser
        )
        assert len(speed_rules) == 1
        predicate: control.AssignmentDurationPredicate = speed_rules[0].predicate
        self.assignment_evaluation_strategy = mos.MOSEvaluationStrategy(
            inputs_to_metadata,
            task_mapping,
            predicate,
            task_duration_function=params.task_duration_function,
        )

    def get_task_id_to_overlap_increase(self, pool_id: str):
        return {}

    def prior_filtration_is_enabled(self) -> bool:
        return False


# in case of classification task, pool input objects are specified at start
# in case of feedback loop, we don't know pool input objects for check in advance
# here order of pool input objects is not important, only in getting results for classification task
def get_pool_input_objects(
    client: toloka.TolokaClient,
    task_mapping: mapping.TaskMapping,
    pool_id: str,
) -> List[mapping.Objects]:
    pool_input_objects = []
    for task in client.get_tasks(pool_id):
        if not task.known_solutions:
            pool_input_objects.append(task_mapping.from_task(task))
    return pool_input_objects


def get_assignments_solutions(
    client: toloka.TolokaClient,
    task_mapping: mapping.TaskMapping,
    pool_id: str,
    status: List[toloka.Assignment.Status],
    with_control_tasks: bool = False,
) -> List[mapping.AssignmentSolutions]:
    return mapping.get_assignments_solutions(
        assignments=list(client.get_assignments(status=status, pool_id=pool_id)),
        mapping=task_mapping,
        with_control_tasks=with_control_tasks,
    )


def calculate_label_probas(
    client: toloka.TolokaClient,
    task_mapping: mapping.TaskMapping,
    assignment_evaluation_strategy: evaluation.AssignmentAccuracyEvaluationStrategy,
    aggregation_algorithm: classification.AggregationAlgorithm,
    assignment_solutions: List[mapping.AssignmentSolutions],
    pool_id: str,
) -> Dict[mapping.TaskID, classification.LabelProba]:
    task_id_to_label_confidence = {}
    worker_weights = evaluation.calculate_worker_weights(assignment_solutions, assignment_evaluation_strategy)
    input_objects = get_pool_input_objects(client, task_mapping, pool_id)
    accepted_assignments = [
        assignment for assignment, _ in assignment_solutions if assignment.status == toloka.Assignment.ACCEPTED
    ]
    tasks_probas = [
        classification.get_most_probable_label(probas)
        for probas, _ in classification.collect_labels_probas_from_assignments(
            assignments=accepted_assignments,
            task_mapping=task_mapping,
            pool_input_objects=input_objects,
            aggregation_algorithm=aggregation_algorithm,
            worker_weights=worker_weights,
        )
    ]
    for task_input_objects, probas in zip(input_objects, tasks_probas):
        if probas is not None:
            task_id = mapping.TaskID(task_input_objects)
            task_id_to_label_confidence[task_id] = probas
    return task_id_to_label_confidence


def rework_not_finalized_tasks(
    client: toloka.TolokaClient,
    assignments: List[mapping.AssignmentSolutions],
    task_mapping: mapping.TaskMapping,
    task_id_to_overlap_increase: Dict[mapping.TaskID, int],
    pool_id: str,
) -> int:
    # full task ID list is available through created tasks, not assignments, because:
    # - input objects can be added iteratively in common case, "pool input objects" notion may be missing
    # - assignments statuses, attempts from which we take into account, can differ depends on scenario
    task_id_map = get_task_id_map(client, pool_id, task_mapping)
    tasks_to_rework = 0
    # model worker solutions do not affect Toloka overlap
    for task_id, attempts in evaluation.get_tasks_attempts(assignments, task_mapping, with_model=False).items():
        overlap_increase = task_id_to_overlap_increase.get(task_id)
        if not overlap_increase:
            continue
        tasks_to_rework += 1
        toloka_task_id = task_id_map[task_id]
        new_overlap = attempts + overlap_increase
        logger.debug(f'rework task "{task_id.id}", set overlap = {new_overlap} for toloka task {toloka_task_id}')
        client.patch_task_overlap_or_min(
            task_id=toloka_task_id, patch=toloka.task.TaskOverlapPatch(overlap=new_overlap)
        )

    if tasks_to_rework > 0:
        client.open_pool(pool_id)

    return tasks_to_rework


def get_task_id_map(
    client: toloka.TolokaClient,
    pool_id: str,
    task_mapping: mapping.TaskMapping,
    with_control_tasks: bool = False,
) -> Dict[mapping.TaskID, str]:
    return {
        mapping.TaskID(task_mapping.from_task(task)): task.id
        for task in client.get_tasks(pool_id)
        if (with_control_tasks or not task.known_solutions)
    }


def get_assignments_and_worker_weights(
    client: toloka.TolokaClient,
    task_mapping: mapping.TaskMapping,
    pool_id: str,
    aggregation_algorithm: classification.AggregationAlgorithm,
    assignment_evaluation_strategy: evaluation.AssignmentAccuracyEvaluationStrategy,
    pool_input_objects: Optional[List[mapping.Objects]] = None,
) -> Tuple[List[mapping.Objects], List[toloka.Assignment], Optional[classification.WorkerWeights]]:
    pool_input_objects = pool_input_objects or get_pool_input_objects(client, task_mapping, pool_id)

    all_assignments = get_assignments_solutions(
        client, task_mapping, pool_id, [toloka.Assignment.ACCEPTED, toloka.Assignment.REJECTED], with_control_tasks=True
    )
    accepted_assignments = [
        assignment for assignment, _ in all_assignments if assignment.status == toloka.Assignment.ACCEPTED
    ]
    worker_weights = (
        evaluation.calculate_worker_weights(all_assignments, assignment_evaluation_strategy)
        if aggregation_algorithm == classification.AggregationAlgorithm.MAX_LIKELIHOOD
        else None
    )

    return pool_input_objects, accepted_assignments, worker_weights
