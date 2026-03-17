import logging
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

import toloka.client as toloka

from .. import classification, classification_loop, datasource, evaluation, mapping, pool as pool_config, utils, worker

from .params import Params, Evaluation
from .results import Results, get_results

logger = logging.getLogger(__name__)


@dataclass
class CheckMarkupIterationStats:
    tasks_to_re_markup: int
    ok_tasks_ratio: float


class FeedbackLoop:
    pool_input_objects: List[mapping.Objects]
    check_params: classification_loop.Params
    markup_params: Params
    evaluation: Evaluation
    markup_task_mapping: mapping.TaskMapping
    check_task_mapping: mapping.TaskMapping
    check_loop: classification_loop.ClassificationLoop
    client: toloka.TolokaClient
    lang: str
    s3: Optional[datasource.S3]
    model_ws: Optional[worker.ModelWorkspace]

    def __init__(
        self,
        pool_input_objects: List[mapping.Objects],
        markup_task_mapping: mapping.TaskMapping,
        check_task_mapping: mapping.TaskMapping,
        markup_params: Params,
        check_params: classification_loop.Params,
        client: toloka.TolokaClient,
        lang: str,
        s3: Optional[datasource.S3] = None,
        model_markup: Optional[worker.Model] = None,
        model_check: Optional[worker.Model] = None,
    ):
        self.evaluation = Evaluation(
            aggregation_algorithm=check_params.aggregation_algorithm,
            assignment_check_sample=markup_params.assignment_check_sample,
            overlap=check_params.overlap,
        )

        self.pool_input_objects = pool_input_objects
        self.check_params = check_params
        self.markup_params = markup_params
        self.client = client
        self.markup_task_mapping = markup_task_mapping
        self.check_task_mapping = check_task_mapping
        self.check_loop = classification_loop.ClassificationLoop(
            client=client,
            task_mapping=self.check_task_mapping,
            params=check_params,
            lang=lang,
            model=model_check,
            # TODO
            #  1) also look at control tasks count for check pool
            #  2) (DATAFORGE-75): correct only when model substitutes all solutions
            with_control_tasks=model_check is None,
        )
        self.lang = lang
        self.s3 = s3
        self.model_ws = None
        if model_markup:
            self.model_ws = worker.ModelWorkspace(model=model_markup, task_mapping=self.markup_task_mapping)

    def create_pools(
        self,
        control_objects: List[mapping.TaskSingleSolution],
        markup_pool_config: pool_config.MarkupConfig,
        check_pool_config: pool_config.ClassificationConfig,
    ) -> Tuple[toloka.Pool, toloka.Pool]:
        check_sample = self.evaluation.assignment_check_sample
        if check_sample is not None and check_sample.max_tasks_to_check is not None:
            assert check_sample.max_tasks_to_check <= markup_pool_config.real_tasks_count

        markup_pool = self.client.create_pool(pool_config.create_pool_params(markup_pool_config))

        markup_tasks = []
        for input_objects in self.pool_input_objects:
            task = self.markup_task_mapping.to_task(input_objects)
            task.pool_id = markup_pool.id
            markup_tasks.append(task)

        logger.debug(f'creating {len(markup_tasks)} markup tasks')
        self.client.create_tasks(markup_tasks, allow_defaults=True, async_mode=True, skip_invalid_items=False)

        check_pool = self.check_loop.create_pool(control_objects, check_pool_config)

        if self.model_ws is None:
            # If model is presented, first attempt is by it, no need to open pool.
            # In case of rejected model solutions pool will be opened later.
            self.client.open_pool(markup_pool.id)

        return markup_pool, check_pool

    def loop(self, markup_pool_id: str, check_pool_id: str):
        iteration = 1
        while True:
            logger.debug(f'feedback loop iteration #{iteration} is started')
            markup_assignments, fast_markup_assignments = self.get_markups(markup_pool_id, check_pool_id)
            check_stats = self.check_markups(markup_pool_id, check_pool_id, markup_assignments, fast_markup_assignments)
            if check_stats is None:
                break
            iteration += 1
        self.give_bonuses_to_users(check_pool_id=check_pool_id, markup_assignments=markup_assignments)

    def give_bonuses_to_users(
        self,
        check_pool_id: str,
        markup_assignments: List[mapping.AssignmentSolutions],
    ) -> Optional[toloka.operations.Operation]:
        bonuses = evaluation.calculate_bonuses_for_pool_of_markup_assignments(
            markup_assignments=markup_assignments,
            solution_id_to_evaluation=self.get_checks(check_pool_id),
            check_task_mapping=self.check_task_mapping,
            control_params=self.markup_params.control,
        )
        if not bonuses:
            return None

        logger.debug(f'giving {len(bonuses)} bonuses to users')
        op = self.client.create_user_bonuses_async(bonuses)
        result = self.client.wait_operation(op)
        assert (
            result.status == toloka.operations.Operation.Status.SUCCESS
        ), f'bonus issuing failed, operation id: {result.id}, details: {result.details}'

        return result

    def get_results(
        self,
        markup_pool_id: str,
        check_pool_id: str,
    ) -> Tuple[Results, Optional[classification.WorkerWeights]]:
        checks, worker_weights = self.get_checks_and_weights(check_pool_id)
        return (
            get_results(
                self.pool_input_objects,
                self.get_markups(markup_pool_id, check_pool_id)[0],
                checks,
                self.markup_task_mapping,
                self.check_task_mapping,
            ),
            worker_weights,
        )

    def get_markups(
        self, markup_pool_id: str, check_pool_id: str
    ) -> Tuple[List[mapping.AssignmentSolutions], List[mapping.AssignmentSolutions]]:
        filtered_human_markups, fast_markups = evaluation.prior_filter_assignments(
            self.client,
            self.get_human_markups(markup_pool_id),
            self.markup_params.control,
            self.lang,
            task_duration_function=self.markup_params.task_duration_function,
        )
        return filtered_human_markups + self.get_model_markups(check_pool_id), fast_markups

    def get_human_markups(self, pool_id: str) -> List[mapping.AssignmentSolutions]:
        utils.wait_pool_for_close(self.client, pool_id)
        return datasource.substitute_media_output(
            mapping.get_assignments_solutions(
                assignments=list(
                    self.client.get_assignments(
                        status=[toloka.Assignment.SUBMITTED, toloka.Assignment.ACCEPTED, toloka.Assignment.REJECTED],
                        pool_id=pool_id,
                    )
                ),
                mapping=self.markup_task_mapping,
            ),
            self.s3,
            self.client,
        )

    def get_model_markups(self, pool_id: str) -> List[mapping.AssignmentSolutions]:
        if not self.model_ws:
            return []

        # TODO(DATAFORGE-75): model can work only on first attempt

        assignment = self.model_ws.get_solutions(self.pool_input_objects)

        solution_id_to_evaluation = self.get_checks(pool_id)

        assignment_solutions = mapping.get_assignments_solutions(
            assignments=[assignment],
            mapping=self.markup_task_mapping,
        )

        if solution_id_to_evaluation:
            # needed to set actual assignment status
            evaluation.evaluate_submitted_assignments_and_apply_rules(
                assignment_solutions,
                evaluation.CheckAssignmentAccuracyEvaluationStrategy(
                    solution_id_to_evaluation,
                    self.check_task_mapping,
                ),
                self.markup_params.control,
                self.client,
                self.lang,
                pool_id,
            )

        return assignment_solutions

    def check_markups(
        self,
        markup_pool_id: str,
        check_pool_id: str,
        markup_assignments: List[mapping.AssignmentSolutions],
        fast_markup_assignments: List[mapping.AssignmentSolutions],
    ) -> Optional[CheckMarkupIterationStats]:
        submitted_markup_assignments = [
            (assignment, solutions)
            for assignment, solutions in markup_assignments
            if assignment.status == toloka.Assignment.SUBMITTED
        ]

        if len(submitted_markup_assignments) == 0:
            return None

        logger.debug(f'checking {len(submitted_markup_assignments)} submitted markup assignments')

        solution_id_to_evaluation = self.get_checks(check_pool_id)

        self.create_new_check_tasks(submitted_markup_assignments, solution_id_to_evaluation, check_pool_id)

        solution_id_to_evaluation = self.get_checks(check_pool_id)

        evaluation.evaluate_submitted_assignments_and_apply_rules(
            submitted_markup_assignments,
            evaluation.CheckAssignmentAccuracyEvaluationStrategy(solution_id_to_evaluation, self.check_task_mapping),
            self.markup_params.control,
            self.client,
            self.lang,
            markup_pool_id,
        )

        finalized_tasks = evaluation.find_finalized_tasks(
            markup_assignments,
            solution_id_to_evaluation,
            self.markup_task_mapping,
            self.check_task_mapping,
            self.evaluation.assignment_check_sample,
            self.markup_params.overlap.max_overlap,
        )

        task_id_to_overlap_increase = {
            task_id: 1
            for task_id in (
                {self.markup_task_mapping.task_id(objects) for objects in self.pool_input_objects} - finalized_tasks
            )
        }

        tasks_to_re_markup = classification_loop.rework_not_finalized_tasks(
            client=self.client,
            assignments=markup_assignments + fast_markup_assignments,
            task_mapping=self.markup_task_mapping,
            task_id_to_overlap_increase=task_id_to_overlap_increase,
            pool_id=markup_pool_id,
        )

        ok_tasks_ratio = evaluation.get_ok_tasks_ratio(solution_id_to_evaluation, self.markup_task_mapping)

        return CheckMarkupIterationStats(tasks_to_re_markup, ok_tasks_ratio)

    def get_checks_and_weights(
        self,
        pool_id: str,
    ) -> Tuple[Dict[mapping.TaskID, evaluation.SolutionEvaluation], Optional[classification.WorkerWeights]]:
        self.check_loop.loop(pool_id)
        _, accepted_assignments, worker_weights = self.check_loop.get_assignments_and_worker_weights(pool_id)
        solution_id_to_evaluation = evaluation.collect_evaluations_from_check_assignments(
            assignments=accepted_assignments,
            check_task_mapping=self.check_task_mapping,
            pool_input_objects=None,
            aggregation_algorithm=self.evaluation.aggregation_algorithm,
            confidence_threshold=self.markup_params.overlap.confidence,
            worker_weights=worker_weights,
        )
        for solution_id, solution_evaluation in solution_id_to_evaluation.items():
            # solutions from previous iterations are also displayed here, may be fix it
            logger.debug(
                f'solution "{solution_id}" is {"OK" if solution_evaluation.ok else "BAD"}, '
                f'confidence={solution_evaluation.confidence:.3f}'
            )
        return solution_id_to_evaluation, worker_weights

    def get_checks(self, pool_id: str) -> Dict[mapping.TaskID, evaluation.SolutionEvaluation]:
        return self.get_checks_and_weights(pool_id)[0]

    def create_new_check_tasks(
        self,
        submitted_markup_assignments: List[mapping.AssignmentSolutions],
        solution_id_to_evaluation: Dict[mapping.TaskID, evaluation.SolutionEvaluation],
        check_pool_id: str,
    ):
        solutions_to_check, already_checked_solutions = evaluation.find_markup_solutions_to_check(
            markup_assignments=submitted_markup_assignments,
            checked_solutions=set(solution_id_to_evaluation.keys()),
            check_task_mapping=self.check_task_mapping,
            check_sample=self.evaluation.assignment_check_sample,
        )

        # TODO: build metric based on already_checked_solutions ratio
        for solution_id in already_checked_solutions:
            logger.debug(f'solution "{solution_id}" is already checked')

        if len(solutions_to_check) == 0:
            return

        self.check_loop.add_input_objects(check_pool_id, solutions_to_check)
