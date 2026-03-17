import abc
import datetime
from collections import defaultdict
from dataclasses import dataclass
import logging
from random import shuffle
from typing import List, Dict, Optional, Tuple, Iterable, Set

import toloka.client as toloka

from .. import base, classification, control, duration, mapping
from ..worker import Human, Model, Worker

logger = logging.getLogger(__name__)


@dataclass
class SolutionEvaluation:
    ok: bool
    confidence: float

    worker_labels: List[classification.WorkerLabel]


def collect_evaluations_from_check_assignments(
    assignments: List[toloka.Assignment],
    check_task_mapping: mapping.TaskMapping,
    pool_input_objects: Optional[List[mapping.Objects]],
    aggregation_algorithm: classification.AggregationAlgorithm,
    confidence_threshold: float,
    worker_weights: Optional[classification.WorkerWeights] = None,
) -> Dict[mapping.TaskID, SolutionEvaluation]:
    if pool_input_objects is None:
        # TODO: questionable
        # In feedback loop, we don't know exactly, which markup solutions we've checked in case of check sample.
        # So pool input objects are generated from check assignments. Order of solutions in relation to input objects
        # will be lost, but it doesn't matter for feedback loop, solutions will be reordered according to confidence.
        # In other cases, i.e. classification tasks, we want to preserve order of solutions in relation to input
        # objects, and it will be preserved, because we will have pool input objects.
        pool_input_objects = []
        task_ids = set()
        for assignment in assignments:
            for task_id, input_objects, _ in mapping.iterate_assignment(assignment, check_task_mapping):
                if task_id in task_ids:
                    continue
                task_ids.add(task_id)
                pool_input_objects.append(input_objects)

    evaluations = []
    for task_labels_probas, worker_labels in classification.collect_labels_probas_from_assignments(
        assignments,
        check_task_mapping,
        pool_input_objects,
        aggregation_algorithm,
        worker_weights,
    ):
        # TODO: support other evaluation types
        # TODO: add individual workers evaluations like in base loop
        # TODO: may be None
        assert task_labels_probas is not None
        confidence = base.BinaryEvaluation.get_ok_confidence(task_labels_probas)
        assert worker_labels
        evaluations.append(
            SolutionEvaluation(
                ok=bool(confidence >= confidence_threshold),  # to convert from numpy bool
                confidence=confidence,
                worker_labels=worker_labels,
            )
        )

    return {
        check_task_mapping.task_id(input_objects): evaluation
        for input_objects, evaluation in zip(pool_input_objects, evaluations)
    }


@dataclass
class AssignmentCheckSample:
    max_tasks_to_check: Optional[int]
    assignment_accuracy_finalization_threshold: Optional[float]


def find_markup_solutions_to_check(
    markup_assignments: List[mapping.AssignmentSolutions],
    checked_solutions: Set[mapping.TaskID],
    check_task_mapping: mapping.TaskMapping,
    check_sample: Optional[AssignmentCheckSample],
) -> Tuple[List[Tuple[mapping.Objects, Worker]], Set[mapping.TaskID]]:
    solutions_to_check = []
    already_checked_solutions = set()
    for assignment, solutions in markup_assignments:
        assert assignment.status == toloka.Assignment.SUBMITTED
        if assignment.id is not None:
            worker = Human(assignment)
        else:
            worker = Model(assignment.user_id, None)
        not_checked_solutions = []
        for input_objects, output_objects in solutions:
            solution = input_objects + output_objects
            solution_id = check_task_mapping.task_id(solution)
            if solution_id in checked_solutions:
                already_checked_solutions.add(solution_id)
            else:
                not_checked_solutions.append(solution)
        from_model = not assignment.id
        solutions_to_check += [
            (assignment_solution, worker)
            for assignment_solution in sample_solutions_to_check(not_checked_solutions, check_sample, from_model)
        ]
    return solutions_to_check, already_checked_solutions


def sample_solutions_to_check(
    solutions: List[mapping.Objects],
    check_sample: Optional[AssignmentCheckSample],
    from_model: bool = False,
) -> List[mapping.Objects]:
    if check_sample is None or check_sample.max_tasks_to_check is None or from_model:
        # check sample is disabled, or it is model solutions from single assignment with arbitrary task count
        return solutions
    shuffle(solutions)
    return solutions[: check_sample.max_tasks_to_check]


def get_tasks_attempts(
    assignments: Iterable[mapping.AssignmentSolutions],
    markup_task_mapping: mapping.TaskMapping,
    with_model: bool = True,
) -> Dict[mapping.TaskID, int]:
    task_id_to_attempts = defaultdict(int)
    for assignment, solutions in assignments:
        for input_objects, _ in solutions:
            if not assignment.id and not with_model:
                increase = 0
            else:
                increase = 1
            task_id_to_attempts[markup_task_mapping.task_id(input_objects)] += increase
    return task_id_to_attempts


def process_assignment_by_duration(
    reject_rules: List[control.Rule],
    block_rules: List[control.Rule],
    assignment: toloka.Assignment,
    lang: str,
    client: toloka.TolokaClient,
    assignment_duration_hint: datetime.timedelta,
) -> Optional[toloka.Assignment.Status]:
    assignment_duration = assignment.submitted - assignment.created
    verdict: Optional[toloka.Assignment.Status] = None
    # We need to first block user, if needed, and only after that set the assignment status:
    # in feedback loop case, both SUBMITTED and REJECTED assignments can arrive here.
    # We do not want to re-apply block to users from already rejected assignments,
    # we do it by taking assignment.status into account in BlockUSer.
    # However, if we set assignment statuses to rejected first, and deal with blocks later,
    # and feedback loop is interrupted between these steps, after the restart user wouldn't be blocked
    # because the assignment will be deemed as "already processed" by BlockUser

    for rule in block_rules:
        restriction = rule.apply(
            value=assignment_duration,
            assignment_duration_hint=assignment_duration_hint,
            client=client,
            user_id=assignment.user_id,
            pool_id=assignment.pool_id,
            block_start=assignment.submitted,
            assignment_status=assignment.status,
        )
        if restriction is not None:
            logger.debug(f'add restriction {restriction} for user {assignment.user_id} by {rule.predicate}')

    for rule in reject_rules:
        verdict = rule.apply(
            value=assignment_duration,
            assignment_duration_hint=assignment_duration_hint,
            client=client,
            assignment=assignment,
            public_comment=assignment_short_rejection_comment[lang],
        )
        if verdict is not None:
            logger.debug(f'set assignment {assignment.id} to status {verdict.value} by {rule.predicate}')
            break
    return verdict


# Some prior assignments checks, i.e. check for completion time, can be performed before real evaluation,
# to avoid possibly time-cost consuming (in case of feedback loop) evaluation.
def prior_filter_assignments(
    client: toloka.TolokaClient,
    submitted_assignments: Iterable[mapping.AssignmentSolutions],
    control_params: control.Control,
    lang: str,
    task_duration_function: duration.TaskDurationFunction,
) -> Tuple[List[mapping.AssignmentSolutions], List[mapping.AssignmentSolutions]]:
    reject_rules = control_params.filter_rules(
        predicate_type=control.AssignmentDurationPredicate, action_type=control.SetAssignmentStatus
    )

    block_rules = control_params.filter_rules(
        predicate_type=control.AssignmentDurationPredicate, action_type=control.BlockUser
    )

    filtered_assignments = []
    fast_assignments = []
    for assignment_solution in submitted_assignments:
        assignment, solutions = assignment_solution
        assignment_duration_hint = sum(
            [task_duration_function(input_objects) for (input_objects, _) in solutions],
            start=datetime.timedelta(seconds=0),
        )
        verdict = process_assignment_by_duration(
            reject_rules=reject_rules,
            block_rules=block_rules,
            assignment=assignment,
            lang=lang,
            client=client,
            assignment_duration_hint=assignment_duration_hint,
        )
        if verdict is None:
            filtered_assignments.append(assignment_solution)
        elif verdict == toloka.Assignment.Status.REJECTED:
            fast_assignments.append(assignment_solution)
        else:
            assert False, f'Found fast assignment {assignment.id} with unexpected status {assignment.status}'
    return filtered_assignments, fast_assignments


@dataclass
class AssignmentEvaluation:
    assignment: toloka.Assignment
    ok_checks: int
    total_checks: int
    objects: int
    incorrect_solution_indexes: List[int]

    def get_accuracy(self) -> Optional[float]:
        if self.total_checks == 0:
            return None
        return self.ok_checks / self.total_checks

    def get_evaluation_recall(self) -> float:
        return self.total_checks / self.objects


class AssignmentAccuracyEvaluationStrategy:
    can_have_zero_checks = False

    @abc.abstractmethod
    def ok(self, task: toloka.Task, solution: mapping.TaskSingleSolution) -> Optional[bool]:
        ...

    def evaluate_assignment(self, assignment_solutions: mapping.AssignmentSolutions) -> AssignmentEvaluation:
        ok_checks, total_checks = 0, 0
        incorrect_solutions_indexes = []
        assignment, solutions = assignment_solutions
        for i, (task, solution) in enumerate(zip(assignment.tasks, solutions)):
            ok = self.ok(task, solution)
            if ok is None:
                continue
            total_checks += 1
            if ok:
                ok_checks += 1
            else:
                incorrect_solutions_indexes.append(i)
        assert self.can_have_zero_checks or total_checks > 0, f'zero checks for assignment {assignment}'

        return AssignmentEvaluation(
            assignment=assignment,
            ok_checks=ok_checks,
            total_checks=total_checks,
            objects=len(solutions),
            incorrect_solution_indexes=incorrect_solutions_indexes,
        )


# driven by evaluations in check pool
@dataclass
class CheckAssignmentAccuracyEvaluationStrategy(AssignmentAccuracyEvaluationStrategy):
    solution_id_to_evaluation: Dict[mapping.TaskID, SolutionEvaluation]
    check_task_mapping: mapping.TaskMapping

    def ok(self, task: toloka.Task, solution: mapping.TaskSingleSolution) -> Optional[bool]:
        input_objects, output_objects = solution
        solution_id = self.check_task_mapping.task_id(input_objects + output_objects)
        if solution_id not in self.solution_id_to_evaluation:
            return None
        return self.solution_id_to_evaluation[solution_id].ok


# driven by control tasks in each assignment
@dataclass
class ControlTasksAssignmentAccuracyEvaluationStrategy(AssignmentAccuracyEvaluationStrategy):
    task_mapping: mapping.TaskMapping
    can_have_zero_checks: bool = False

    def ok(self, task: toloka.Task, solution: mapping.TaskSingleSolution) -> Optional[bool]:
        # we count on embedding control task single expected solution to Task.known_solutions
        if not task.known_solutions:
            return None
        assert len(task.known_solutions) == 1
        expected_solution = task.known_solutions[0].output_values
        _, output_objects = solution
        actual_solution = self.task_mapping.to_solution(output_objects).output_values

        # we check only values which are in expected solution, some output values,
        # i.e. comments, is meaninglessly to check
        return {**actual_solution, **expected_solution} == actual_solution


class CustomEvaluationStrategy(AssignmentAccuracyEvaluationStrategy):
    @abc.abstractmethod
    def update(self, *args, **kwargs):
        ...


def apply_rules_to_assignment(
    set_verdict_rules: List[control.Rule],
    block_rules: List[control.Rule],
    evaluation: AssignmentEvaluation,
    client: toloka.TolokaClient,
    lang: str,
    pool_id: str,
) -> toloka.Assignment.Status:
    verdict: Optional[toloka.Assignment.Status] = None
    accuracy = evaluation.get_accuracy()
    for rule in block_rules:
        if not evaluation.assignment.id:
            continue  # model workers are not blocked
        restriction = rule.apply(
            value=accuracy,
            client=client,
            user_id=evaluation.assignment.user_id,
            pool_id=pool_id,
            block_start=evaluation.assignment.submitted,
            assignment_status=evaluation.assignment.status,
        )
        if restriction is not None:
            logger.debug(f'add restriction {restriction} for user {evaluation.assignment.user_id} by {rule.predicate}')
    for rule in set_verdict_rules:
        verdict = rule.apply(
            value=accuracy,
            client=client,
            assignment=evaluation.assignment,
            public_comment=get_assignment_rejection_comment(evaluation.incorrect_solution_indexes, lang),
        )
        if not evaluation.assignment.id:
            # model worker assignments are not present in Toloka storage, so we have no possibility to store their
            # status as for usual workers, so we set it on the fly
            evaluation.assignment.status = verdict
        if verdict is not None:
            logger.debug(f'set assignment {evaluation.assignment.id} to status {verdict.value} by {rule.predicate}')
            break
    assert verdict is not None
    return verdict


assignment_short_rejection_comment = base.LocalizedString(
    {
        'EN': 'Too few correct solutions',
        'RU': 'Мало правильных решений',
        'TR': 'Çok az doğru karar var',
        'KK': 'Дұрыс шешімдер аз',
    }
)

assignment_rejection_comment = base.LocalizedString(
    {
        'EN': 'Check the tasks with numbers: {incorrect_solutions_str}. Learn more about tasks acceptance and filing '
        'appeals in the project instructions.',
        'RU': 'Проверьте задания c номерами: {incorrect_solutions_str}. Подробнее о проверке заданий и подаче '
        'апелляций читайте в инструкции проекта.',
        'TR': 'Numaralardaki görevleri kontrol edin: {incorrect_solutions_str}. Görevlerin incelenmesi ve temyiz '
        'başvurusu hakkında daha fazla bilgi için proje talimatlarını okuyun.',
        'KK': 'Тапсырмаларды нөмірлермен тексеріңіз: {incorrect_solutions_str}. Тапсырмаларды тексеру және '
        'апелляцияларды беру туралы толығырақ жоба нұсқаулығынан оқыңыз.',
    }
)


def get_assignment_rejection_comment(incorrect_solutions_indexes: List[int], lang: str) -> str:
    if incorrect_solutions_indexes:
        incorrect_solutions_str = ', '.join(str(i + 1) for i in incorrect_solutions_indexes)
        return assignment_rejection_comment[lang].format(incorrect_solutions_str=incorrect_solutions_str)
    return assignment_short_rejection_comment[lang]


def evaluate_submitted_assignments_and_apply_rules(
    submitted_assignments: List[mapping.AssignmentSolutions],
    assignment_accuracy_evaluation_strategy: AssignmentAccuracyEvaluationStrategy,
    control_params: control.Control,
    client: toloka.TolokaClient,
    lang: str,
    pool_id: str,
) -> List[toloka.Assignment.Status]:
    assert all(assignment.status == toloka.Assignment.SUBMITTED for assignment, _ in submitted_assignments)

    assignment_evaluations = [
        assignment_accuracy_evaluation_strategy.evaluate_assignment(assignment) for assignment in submitted_assignments
    ]

    set_verdict_rules = control_params.filter_rules(
        predicate_type=control.AssignmentAccuracyPredicate, action_type=control.SetAssignmentStatus
    )

    block_rules = control_params.filter_rules(
        predicate_type=control.AssignmentAccuracyPredicate, action_type=control.BlockUser
    )

    return [
        apply_rules_to_assignment(set_verdict_rules, block_rules, evaluation, client, lang, pool_id)
        for evaluation in assignment_evaluations
    ]


def get_markup_task_id(check_task_id: mapping.TaskID, markup_task_mapping: mapping.TaskMapping) -> mapping.TaskID:
    """
    We receive markup task evaluations as map markup_solution_id -> evaluation.
    Sometimes we need to get markup_task_id from markup_solution_id.
    """
    return markup_task_mapping.task_id(check_task_id.objects[: len(markup_task_mapping.input_mapping)])


# The task is being finalized if at least one of the following conditions is met:
# 1) It has a solution of acceptable quality.
# 2) The number of attempts is exhausted.
# 3) In case of check sample, if the task from the assignment is not checked,
#    but the accuracy of the assignment is quite high.
def find_finalized_tasks(
    markup_assignments: List[mapping.AssignmentSolutions],
    solution_id_to_evaluation: Dict[mapping.TaskID, SolutionEvaluation],
    markup_task_mapping: mapping.TaskMapping,
    check_task_mapping: mapping.TaskMapping,
    check_sample: Optional[AssignmentCheckSample],
    max_object_markup_attempts: Optional[int],
) -> Set[mapping.TaskID]:
    assignment_accuracy_evaluation_strategy = CheckAssignmentAccuracyEvaluationStrategy(
        solution_id_to_evaluation, check_task_mapping
    )
    assignments_evaluations = [
        assignment_accuracy_evaluation_strategy.evaluate_assignment(assignment) for assignment in markup_assignments
    ]
    task_id_to_markup_attempts = get_tasks_attempts(markup_assignments, markup_task_mapping)

    finalized_task_ids = set()
    for solution_id, solution_evaluation in solution_id_to_evaluation.items():
        if solution_evaluation.ok:
            finalized_task_ids.add(get_markup_task_id(solution_id, markup_task_mapping))

    for task_id, markup_attempts in task_id_to_markup_attempts.items():
        if max_object_markup_attempts is not None and markup_attempts >= max_object_markup_attempts:
            finalized_task_ids.add(task_id)

    assignment_id_to_accuracy = {
        evaluation.assignment.id: evaluation.get_accuracy() for evaluation in assignments_evaluations
    }

    for assignment, solutions in markup_assignments:
        assignment_accuracy = assignment_id_to_accuracy.get(assignment.id)
        if (
            assignment_accuracy is not None
            and check_sample is not None
            and check_sample.assignment_accuracy_finalization_threshold is not None
            and assignment_accuracy >= check_sample.assignment_accuracy_finalization_threshold
        ):
            for input_objects, output_objects in solutions:
                solution_id = check_task_mapping.task_id(input_objects + output_objects)
                if solution_id not in solution_id_to_evaluation:
                    finalized_task_ids.add(markup_task_mapping.task_id(input_objects))

    return finalized_task_ids


def get_ok_tasks_ratio(
    solution_id_to_evaluation: Dict[mapping.TaskID, SolutionEvaluation],
    markup_task_mapping: mapping.TaskMapping,
) -> float:
    tasks, ok_tasks = set(), set()
    for solution_id, solution_evaluation in solution_id_to_evaluation.items():
        task_id = get_markup_task_id(solution_id, markup_task_mapping)
        tasks.add(task_id)
        if solution_evaluation.ok:
            ok_tasks.add(task_id)
    return len(ok_tasks) / len(tasks)


def calculate_bonuses_for_markup_assignment(
    rules: List[control.Rule],
    assignment_evaluation: AssignmentEvaluation,
) -> List[toloka.user_bonus.UserBonus]:
    bonuses = []
    for rule in rules:
        bonus = rule.apply(
            user_id=assignment_evaluation.assignment.user_id,
            assignment_id=assignment_evaluation.assignment.id,
            value=assignment_evaluation.get_accuracy(),
        )
        if bonus is not None:
            bonuses.append(bonus)

    return bonuses


def calculate_bonuses_for_pool_of_markup_assignments(
    markup_assignments: List[mapping.AssignmentSolutions],
    solution_id_to_evaluation: Dict[mapping.TaskID, SolutionEvaluation],
    check_task_mapping: mapping.TaskMapping,
    control_params: control.Control,
) -> List[toloka.user_bonus.UserBonus]:
    assert all(
        assignment.status in (toloka.Assignment.ACCEPTED, toloka.Assignment.REJECTED)
        for assignment, _ in markup_assignments
    )

    # todo add ability to filter rules by only action/only predicate type
    bonus_rules = control_params.filter_rules(
        predicate_type=control.AssignmentAccuracyPredicate, action_type=control.GiveBonusToUser
    )

    if not bonus_rules:
        return []

    assignment_accuracy_evaluation_strategy = CheckAssignmentAccuracyEvaluationStrategy(
        solution_id_to_evaluation, check_task_mapping
    )
    assignment_evaluations = [
        assignment_accuracy_evaluation_strategy.evaluate_assignment(assignment) for assignment in markup_assignments
    ]

    bonuses = []
    for evaluation in assignment_evaluations:
        bonuses += calculate_bonuses_for_markup_assignment(bonus_rules, evaluation)

    return bonuses


def calculate_worker_weights(
    assignments: List[mapping.AssignmentSolutions],
    assignment_accuracy_evaluation_strategy: Optional[AssignmentAccuracyEvaluationStrategy],
    smoothness_k: float = 0.5,
) -> classification.WorkerWeights:
    assignment_evaluations = [
        assignment_accuracy_evaluation_strategy.evaluate_assignment(assignment) for assignment in assignments
    ]
    correct_count = defaultdict(int)
    total_count = defaultdict(int)
    for assignment_evaluation in assignment_evaluations:
        worker_id = assignment_evaluation.assignment.user_id
        correct_count[worker_id] += assignment_evaluation.ok_checks
        total_count[worker_id] += assignment_evaluation.total_checks

    return {
        worker_id: (correct + smoothness_k) / (total_count[worker_id] + 2 * smoothness_k)
        for worker_id, correct in correct_count.items()
    }
