from collections import defaultdict
from dataclasses import dataclass
import enum
from typing import List, Dict, Optional

import toloka.client as toloka

from .. import evaluation, mapping, worker as workers


class SolutionVerdict(enum.Enum):
    BAD = 0
    UNKNOWN = 1
    OK = 2


@dataclass
class Solution:
    solution: mapping.Objects
    verdict: SolutionVerdict
    evaluation: Optional[evaluation.SolutionEvaluation]
    assignment_accuracy: float  # ratio of correctly completed tasks in source assignment
    assignment_evaluation_recall: float  # ratio of evaluated tasks in source assignment
    worker: workers.Worker

    def __lt__(self, other: 'Solution') -> bool:
        if self.verdict.value != other.verdict.value:
            return self.verdict.value < other.verdict.value

        assert (
            self.evaluation is None
            and other.evaluation is None
            or self.evaluation is not None
            and other.evaluation is not None
        )

        if self.evaluation is not None and self.evaluation.confidence != other.evaluation.confidence:
            return self.evaluation.confidence < other.evaluation.confidence

        def f1(p, r):
            return 2 * (p * r) / (p + r)

        return f1(self.assignment_accuracy, self.assignment_evaluation_recall) < f1(
            other.assignment_accuracy, other.assignment_evaluation_recall
        )


Results = List[List[Solution]]


def get_results(
    pool_input_objects: List[mapping.Objects],
    markup_assignments: List[mapping.AssignmentSolutions],
    solution_id_to_evaluation: Dict[mapping.TaskID, evaluation.SolutionEvaluation],
    markup_task_mapping: mapping.TaskMapping,
    check_task_mapping: mapping.TaskMapping,
) -> Results:
    assert all(
        assignment.status in (toloka.Assignment.ACCEPTED, toloka.Assignment.REJECTED)
        for assignment, _ in markup_assignments
    )

    assignment_accuracy_evaluation_strategy = evaluation.CheckAssignmentAccuracyEvaluationStrategy(
        solution_id_to_evaluation, check_task_mapping
    )
    assignments_evaluations = [
        assignment_accuracy_evaluation_strategy.evaluate_assignment(assignment) for assignment in markup_assignments
    ]
    assignment_id_to_accuracy = {e.assignment.id: e.get_accuracy() for e in assignments_evaluations}
    assignment_id_to_evaluation_recall = {e.assignment.id: e.get_evaluation_recall() for e in assignments_evaluations}

    task_id_to_result = defaultdict(list)
    for assignment, solutions in markup_assignments:
        for input_objects, output_objects in solutions:
            task_id = markup_task_mapping.task_id(input_objects)
            solution_id = check_task_mapping.task_id(input_objects + output_objects)
            solution_evaluation = solution_id_to_evaluation.get(solution_id)
            verdict = SolutionVerdict.UNKNOWN
            if solution_evaluation:
                verdict = SolutionVerdict.OK if solution_evaluation.ok else SolutionVerdict.BAD
            solution = Solution(
                solution=output_objects,
                worker=workers.Human(assignment),
                verdict=verdict,
                evaluation=solution_evaluation,
                assignment_accuracy=assignment_id_to_accuracy[assignment.id],
                assignment_evaluation_recall=assignment_id_to_evaluation_recall[assignment.id],
            )
            task_id_to_result[task_id].append(solution)

    for solutions in task_id_to_result.values():
        solutions.sort(reverse=True)

    # order results in pool input order
    results = []
    for input_objects in pool_input_objects:
        results.append(task_id_to_result[markup_task_mapping.task_id(input_objects)])

    return results
