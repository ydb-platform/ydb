from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import toloka.client as toloka

from .. import evaluation

from .pipeline import FeedbackLoop
from .results import get_results


@dataclass
class Metrics:
    @dataclass
    class QualityDistribution:
        unknown: int
        ok: int
        not_ok: int

    markup_attempts: Dict[int, int]
    confidence_percentiles: Dict[int, float]
    quality: QualityDistribution

    cost_markup: float
    cost_check: float
    duration_seconds: float

    cost_markup_per_unit: Optional[float]
    cost_check_per_unit: Optional[float]
    session_speed: Optional[float]

    # TODO: add objects_speed, similar to our records_speed in audio transcript pipeline
    # TODO: cost doesn't consider incomplete assignments


percentiles = [10, 25, 50, 75, 90, 95, 99]


def calculate_metrics(
    fb_loop: FeedbackLoop,
    markup_pool_id: str,
    check_pool_id: str,
) -> Metrics:
    markup_pool = fb_loop.client.get_pool(markup_pool_id)
    check_pool = fb_loop.client.get_pool(check_pool_id)

    assert all(
        pool.status == toloka.Pool.Status.CLOSED for pool in (markup_pool, check_pool)
    ), 'pools must be closed before metrics calculation'

    duration_seconds = (check_pool.last_stopped - markup_pool.created).total_seconds()

    markup_assignments = fb_loop.get_human_markups(markup_pool_id)
    accepted_markup_assignments = [
        assignment for assignment, _ in markup_assignments if assignment.status == toloka.Assignment.ACCEPTED
    ]
    accepted_check_assignments = list(
        fb_loop.client.get_assignments(status=toloka.Assignment.ACCEPTED, pool_id=check_pool_id)
    )

    solution_id_to_evaluation = evaluation.collect_evaluations_from_check_assignments(
        assignments=accepted_check_assignments,
        check_task_mapping=fb_loop.check_task_mapping,
        pool_input_objects=None,
        aggregation_algorithm=fb_loop.params.evaluation.aggregation_algorithm,
        confidence_threshold=fb_loop.params.quality.confidence_threshold,
    )

    given_bonuses = evaluation.calculate_bonuses_for_pool_of_markup_assignments(
        markup_assignments=markup_assignments,
        solution_id_to_evaluation=solution_id_to_evaluation,
        check_task_mapping=fb_loop.check_task_mapping,
        control_params=fb_loop.params.control_markup,
    )

    cost_markup_bonus = sum(bonus.amount for bonus in given_bonuses)
    cost_markup = len(accepted_markup_assignments) * markup_pool.reward_per_assignment + cost_markup_bonus
    cost_check = len(accepted_check_assignments) * check_pool.reward_per_assignment

    cost_markup_per_unit = None
    cost_check_per_unit = None
    session_speed = None

    # TODO: bring volume back, broken after ASREXP-1687
    # if fb_loop.objects[0].volume() is not None:
    #     total_volume = sum(obj.volume() for obj in fb_loop.objects)
    #     cost_markup_per_unit = cost_markup / total_volume
    #     cost_check_per_unit = cost_check / total_volume
    #     session_speed = total_volume / duration_seconds

    markup_attempts = defaultdict(int)
    for attempts in evaluation.get_tasks_attempts(markup_assignments, fb_loop.markup_task_mapping).values():
        markup_attempts[attempts] += 1

    confidences = []
    quality_unknown = 0
    quality_ok = 0
    quality_not_ok = 0
    for task_result in get_results(
        fb_loop.pool_input_objects,
        markup_assignments,
        solution_id_to_evaluation,
        fb_loop.markup_task_mapping,
        fb_loop.check_task_mapping,
    ):
        task_confidences = [
            None if solution.evaluation is None else solution.evaluation.confidence
            for solution in task_result.solutions
        ]
        confidences += [c for c in task_confidences if c is not None]

        max_confidence = None
        for c in task_confidences:
            if c is None:
                continue
            if max_confidence is None:
                max_confidence = c
            elif c > max_confidence:
                max_confidence = c

        if max_confidence is None:
            quality_unknown += 1
        else:
            if max_confidence >= fb_loop.params.quality.confidence_threshold:
                quality_ok += 1
            else:
                quality_not_ok += 1

    return Metrics(
        markup_attempts=markup_attempts,
        confidence_percentiles={p: np.percentile(np.array(confidences), p) for p in percentiles},
        quality=Metrics.QualityDistribution(unknown=quality_unknown, ok=quality_ok, not_ok=quality_not_ok),
        cost_markup=cost_markup,
        cost_check=cost_check,
        duration_seconds=duration_seconds,
        cost_markup_per_unit=cost_markup_per_unit,
        cost_check_per_unit=cost_check_per_unit,
        session_speed=session_speed,
    )
