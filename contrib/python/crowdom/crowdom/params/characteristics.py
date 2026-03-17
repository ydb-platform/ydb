from datetime import timedelta
import time
from typing import List, Tuple, Optional, Union

import numpy as np

from .. import classification_loop, evaluation, pricing
from ..pricing import get_pool_price, AnnotationPriceFormula, _get_random_chance

from .util import get_overlap


ITERATIONS_COUNT: int = 500
CLASSIFICATION_WORKERS_COUNT = 7
ANNOTATION_WORKERS_COUNT = CLASSIFICATION_WORKERS_COUNT - 1


def hmean(a: list) -> float:
    return len(a) / (1 / np.maximum(np.array(a), 1e-12)).sum()


def simulate(
    overlap: classification_loop.Overlap,
    worker_q: float,
    control_task_count: int,
    control_tasks_for_accept: int,
    correct_control_tasks_in_solved: np.array,
    time_budget: float = 3.0,
    collect_rejected_attempts: bool = False,
    accuracy_threshold: Optional[int] = None,
) -> Tuple[int, int, int, float, bool]:
    start = time.time()
    ok = True
    overlap_increase = overlap.min_overlap
    needed_confidence = 0.0 if isinstance(overlap, classification_loop.StaticOverlap) else overlap.confidence
    if (
        collect_rejected_attempts and accuracy_threshold is not None
    ):  # we can have annotation setup, but no check sample
        needed_confidence = min(needed_confidence, accuracy_threshold / control_task_count)
    assert isinstance(needed_confidence, float)  # may be dict {label: float}
    accepted_solutions_count, loop_iterations, attempts = 0, 0, 0
    correct_control_tasks_in_accepted = np.array([], dtype=int)

    while overlap_increase > 0:
        loop_iterations += 1
        current = time.time()
        budget = time_budget - (current - start)
        if budget < 0 or attempts + overlap_increase >= len(correct_control_tasks_in_solved):
            ok = False
            break
        correct_control_tasks_in_collected = correct_control_tasks_in_solved[attempts : attempts + overlap_increase]
        attempts += overlap_increase

        accepted_solutions_count += (correct_control_tasks_in_collected >= control_tasks_for_accept).sum()

        if not collect_rejected_attempts:
            correct_control_tasks_in_collected = correct_control_tasks_in_collected[
                correct_control_tasks_in_collected >= control_tasks_for_accept
            ]

        correct_control_tasks_in_accepted = np.concatenate(
            (
                correct_control_tasks_in_accepted,
                correct_control_tasks_in_collected,
            )
        )

        collected_solutions_count = attempts if collect_rejected_attempts else accepted_solutions_count
        if collected_solutions_count < overlap.min_overlap:
            overlap_increase = overlap.min_overlap - collected_solutions_count
        elif collected_solutions_count == overlap.max_overlap:
            overlap_increase = 0
        else:
            if len(correct_control_tasks_in_accepted) == 0:
                confidence = 0.0
            else:
                confidence = correct_control_tasks_in_accepted.mean() / control_task_count
            if confidence >= needed_confidence:
                overlap_increase = 0
            else:
                overlap_increase = 1
    confidence = (
        correct_control_tasks_in_accepted.mean() / control_task_count
        if len(correct_control_tasks_in_accepted) > 0
        else 0.0
    )
    return (
        loop_iterations,
        accepted_solutions_count,
        attempts,
        hmean([confidence, len(correct_control_tasks_in_accepted) / 4]),
        ok,
    )


def simulate_avg(
    overlap: classification_loop.Overlap,
    worker_q: float,
    control_task_count: int,
    control_tasks_for_accept: int,
    n_iter: int = 100,
    time_budget: float = 3.0,
    collect_rejected_attempts: bool = False,
    accuracy_threshold: Optional[int] = None,
) -> Tuple[float, float, float, float, bool]:
    start = time.time()
    correct_control_tasks_in_solved = np.random.binomial(
        control_task_count, worker_q, (n_iter, control_task_count * 100)
    )
    loop_iterations_sum, attempts_sum, overlap_sum, confidence_sum = 0, 0, 0, 0.0
    ok = True

    for i in range(n_iter):
        current = time.time()
        budget = time_budget - (current - start)
        if budget < 0:
            ok = False
            break
        loop_iterations, iteration_overlap, attempts, confidence, ok = simulate(
            overlap,
            worker_q,
            control_task_count,
            control_tasks_for_accept,
            correct_control_tasks_in_solved=correct_control_tasks_in_solved[i],
            time_budget=budget,
            collect_rejected_attempts=collect_rejected_attempts,
            accuracy_threshold=accuracy_threshold,
        )
        loop_iterations_sum += loop_iterations
        attempts_sum += attempts
        confidence_sum += confidence
        overlap_sum += iteration_overlap
        if not ok:
            break
    return loop_iterations_sum / n_iter, overlap_sum / n_iter, attempts_sum / n_iter, confidence_sum / n_iter, ok


def _get_stats_and_chars(
    task_duration_hint: timedelta,
    task_count: int,
    control_task_count: int,
    assignment_price: float,
    overlap: str,
    overlap_confidence: float,
    control_tasks_for_accept: int,
    num_classes: int,
    verified_lang: bool,
    training: bool,
    training_score: int,
) -> Tuple[List[float], List[str]]:
    sz = 1000
    duration = task_duration_hint * (task_count + control_task_count)
    outputs = []

    commission = pricing.toloka_commission * assignment_price
    price_per_hour = timedelta(hours=1) / duration * assignment_price
    pricing_config = pricing.PoolPricingConfig(
        assignment_price=assignment_price,
        real_tasks_count=task_count,
        control_tasks_count=control_task_count,
    )
    calc_overlap = get_overlap(overlap, overlap_confidence)
    pool_price = get_pool_price(sz, pricing_config, overlap=calc_overlap).replace('$', '\\$')
    robustness = 1.0 - _get_random_chance(control_task_count, control_tasks_for_accept, num_classes)

    max_overlap = classification_loop.StaticOverlap(5)
    _, max_price, _ = pricing._get_assignment_params(
        task_count=2,
        task_duration_hint=task_duration_hint,
        price_per_hour=2.0,
    )
    max_pricing_config = pricing.PoolPricingConfig(
        assignment_price=max_price,
        real_tasks_count=1,
        control_tasks_count=1,
    )

    max_pool_price = float(get_pool_price(sz, max_pricing_config, overlap=max_overlap)[:-1])

    outputs.append(f'Robustness: {robustness:.2f}, recommended range - [0.6, 0.9]')
    outputs.append(f'Assignment duration: {duration}, recommended range - [30s, 5m]')
    if commission >= pricing.min_toloka_commission:
        outputs.append(f'Assignment commission, usd: {commission:.3f}')
    else:
        outputs.append(
            f'Warning: unprofitable assignment comission, usd: {pricing.min_toloka_commission:.3f} &gt; {commission:.3f}'
        )
    outputs.append(f'Expected worker earnings per hour: {price_per_hour:.3f}\\$')
    outputs.append(f'Expected price per {sz} objects: {pool_price}')

    worker_q = min(
        0.95,
        0.65 + 0.1 * (int(verified_lang) + int(training)) + 0.1 * (0.0 if not training else training_score / 100),
    )
    base_overlap = classification_loop.StaticOverlap(1)
    avg_iter, avg_overlap, _, avg_conf, ok = simulate_avg(
        calc_overlap, worker_q, max(1, control_task_count), control_tasks_for_accept, n_iter=ITERATIONS_COUNT
    )
    if control_task_count == 0:
        avg_conf = 0.1
    if not ok:
        outputs = ["<font color='red'> <b>WARNING: loop may not converge; unreliable results</b>"] + outputs

    pricing_config = pricing.PoolPricingConfig(
        assignment_price=assignment_price,
        real_tasks_count=task_count,
        control_tasks_count=control_task_count,
    )
    base_pool_price = float(get_pool_price(sz, pricing_config, base_overlap)[:-1])
    avg_pool_price = base_pool_price * avg_overlap
    outputs.append(f'Expected loop iterations: {avg_iter}')
    if isinstance(calc_overlap, classification_loop.DynamicOverlap):
        outputs.append(f'Expected average overlap: {avg_overlap}')
        outputs.append(f'Expected average price per {sz} objects: {avg_pool_price:.2f}\\$')
    #   outputs.append(f'MAX price per {sz} objects: {max_pool_price}')

    worker_count = CLASSIFICATION_WORKERS_COUNT
    worker_count -= int(task_count + control_task_count > 15)
    worker_count -= int(duration > timedelta(minutes=5))
    worker_count -= int(training)
    worker_count -= int(verified_lang)
    worker_count -= int(False if control_task_count == 0 else control_tasks_for_accept / control_task_count > 0.8)
    worker_count -= int(price_per_hour < 0.3)
    worker_count += int(price_per_hour > 0.9)

    outputs.append(f'Task attractiveness index: {worker_count - 1} / {CLASSIFICATION_WORKERS_COUNT}')

    c = avg_pool_price / max_pool_price * 3
    q = hmean([avg_conf, robustness])
    s = (1.0 - avg_iter / 5) * worker_count / CLASSIFICATION_WORKERS_COUNT
    t = 1.0 - s

    return [q, t, c], outputs


def _get_annotation_stats_and_chars(
    task_duration_hint: timedelta,
    task_count: int,
    assignment_price: float,
    check_sample: bool,
    max_tasks_to_check: int,
    accuracy_threshold: int,
    overlap: str,
    overlap_confidence: float,
    control_tasks_for_accept: int,
    verified_lang: bool,
) -> Tuple[List[float], List[str]]:
    sz = 1000
    duration = task_duration_hint * task_count
    outputs = []

    commission = pricing.toloka_commission * assignment_price
    price_per_hour = timedelta(hours=1) / duration * assignment_price
    calc_overlap = classification_loop.DynamicOverlap(
        min_overlap=1, max_overlap=int(overlap), confidence=overlap_confidence
    )

    max_task_count = task_count
    if check_sample:
        max_task_count = max_tasks_to_check

    # todo: is static overlap as max measure ok?
    max_overlap = classification_loop.StaticOverlap(5)
    _, max_price, _ = pricing._get_assignment_params(
        task_count=1,
        task_duration_hint=task_duration_hint,
        price_per_hour=2.0,
    )
    max_pricing_config = pricing.PoolPricingConfig(
        assignment_price=max_price,
        real_tasks_count=1,
        control_tasks_count=0,
    )
    # todo: we need to use get_annotation_price here with the clf pricing config
    max_pool_price = float(get_pool_price(sz, max_pricing_config, overlap=max_overlap)[:-1])

    outputs.append(f'Assignment duration: {duration}, recommended range - [30s, 5m]')
    if commission >= pricing.min_toloka_commission:
        outputs.append(f'Assignment commission, usd: {commission:.3f}')
    else:
        outputs.append(
            f'Warning: unprofitable assignment comission, usd: {pricing.min_toloka_commission:.3f} &gt; {commission:.3f}'
        )
    outputs.append(f'Expected worker earnings per hour: {price_per_hour:.3f}\\$')

    worker_q = min(0.95, 0.65 + 0.1 * int(verified_lang))

    base_overlap = classification_loop.StaticOverlap(1)
    avg_iter, avg_accepted, avg_attempts, avg_conf, ok = simulate_avg(
        calc_overlap,
        worker_q,
        max(1, max_task_count),
        control_tasks_for_accept,
        n_iter=ITERATIONS_COUNT,
        collect_rejected_attempts=True,
        accuracy_threshold=accuracy_threshold if check_sample else None,
    )

    if not ok:
        outputs = ["<font color='red'> <b>WARNING: loop may not converge; unreliable results</b>"] + outputs

    pricing_config = pricing.PoolPricingConfig(
        assignment_price=assignment_price,
        real_tasks_count=task_count,
        control_tasks_count=0,
    )
    base_pool_price = float(get_pool_price(sz, pricing_config, base_overlap)[:-1])
    avg_pool_price = base_pool_price * avg_accepted
    outputs.append(f'Expected loop iterations: {avg_iter}')
    outputs.append(f'Expected average overlap: {avg_attempts}')

    worker_count = ANNOTATION_WORKERS_COUNT
    worker_count -= int(task_count > 15)
    worker_count -= int(duration > timedelta(minutes=5))
    worker_count -= int(verified_lang)
    worker_count -= int(control_tasks_for_accept / max_tasks_to_check > 0.8)
    worker_count -= int(price_per_hour < 0.3)
    worker_count += int(price_per_hour > 0.9)

    outputs.append(f'Task attractiveness index: {worker_count - 1} / {ANNOTATION_WORKERS_COUNT}')

    c = avg_pool_price / max_pool_price * 5
    q = avg_conf
    s = (1.0 - (avg_iter + avg_attempts) / 10) * worker_count / ANNOTATION_WORKERS_COUNT
    t = 1.0 - s

    return [q, t, c], outputs


def _get_cumulative_stats_and_characteristics(
    check_quality: float,
    check_time: float,
    check_cost: float,
    markup_quality: float,
    markup_time: float,
    markup_cost: float,
    check_task_count: int,
    check_control_task_count: int,
    check_assignment_price: float,
    markup_task_count: int,
    markup_assignment_price: float,
    check_overlap: str,
    check_overlap_confidence: float,
    markup_overlap: str,
    markup_overlap_confidence: float,
    markup_check_sample: bool,
    markup_max_tasks_to_check: int,
    markup_accuracy_threshold: int,
) -> Tuple[List[Union[float, Tuple[float, str]]], List[str]]:
    sz = 1000

    check_pricing_config = pricing.PoolPricingConfig(
        real_tasks_count=check_task_count,
        control_tasks_count=check_control_task_count,
        assignment_price=check_assignment_price,
    )

    markup_pricing_config = pricing.PoolPricingConfig(
        real_tasks_count=markup_task_count,
        control_tasks_count=0,
        assignment_price=markup_assignment_price,
    )
    assignment_check_sample = None
    extra_text = ''
    check_coverage = 1.0
    if markup_check_sample:
        assignment_check_sample = evaluation.AssignmentCheckSample(
            max_tasks_to_check=markup_max_tasks_to_check,
            assignment_accuracy_finalization_threshold=markup_accuracy_threshold,
        )
        check_coverage = markup_max_tasks_to_check / markup_task_count
        color = 'black'
        if check_coverage < 0.5:
            color = 'red'
        extra_text = f"<font color='{color}'>Check coverage: {check_coverage:.2f}</font>"

    pool_price_formula = AnnotationPriceFormula(
        input_objects_count=sz,
        markup_config=markup_pricing_config,
        check_config=check_pricing_config,
        markup_overlap=get_overlap(f'1-{markup_overlap}', markup_overlap_confidence),
        check_overlap=get_overlap(check_overlap, check_overlap_confidence),
        assignment_check_sample=assignment_check_sample,
    )

    return [
        (hmean([check_quality, markup_quality]), extra_text),
        (check_time + check_coverage * markup_time) / 2,
        (check_cost + check_coverage * markup_cost) / 2,
    ], [f'Expected price for {sz} objects:'] + [f'$${v}$$' for v in pool_price_formula.clear_formula()]
