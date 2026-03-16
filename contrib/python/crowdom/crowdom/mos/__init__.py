import collections
import datetime
from collections import defaultdict
from dataclasses import dataclass
import logging
from typing import List, Tuple, Dict, Optional, Union, Set

import numpy as np
import toloka.client as toloka

from .. import base, classification, control, duration, evaluation, mapping
from ..worker import Human

logger = logging.getLogger(__name__)

# MOS implementation from https://www.microsoft.com/en-us/research/wp-content/uploads/2011/05/0002416.pdf

MIN_ASSIGNMENTS: int = 1  # todo: 10 as default in paper code, maybe should be a dynamic param
MIN_CORRELATION_FOR_ACCEPT: float = 0.25
MIN_CORRELATION_FOR_FINALIZATION: float = 0.7
EPS: float = 1e-10

STUB_CORRELATION: float = MIN_CORRELATION_FOR_ACCEPT + EPS
assert STUB_CORRELATION < MIN_CORRELATION_FOR_FINALIZATION


@dataclass
class ObjectsMetadata:
    item_id: str  # e.g. text that is being synthesised
    algorithm: str  # e.g. speech synthesis model


def get_solutions(
    assignment_solutions: List[mapping.AssignmentSolutions],
) -> List[Tuple[mapping.Objects, classification.WorkerLabel]]:
    tasks = []

    for assignment, solutions in assignment_solutions:
        for solution in solutions:
            (inputs, (score,)) = solution
            assert isinstance(score, base.Label)
            tasks.append((inputs, (score, Human(assignment))))
    return tasks


def t95(n: int, p=0.05) -> float:
    from scipy.stats import t

    return t.ppf(0.5 * (1 + (1.0 - p)), n)


class Stats:
    def __init__(self, values: List[float]):
        self.values = np.array(values)
        self.count = len(values)
        self.mean = np.mean(self.values)
        self.min = np.min(self.values)
        self.max = np.max(self.values)
        self.var = np.var(self.values)  # todo: check var vs std bias
        self.std = np.std(self.values)
        # self.kurtosis = sps.kurtosis(self.values)
        self.ci = None
        if self.count > 1:
            self.ci = t95(self.count - 1) * np.sqrt(self.var / self.count)


class StatDict(collections.UserDict):
    def __init__(self):
        super().__init__({})
        self.stat_values = []
        self.calc_stats = None

    def calculate(self):
        self.calc_stats = Stats(self.stat_values)


class StatList(collections.UserList):
    def __init__(self):
        super().__init__([])
        self.calc_stats = None


def correlation_coefficient(v1: Union[List[float], np.array], v2: Union[List[float], np.array]) -> float:
    from scipy.stats import moment

    assert len(v1) == len(v2)
    if len(v1) <= 1:
        return np.nan
    m1 = np.mean(v1)
    m2 = np.mean(v2)
    count = len(v1)

    sigma1 = np.sqrt(moment(v1, 2))
    sigma2 = np.sqrt(moment(v2, 2))

    sigma12 = 0.0
    for i in range(count):
        sigma12 += (v1[i] - m1) * (v2[i] - m2)
    sigma12 /= count
    return sigma12 / (sigma1 * sigma2 + EPS)


@dataclass
class MOSCI:
    mu: float
    ci: float

    def __post_init__(self):
        self.mu = round(float(self.mu), 2)
        self.ci = round(float(self.ci), 2)

    @property
    def left(self):
        return self.mu - self.ci

    @property
    def right(self):
        return self.mu + self.ci


@dataclass
class AssignmentSet:
    workers: List[str]
    items: List[str]
    algorithms: List[str]
    items_stats: StatDict
    worker_stats: StatDict
    assignments: List[mapping.AssignmentSolutions]

    def worker_correlation(self, worker: str) -> float:
        a_correlation = self.algorithm_mos_correlation(worker)
        s_correlation = self.item_mos_correlation(worker)
        if np.isnan(a_correlation) and np.isnan(s_correlation):
            # edge case: can happen if one worker gets only 1 partial page with one task
            # thus, we only have one submitted answer from them.
            # we should accept the assignment, but not count it towards final score
            return STUB_CORRELATION
        return a_correlation if not np.isnan(a_correlation) else s_correlation

    def item_mos_correlation(self, worker: str) -> float:
        v1, v2 = [], []
        for item in self.items:
            for algorithm in self.algorithms:
                if (
                    algorithm not in self.items_stats
                    or item not in self.items_stats[algorithm]
                    or worker not in self.items_stats[algorithm][item]
                ):
                    continue

                v1.append(self.items_stats[algorithm][item].calc_stats.mean)
                v2.append(self.items_stats[algorithm][item][worker].calc_stats.mean)

        return correlation_coefficient(v1, v2)

    def algorithm_mos_correlation(self, worker: str) -> float:
        v1, v2 = [], []
        for algorithm in self.algorithms:
            if algorithm not in self.worker_stats or worker not in self.worker_stats[algorithm]:
                continue

            v1.append(self.worker_stats[algorithm].calc_stats.mean)
            v2.append(self.worker_stats[algorithm][worker].calc_stats.mean)
        return correlation_coefficient(v1, v2)

    def fast_workers(
        self,
        fast_submits_predicate: control.AssignmentDurationPredicate,
        task_duration_function: duration.TaskDurationFunction,
        task_mapping: mapping.TaskMapping,
    ) -> Set[str]:
        fast_workers = set()
        for assignment, _ in self.assignments:
            assignment_duration = assignment.submitted - assignment.created
            assignment_duration_hint = sum(
                [task_duration_function(task_mapping.from_task(task)) for task in assignment.tasks],
                start=datetime.timedelta(seconds=0),
            )
            if fast_submits_predicate.check(
                assignment_duration,
                assignment_duration_hint=assignment_duration_hint,
            ):
                fast_workers.add(assignment.user_id)

        return fast_workers

    def bad_workers(self) -> Set[str]:
        # todo: there was unused min_assignments param
        bad_workers = set()
        for worker in self.workers:
            if self.worker_correlation(worker) < MIN_CORRELATION_FOR_ACCEPT:
                bad_workers.add(worker)
        return bad_workers

    def outliers(self) -> Set[str]:
        outliers = set()
        worker_to_assignment_count = defaultdict(int)
        for assignment, _ in self.assignments:
            worker_to_assignment_count[assignment.user_id] += 1

        for worker in self.workers:
            if (
                worker_to_assignment_count[worker] < MIN_ASSIGNMENTS
                or self.worker_correlation(worker) < MIN_CORRELATION_FOR_FINALIZATION
            ):
                outliers.add(worker)

        return outliers

    def get_algorithm_stats(self) -> Dict[str, np.array]:
        results = {}
        for algorithm in self.algorithms:
            m = []
            for w in self.workers:
                v = []
                for item in self.items:

                    if (
                        algorithm in self.items_stats
                        and item in self.items_stats[algorithm]
                        and w in self.items_stats[algorithm][item]
                        and self.items_stats[algorithm][item][w].calc_stats.mean
                    ):
                        v.append(self.items_stats[algorithm][item][w].calc_stats.mean)
                    else:
                        v.append(np.nan)
                m.append(v)

            results[algorithm] = np.array(m)

        return results

    def get_algorithm_ci(self) -> Dict[str, MOSCI]:
        algorithm_to_matrix = self.get_algorithm_stats()
        results = {}
        for algorithm in self.algorithms:
            matrix = algorithm_to_matrix[algorithm]
            t = t95(min(matrix.shape))
            mos = np.nanmean(matrix)
            ci95 = t * np.sqrt(mos_variance(matrix))

            results[algorithm] = MOSCI(mos, ci95)

        return results


def mos_variance(Z: np.array) -> np.array:
    N, M = Z.shape

    W = np.logical_not(np.isnan(Z))

    Mi = np.sum(W, 0)
    Nj = np.sum(W, 1)
    T = np.sum(W)

    v_su = vertical_var(Z.T)
    v_wu = vertical_var(Z)
    v_swu = np.nanvar(Z)

    if not np.isnan(v_su) and not np.isnan(v_wu):
        v = np.linalg.solve([[1.0, 0.0, 1.0], [0.0, 1.0, 1.0], [1.0, 1.0, 1.0]], [v_su, v_wu, v_swu])
        v = np.maximum(v, 0)
        v_s, v_w, v_u = v[0], v[1], v[2]
        v_mu = v_s * np.sum(Mi**2) / T**2 + v_w * np.sum(Nj**2) / T**2 + v_u / T
    elif np.isnan(v_su) and not np.isnan(v_wu):
        v = np.linalg.solve([[0.0, 1.0], [1.0, 1.0]], [v_wu, v_swu])
        v = np.maximum(v, 0)

        v_s, v_wu = v[0], v[1]
        v_mu = v_s * np.sum(Mi**2) / T**2 + v_wu / T

    elif not np.isnan(v_su) and np.isnan(v_wu):
        v = np.linalg.solve([[0.0, 1.0], [1.0, 1.0]], [v_su, v_swu])
        v = np.maximum(v, 0)
        v_w, v_su = v[0], v[1]

        v_mu = v_w * np.sum(Nj**2) / T**2 + v_su / T
    else:
        assert np.isnan(v_su) and np.isnan(v_wu)
        v_mu = v_swu / T

    return v_mu


def nancount(vec: np.array) -> int:
    return np.sum(np.logical_not(np.isnan(vec)))


def vertical_var(Z: np.array) -> np.array:
    rows, cols = Z.shape
    v = []
    for i in range(cols):
        if nancount(Z[:, i]) >= 2:
            v.append(np.nanvar(Z[:, i]))
    return np.mean(v)


def _build_stats(
    stats: Dict[str, Dict[str, Dict[str, List[float]]]], k1: List[str], k2: List[str], k3: List[str]
) -> StatDict:
    new_stats = StatDict()
    for key_1 in k1:
        if key_1 not in stats:
            continue
        if key_1 not in new_stats:
            new_stats[key_1] = StatDict()
        for key_2 in k2:
            if key_2 not in stats[key_1]:
                continue
            if key_2 not in new_stats[key_1]:
                new_stats[key_1][key_2] = StatDict()
            for key_3 in k3:
                if key_3 not in stats[key_1][key_2]:
                    continue
                if key_3 not in new_stats[key_1][key_2]:
                    new_stats[key_1][key_2][key_3] = StatList()
                calc_stats = Stats(stats[key_1][key_2][key_3])
                new_stats[key_1][key_2][key_3].calc_stats = calc_stats
                new_stats[key_1][key_2][key_3].extend(stats[key_1][key_2][key_3])
                new_stats[key_1][key_2].stat_values.append(calc_stats.mean)
                new_stats[key_1].stat_values.append(calc_stats.mean)

            new_stats[key_1][key_2].calculate()

        new_stats[key_1].calculate()

    return new_stats


def compute_algorithm_ci95(
    algorithms: List[str], items: List[str], workers: List[str], stats_items: StatDict, stats_workers: StatDict
):
    for algorithm in algorithms:
        if algorithm not in stats_items:
            continue

        t = 0
        mi = []
        nj = []

        v_wu = []

        for item in items:
            if item not in stats_items[algorithm]:
                continue

            if stats_items[algorithm][item].calc_stats.count >= 2:
                v_wu.append(stats_items[algorithm][item].calc_stats.var)
                mi.append(stats_items[algorithm][item].calc_stats.count)
        if v_wu:
            v_wu = np.mean(v_wu)

        v_su = []

        for w in workers:
            if w not in stats_workers[algorithm]:
                continue

            if stats_workers[algorithm][w].calc_stats.count >= 2:
                v_su.append(stats_workers[algorithm][w].calc_stats.var)
                nj.append(stats_workers[algorithm][w].calc_stats.count)

        if v_su:
            v_su = np.mean(v_su)
        v_swu = []

        for item in items:
            if item not in stats_items[algorithm]:
                continue

            for w in workers:
                if w not in stats_items[algorithm][item]:
                    continue
                v_swu.append(stats_items[algorithm][item][w].calc_stats.mean)
                t += 1

        v_swu = np.var(v_swu)
        mi2 = (np.array(mi) ** 2).sum()
        nj2 = (np.array(nj) ** 2).sum()

        if v_su and v_wu:
            m = np.array([[1.0, 0.0, 1.0], [0.0, 1.0, 1.0], [1.0, 1.0, 1.0]])
            c = np.array([[v_su], [v_wu], [v_swu]])
            v = np.linalg.inv(m) @ c
            v_s = max(v[0], 0.0)
            v_w = max(v[1], 0.0)
            v_u = max(v[2], 0.0)
            v_mu = v_s * mi2 / (t**2) + v_w * nj2 / (t**2) + v_u / t
        elif not v_su and v_wu:
            m = np.array([[0.0, 1.0], [1.0, 1.0]])
            c = np.array([[v_wu], [v_swu]])
            v = np.linalg.inv(m) @ c
            v_s = max(v[0], 0.0)
            v_wu = max(v[1], 0.0)
            v_mu = v_s * mi2 / (t**2) + v_wu / t

        elif v_su and not v_wu:
            m = np.array([[0.0, 1.0], [1.0, 1.0]])
            c = np.array([[v_su], [v_swu]])
            v = np.linalg.inv(m) @ c
            v_w = max(v[0], 0.0)
            v_su = max(v[1], 0.0)
            v_mu = v_w * nj2 / (t**2) + v_su / t
        else:
            assert not v_su and not v_wu
            v_mu = v_swu / t

        t = t95(min(len(items), len(workers)) - 1)
        stats_items[algorithm].calc_stats.ci = t * np.sqrt(v_mu)
        stats_workers[algorithm].calc_stats.ci = t * np.sqrt(v_mu)


def build_stats(
    assignments: List[mapping.AssignmentSolutions],
    inputs_to_metadata: Dict[mapping.Objects, ObjectsMetadata],
) -> AssignmentSet:
    tasks = get_solutions(assignments)

    workers = list(set(w.user_id for (_, (_, w)) in tasks))
    items = list(set(inputs_to_metadata[inputs].item_id for (inputs, _) in tasks))
    algorithms = list(set(inputs_to_metadata[inputs].algorithm for (inputs, _) in tasks))
    # stats_workers[algorithm][worker][sentence] = score
    stats_workers = defaultdict(lambda: defaultdict(dict))

    # stats_items[algorithm][sentence][worker] = score
    stats_items = defaultdict(lambda: defaultdict(dict))

    for (inputs, (score, w)) in tasks:
        # task_id = task_mapping.task_id(inputs).id
        assert isinstance(score, base.ScoreEvaluation)
        metadata = inputs_to_metadata[inputs]
        item, algorithm = metadata.item_id, metadata.algorithm
        stats_workers[algorithm][w.user_id][item] = [int(score.value)]
        stats_items[algorithm][item][w.user_id] = [int(score.value)]

    new_stats_items = _build_stats(stats_items, algorithms, items, workers)
    new_stats_workers = _build_stats(stats_workers, algorithms, workers, items)
    compute_algorithm_ci95(algorithms, items, workers, new_stats_items, new_stats_workers)
    return AssignmentSet(workers, items, algorithms, new_stats_items, new_stats_workers, assignments)


class KeyDefaultdict(defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret


@dataclass
class MOSEvaluationStrategy(evaluation.CustomEvaluationStrategy):
    inputs_to_metadata: Optional[Dict[mapping.Objects, ObjectsMetadata]]
    task_mapping: mapping.TaskMapping
    fast_submits_predicate: control.AssignmentDurationPredicate
    task_duration_function: duration.TaskDurationFunction
    updated: bool = False

    def __post_init__(self):
        if self.inputs_to_metadata is None:
            self.inputs_to_metadata = KeyDefaultdict(
                lambda items: ObjectsMetadata(self.task_mapping.task_id(items).id, 'default')
            )

    def ok(self, task: toloka.Task, solution: mapping.TaskSingleSolution) -> Optional[bool]:
        # abc restriction
        return None

    def update(self, submitted_assignments: List[mapping.AssignmentSolutions]):
        assignment_set = build_stats(submitted_assignments, self.inputs_to_metadata)
        fast_workers = assignment_set.fast_workers(
            self.fast_submits_predicate, self.task_duration_function, self.task_mapping
        )
        bad_workers = assignment_set.bad_workers()
        # todo: add logging?
        # todo: maybe reject only bad workers or fast _assignments_
        self.rejected_workers = fast_workers | bad_workers

        filtered_assignments = [
            (assignment, solution)
            for (assignment, solution) in submitted_assignments
            if assignment.user_id not in self.rejected_workers
        ]
        filtered_assignment_set = build_stats(filtered_assignments, self.inputs_to_metadata)

        outliers = filtered_assignment_set.outliers()

        self.final_assignments = [
            (assignment, solution)
            for (assignment, solution) in filtered_assignments
            if assignment.user_id not in outliers
        ]
        self.final_assignment_set = build_stats(self.final_assignments, self.inputs_to_metadata)

        self.updated = True

    def evaluate_assignment(self, assignment_solutions: mapping.AssignmentSolutions) -> evaluation.AssignmentEvaluation:
        assignment, solutions = assignment_solutions
        if not self.updated:
            # can happen inside metrics before end of loop iteration
            return evaluation.AssignmentEvaluation(
                assignment=assignment,
                ok_checks=0,
                total_checks=0,
                objects=len(solutions),
                incorrect_solution_indexes=[],
            )
        ok_checks = 0 if assignment.user_id in self.rejected_workers else 1

        return evaluation.AssignmentEvaluation(
            assignment=assignment,
            ok_checks=ok_checks,
            total_checks=1,
            objects=len(solutions),
            incorrect_solution_indexes=[],
        )
