import abc
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
import threading
from time import sleep
from typing import List, Dict, Optional, Tuple, Any

from ipywidgets import Output
import matplotlib.pyplot as plt
from seaborn import heatmap

import pandas as pd
from IPython.display import display, clear_output, Image
import toloka.client as toloka

from .. import base, classification, classification_loop, evaluation, feedback_loop, mapping, task_spec as spec

logger = logging.getLogger(__name__)

METRICS_IMAGE_FILE = 'metrics.png'


def get_durations(assignment_solutions: List[mapping.AssignmentSolutions]) -> List[timedelta]:
    return [
        (assignment.submitted - assignment.created) / len(assignment.tasks)
        for assignment, _ in assignment_solutions
        if assignment.status != toloka.Assignment.REJECTED
    ]


@dataclass
class Metrics:
    durations: List[timedelta]
    overlaps: List[int]
    probas: List[float]
    confusion_matrix: Optional[pd.DataFrame] = None


def _collect_and_update_pool_metrics(
    toloka_client: toloka.TolokaClient,
    pool_id: str,
    history: Dict[str, List[int]],
) -> int:
    operation = toloka_client.get_analytics(
        [
            toloka.analytics_request.SubmittedAssignmentsCountPoolAnalytics(subject_id=pool_id),
            toloka.analytics_request.RejectedAssignmentsCountPoolAnalytics(subject_id=pool_id),
            toloka.analytics_request.ApprovedAssignmentsCountPoolAnalytics(subject_id=pool_id),
            toloka.analytics_request.SkippedAssignmentsCountPoolAnalytics(subject_id=pool_id),
        ]
    )
    operation = toloka_client.wait_operation(operation)

    for result in operation.details['value']:
        status = result['request']['name'].split('_')[0]
        history[status].append(result['result'])

    operation = toloka_client.get_analytics(
        [toloka.analytics_request.CompletionPercentagePoolAnalytics(subject_id=pool_id)]
    )
    operation = toloka_client.wait_operation(operation)
    completed = operation.details['value'][0]['result']['value']
    return completed


def _get_confusion_matrix(
    task_mapping: mapping.TaskMapping,
    assignment_solutions: List[mapping.AssignmentSolutions],
) -> pd.DataFrame:
    assert len(task_mapping.output_mapping) == 1
    label_class = task_mapping.output_mapping[0].obj_type
    assert issubclass(label_class, base.Label)
    label_set = label_class.possible_values()
    instance_to_value = {
        instance: value for instance, value in zip(label_class.possible_instances(), label_class.possible_values())
    }

    statistics = {real_label: {submitted_label: 0 for submitted_label in label_set} for real_label in label_set}
    for assignment, solutions in assignment_solutions:
        for task, solution in zip(assignment.tasks, solutions):
            if not task.known_solutions:
                continue
            assert len(task.known_solutions) == 1

            real_solution = task_mapping.from_solution(task.known_solutions[0])
            _, submitted_solution = solution
            statistics[instance_to_value[real_solution[0]]][instance_to_value[submitted_solution[0]]] += 1

    df = pd.DataFrame([statistics[label] for label in label_set], index=label_set)
    df.index.name = 'Real label'
    df.columns.name = 'Submitted label'
    return df


def _collect_and_update_classification_metrics(
    toloka_client: toloka.TolokaClient,
    task_mapping: mapping.TaskMapping,
    pool_id: str,
    assignment_evaluation_strategy: evaluation.AssignmentAccuracyEvaluationStrategy,
    aggregation_algorithm: classification.AggregationAlgorithm,
    history: Dict[str, List[int]],
    completed: List[int],
) -> Metrics:
    assignments_solutions = classification_loop.get_assignments_solutions(
        toloka_client,
        task_mapping,
        pool_id,
        [toloka.Assignment.ACCEPTED, toloka.Assignment.REJECTED, toloka.Assignment.SUBMITTED],
        with_control_tasks=True,
    )
    not_rejected_assignments_solutions = [
        (assignment, solution)
        for assignment, solution in assignments_solutions
        if assignment.status != toloka.Assignment.REJECTED
    ]
    durations = get_durations(assignments_solutions)

    task_id_to_label_confidence = classification_loop.calculate_label_probas(
        toloka_client,
        task_mapping,
        assignment_evaluation_strategy,
        aggregation_algorithm,
        assignments_solutions,
        pool_id,
    )
    task_id_to_current_overlap = evaluation.get_tasks_attempts(not_rejected_assignments_solutions, task_mapping)
    overlaps = [
        overlap for task_id, overlap in task_id_to_current_overlap.items() if task_id in task_id_to_label_confidence
    ]
    probas = [proba for _, proba in task_id_to_label_confidence.values()]
    completed.append(_collect_and_update_pool_metrics(toloka_client, pool_id, history))
    confusion_matrix = _get_confusion_matrix(task_mapping, not_rejected_assignments_solutions)
    return Metrics(durations, overlaps, probas, confusion_matrix)


def _collect_and_update_markup_metrics(
    toloka_client: toloka.TolokaClient,
    check_task_mapping: mapping.TaskMapping,
    markup_task_mapping: mapping.TaskMapping,
    check_pool_id: str,
    markup_pool_id: str,
    aggregation_algorithm: classification.AggregationAlgorithm,
    check_assignment_evaluation_strategy: evaluation.AssignmentAccuracyEvaluationStrategy,
    history: Dict[str, Dict[str, List[int]]],
    completed: Dict[str, List[int]],
) -> Tuple[Metrics, Metrics]:

    check_assignments = classification_loop.get_assignments_solutions(
        toloka_client,
        check_task_mapping,
        check_pool_id,
        [toloka.Assignment.ACCEPTED, toloka.Assignment.REJECTED, toloka.Assignment.SUBMITTED],
        with_control_tasks=True,
    )

    check_durations = get_durations(check_assignments)

    not_rejected_check_assignments_solutions = [
        assignment_solution
        for assignment_solution in check_assignments
        if assignment_solution[0].status != toloka.Assignment.REJECTED
    ]

    check_worker_weights = (
        evaluation.calculate_worker_weights(check_assignments, check_assignment_evaluation_strategy)
        if aggregation_algorithm == classification.AggregationAlgorithm.MAX_LIKELIHOOD
        else None
    )

    check_task_id_to_label_confidence = classification_loop.calculate_label_probas(
        toloka_client,
        check_task_mapping,
        check_assignment_evaluation_strategy,
        aggregation_algorithm,
        check_assignments,
        check_pool_id,
    )

    check_overlaps = [
        overlap
        for task_id, overlap in evaluation.get_tasks_attempts(
            not_rejected_check_assignments_solutions, check_task_mapping
        ).items()
        if task_id in check_task_id_to_label_confidence
    ]
    check_probas = [proba for _, proba in check_task_id_to_label_confidence.values()]
    check_confusion_matrix = _get_confusion_matrix(check_task_mapping, not_rejected_check_assignments_solutions)

    markup_assignments = classification_loop.get_assignments_solutions(
        toloka_client,
        markup_task_mapping,
        markup_pool_id,
        [toloka.Assignment.ACCEPTED, toloka.Assignment.REJECTED, toloka.Assignment.SUBMITTED],
    )

    markup_durations = get_durations(markup_assignments)

    markup_solution_id_to_evaluation = evaluation.collect_evaluations_from_check_assignments(
        assignments=list(toloka_client.get_assignments(status=toloka.Assignment.ACCEPTED, pool_id=check_pool_id)),
        check_task_mapping=check_task_mapping,
        pool_input_objects=None,
        aggregation_algorithm=aggregation_algorithm,
        confidence_threshold=0.0,
        worker_weights=check_worker_weights,
    )  # TODO: it seems not important which confidence to use here
    markup_task_id_to_attempts_count = evaluation.get_tasks_attempts(markup_assignments, markup_task_mapping)
    markup_overlaps = list(markup_task_id_to_attempts_count.values())
    markup_probas = [
        solution_evaluation.confidence for solution_evaluation in markup_solution_id_to_evaluation.values()
    ]

    completed['check'].append(_collect_and_update_pool_metrics(toloka_client, check_pool_id, history['check']))
    completed['annotation'].append(
        _collect_and_update_pool_metrics(toloka_client, markup_pool_id, history['annotation'])
    )
    return Metrics(markup_durations, markup_overlaps, markup_probas), Metrics(
        check_durations, check_overlaps, check_probas, check_confusion_matrix
    )


def _plot_timeline(ax, times: List[datetime], history: Dict[str, List[int]], pool_name: str = ''):
    for status, history_values in history.items():
        ax.plot(times, history_values, label=status)
    ax.legend(loc=4, fontsize=16)
    ax.set_title(f'{pool_name} task assignments timeline'.strip().capitalize(), size=24)
    ax.tick_params(labelsize=16)


def _plot_completion_timeline(ax, times: List[datetime], completed: List[int], pool_name: str = ''):
    ax.plot(times, completed, label='%')
    ax.legend(loc=4, fontsize=16)
    ax.set_title(f'{pool_name} completion timeline'.strip().capitalize(), size=24)
    ax.tick_params(labelsize=16)


def _plot_time_distribution(ax, durations: List[timedelta], task_duration_hint: timedelta, pool_name: str = ''):
    ax.axvline(task_duration_hint.total_seconds(), c='red', label='hint', linestyle='--')
    ax.hist([duration.total_seconds() for duration in durations], alpha=0.3)
    ax.set_title(f'{pool_name} task duration distribution (seconds)'.strip().capitalize(), size=24)
    ax.legend(loc=4, fontsize=16)
    ax.tick_params(labelsize=16)


def _plot_confidence_distribution(ax, probas: List[float], pool_name: str = ''):
    ax.hist(probas, alpha=0.3)
    ax.set_title(f'{pool_name} confidence distribution'.strip().capitalize(), size=24)
    ax.tick_params(labelsize=16)


def _plot_attempts_distribution(ax, overlaps: List[int], pool_name: str = ''):
    ax.hist(overlaps, alpha=0.3)
    ax.set_title(f'{pool_name} overlap distribution'.strip().capitalize(), size=24)
    ax.tick_params(labelsize=16)


def _plot_confusion_matrix(ax, confusion_matrix: Optional[pd.DataFrame], pool_name: str = ''):
    if confusion_matrix is None:
        return

    heatmap(
        confusion_matrix,
        ax=ax,
        vmin=0,
        annot=True,
        fmt="d",
        linewidths=0.5,
        cmap='Blues',
        annot_kws={'size': 14},
    )
    ax.tick_params(labelsize=16)
    ax.set_title(f'{pool_name} control tasks confusion matrix'.strip().capitalize(), size=24)


@dataclass
class MetricsPlotter:
    toloka_client: toloka.TolokaClient
    stop_event: threading.Event
    redraw_period_seconds: int
    thread: threading.Thread = field(init=False)
    output: Any = field(init=False)

    def __post_init__(self):
        self.plots_image_file_name = METRICS_IMAGE_FILE
        self.redraw_period_seconds = 60

        self.output = Output()
        display(self.output)
        self.thread = threading.Thread(target=self.plot)
        self.thread.start()

    @abc.abstractmethod
    def plot(self):
        ...

    def post_plot(self) -> bool:
        plt.savefig(self.plots_image_file_name)
        plt.close()

        with self.output:
            clear_output(wait=True)
            display(Image(self.plots_image_file_name))

        logger.debug('metrics are updated')

        if self.stop_event.is_set():
            logger.debug('terminating metrics plotter')
            return True

        sleep(self.redraw_period_seconds)
        return False

    def join(self, timeout: float):
        self.thread.join(timeout=timeout)


@dataclass
class ClassificationMetricsPlotter(MetricsPlotter):
    task_mapping: mapping.TaskMapping
    pool_id: str
    task_duration_hint: timedelta
    params: classification_loop.Params
    assignment_evaluation_strategy: evaluation.AssignmentAccuracyEvaluationStrategy

    def plot(self):
        history = defaultdict(list)
        completed = []
        times = []
        aggregation_algorithm = self.params.aggregation_algorithm

        while True:
            metrics = _collect_and_update_classification_metrics(
                self.toloka_client,
                self.task_mapping,
                self.pool_id,
                self.assignment_evaluation_strategy,
                aggregation_algorithm,
                history,
                completed,
            )
            times.append(datetime.now())
            with plt.style.context({'axes.labelsize': 20}):
                fig, ax = plt.subplots(2, 3, figsize=(30, 15))
                _plot_timeline(ax[0][0], times, history)
                _plot_completion_timeline(ax[0][1], times, completed)
                _plot_time_distribution(ax[0][2], metrics.durations, self.task_duration_hint)
                _plot_confidence_distribution(ax[1][0], metrics.probas)
                _plot_attempts_distribution(ax[1][1], metrics.overlaps)
                _plot_confusion_matrix(ax[1][2], metrics.confusion_matrix)
            stopped = self.post_plot()
            if stopped:
                return


@dataclass
class AnnotationMetricsPlotter(MetricsPlotter):
    task_spec: spec.AnnotationTaskSpec
    check_pool_id: str
    markup_pool_id: str
    check_task_duration_hint: timedelta
    markup_task_duration_hint: timedelta
    evaluation: feedback_loop.Evaluation

    def plot(self):
        history = defaultdict(lambda: defaultdict(list))
        completed = defaultdict(list)
        times = []
        assert isinstance(self.task_spec.function, base.AnnotationFunction)
        check_task_mapping, markup_task_mapping = self.task_spec.check.task_mapping, self.task_spec.task_mapping
        check_assignment_evaluation_strategy = evaluation.ControlTasksAssignmentAccuracyEvaluationStrategy(
            check_task_mapping
        )
        aggregation_algorithm = self.evaluation.aggregation_algorithm

        while True:
            markup_metrics, check_metrics = _collect_and_update_markup_metrics(
                self.toloka_client,
                check_task_mapping,
                markup_task_mapping,
                self.check_pool_id,
                self.markup_pool_id,
                aggregation_algorithm,
                check_assignment_evaluation_strategy,
                history,
                completed,
            )

            times.append(datetime.now())
            fig, ax = plt.subplots(4, 3, figsize=(30, 45))
            for i, (pool_name, metrics, hint) in enumerate(
                [
                    ('annotation', markup_metrics, self.markup_task_duration_hint),
                    ('check', check_metrics, self.check_task_duration_hint),
                ]
            ):
                _plot_timeline(ax[i][0], times, history[pool_name], pool_name=pool_name)
                _plot_completion_timeline(ax[i][1], times, completed[pool_name], pool_name=pool_name)
                _plot_time_distribution(ax[i][2], metrics.durations, hint, pool_name=pool_name)
                _plot_attempts_distribution(ax[2][i + 1], metrics.overlaps, pool_name=pool_name)
                if pool_name == 'annotation':
                    _plot_confidence_distribution(ax[2][0], metrics.probas, pool_name=pool_name)

            _plot_confusion_matrix(ax[3][1], check_metrics.confusion_matrix)

            stopped = self.post_plot()
            if stopped:
                return
