from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from .. import (
    duration,
    classification,
    classification_loop,
    control as control_params,
    evaluation,
    feedback_loop,
    pricing,
    worker,
)


@dataclass(init=False)
class ExpertParams:
    task_duration_hint: timedelta
    assignment_price: float

    real_tasks_count: int

    @property
    def pricing_config(self) -> pricing.PoolPricingConfig:
        return pricing.PoolPricingConfig(
            assignment_price=self.assignment_price,
            real_tasks_count=self.real_tasks_count,
            control_tasks_count=0,
        )

    def __init__(self, task_duration_hint: timedelta, pricing_config: pricing.PoolPricingConfig):
        self.task_duration_hint = task_duration_hint
        self.assignment_price = pricing_config.assignment_price
        self.real_tasks_count = pricing_config.real_tasks_count


@dataclass(init=False)
class Params(ExpertParams):
    worker_filter: worker.BaseWorkerFilter

    control_tasks_count: int
    pricing_strategy: pricing.PricingStrategy

    overlap: classification_loop.Overlap
    aggregation_algorithm: Optional[classification.AggregationAlgorithm]

    control: control_params.Control
    work_duration_before_vacation: timedelta
    # TODO(DATAFORGE-75): model is always applied on first loop iteration
    model: Optional[worker.Model]

    task_duration_function: Optional[duration.TaskDurationFunction]

    def __init__(
        self,
        task_duration_hint: timedelta,
        pricing_config: pricing.PoolPricingConfig,
        overlap: classification_loop.Overlap,
        control: control_params.Control,
        worker_filter: worker.BaseWorkerFilter,
        aggregation_algorithm: Optional[classification.AggregationAlgorithm] = None,
        pricing_strategy: pricing.PricingStrategy = pricing.StaticPricingStrategy(),
        task_duration_function: Optional[duration.TaskDurationFunction] = None,
        work_duration_before_vacation: timedelta = timedelta(hours=1),
    ):
        super(Params, self).__init__(task_duration_hint, pricing_config)
        self.control_tasks_count = pricing_config.control_tasks_count
        self.overlap = overlap
        self.aggregation_algorithm = aggregation_algorithm
        self.control = control
        self.worker_filter = worker_filter
        self.pricing_strategy = pricing_strategy
        self.model = None
        self.task_duration_function = task_duration_function
        self.work_duration_before_vacation = work_duration_before_vacation

    @property
    def pricing_config(self) -> pricing.PoolPricingConfig:
        return pricing.PoolPricingConfig(
            assignment_price=self.assignment_price,
            real_tasks_count=self.real_tasks_count,
            control_tasks_count=self.control_tasks_count,
        )

    @property
    def classification_loop_params(self) -> classification_loop.Params:
        task_duration_function = self.task_duration_function
        if task_duration_function is None:
            task_duration_function = duration.get_const_task_duration_function(self.task_duration_hint)
        return classification_loop.Params(
            aggregation_algorithm=self.aggregation_algorithm,
            overlap=self.overlap,
            control=self.control,
            task_duration_function=task_duration_function,
        )


@dataclass(init=False)
class AnnotationParams(Params):
    assignment_check_sample: Optional[evaluation.AssignmentCheckSample]
    overlap: classification_loop.DynamicOverlap

    def __init__(
        self,
        task_duration_hint: timedelta,
        pricing_config: pricing.PoolPricingConfig,
        overlap: classification_loop.Overlap,
        control: control_params.Control,
        worker_filter: worker.BaseWorkerFilter,
        aggregation_algorithm: Optional[classification.AggregationAlgorithm] = None,
        pricing_strategy: pricing.PricingStrategy = pricing.StaticPricingStrategy(),
        assignment_check_sample: Optional[evaluation.AssignmentCheckSample] = None,
        task_duration_function: Optional[duration.TaskDurationFunction] = None,
        work_duration_before_vacation: Optional[timedelta] = timedelta(hours=1),
    ):
        super(AnnotationParams, self).__init__(
            task_duration_hint,
            pricing_config,
            overlap,
            control,
            worker_filter,
            aggregation_algorithm,
            pricing_strategy,
            task_duration_function,
            work_duration_before_vacation,
        )
        assert isinstance(
            self.overlap, classification_loop.DynamicOverlap
        ), 'You should use dynamic overlap for annotation'
        assert self.overlap.min_overlap == 1, 'For annotation we always start with one attempt'
        assert self.aggregation_algorithm is None, 'No aggregation algorithm should be specified for annotation params'
        self.assignment_check_sample = assignment_check_sample
        assert (
            self.pricing_config.control_tasks_count == 0
        ), 'We cannot use control tasks as measure of quality control for annotation tasks'

    @property
    def feedback_loop_params(self) -> feedback_loop.Params:
        task_duration_function = self.task_duration_function
        if task_duration_function is None:
            task_duration_function = duration.get_const_task_duration_function(self.task_duration_hint)
        return feedback_loop.Params(self.assignment_check_sample, self.control, self.overlap, task_duration_function)
