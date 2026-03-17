from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from pure_protobuf.dataclasses_ import field, message

from .classification import AggregationAlgorithm
from .classification_loop import Overlap
from .common import ProtobufSerializer, func_registry, func_registry_reversed
from .control import Control
from .evaluation import AssignmentCheckSample
from .pricing import PricingStrategy
from .worker import HumanFilter, Model
from ...pricing import PoolPricingConfig
import crowdom.params as params


@message
@dataclass
class ExpertParams(ProtobufSerializer[params.ExpertParams]):
    task_duration_hint: timedelta = field(1)
    assignment_price: float = field(2)
    real_tasks_count: int = field(3)

    @staticmethod
    def serialize(obj: params.ExpertParams) -> 'ExpertParams':
        return ExpertParams(
            task_duration_hint=obj.task_duration_hint,
            assignment_price=obj.assignment_price,
            real_tasks_count=obj.real_tasks_count,
        )

    def deserialize(self) -> params.ExpertParams:
        return params.ExpertParams(
            task_duration_hint=self.task_duration_hint,
            pricing_config=PoolPricingConfig(
                assignment_price=self.assignment_price,
                real_tasks_count=self.real_tasks_count,
                control_tasks_count=0,
            ),
        )


@message
@dataclass
class Params(ExpertParams, ProtobufSerializer[params.Params]):
    worker_filter: HumanFilter = field(100)
    control_tasks_count: int = field(101)
    pricing_strategy: PricingStrategy = field(102)
    control: Control = field(103)
    overlap: Overlap = field(104)
    aggregation_algorithm: Optional[AggregationAlgorithm] = field(105, default=None)
    model: Optional[Model] = field(106, default=None)
    task_duration_function: Optional[str] = field(107, default=None)

    @staticmethod
    def serialize(obj: params.Params) -> 'Params':
        p = ExpertParams.serialize(obj)
        return Params(
            task_duration_hint=p.task_duration_hint,
            assignment_price=p.assignment_price,
            real_tasks_count=p.real_tasks_count,
            worker_filter=HumanFilter.serialize(obj.worker_filter),
            control_tasks_count=obj.control_tasks_count,
            pricing_strategy=PricingStrategy.serialize(obj.pricing_strategy),
            control=Control.serialize(obj.control),
            overlap=Overlap.serialize(obj.overlap),
            aggregation_algorithm=AggregationAlgorithm.serialize(obj.aggregation_algorithm)
            if obj.aggregation_algorithm
            else None,
            model=Model.serialize(obj.model) if obj.model else None,
            task_duration_function=func_registry[obj.task_duration_function] if obj.task_duration_function else None,
        )

    def deserialize(self) -> params.Params:
        p = super(Params, self).deserialize()
        return params.Params(
            task_duration_hint=p.task_duration_hint,
            pricing_config=PoolPricingConfig(
                assignment_price=p.pricing_config.assignment_price,
                real_tasks_count=p.pricing_config.real_tasks_count,
                control_tasks_count=self.control_tasks_count,
            ),
            overlap=Overlap.deserialize(self.overlap),
            control=Control.deserialize(self.control),
            worker_filter=HumanFilter.deserialize(self.worker_filter),
            aggregation_algorithm=AggregationAlgorithm.deserialize(self.aggregation_algorithm)
            if self.aggregation_algorithm
            else None,
            pricing_strategy=PricingStrategy.deserialize(self.pricing_strategy),
            task_duration_function=func_registry_reversed[self.task_duration_function]
            if self.task_duration_function
            else None,
        )


@message
@dataclass
class AnnotationParams(Params, ProtobufSerializer[params.AnnotationParams]):
    assignment_check_sample: Optional[AssignmentCheckSample] = field(200, default=None)

    @staticmethod
    def serialize(obj: params.AnnotationParams) -> 'AnnotationParams':
        p = Params.serialize(obj)
        return AnnotationParams(
            task_duration_hint=p.task_duration_hint,
            assignment_price=p.assignment_price,
            real_tasks_count=p.real_tasks_count,
            worker_filter=p.worker_filter,
            control_tasks_count=p.control_tasks_count,
            pricing_strategy=p.pricing_strategy,
            control=p.control,
            overlap=p.overlap,
            aggregation_algorithm=p.aggregation_algorithm,
            model=p.model,
            task_duration_function=p.task_duration_function,
            assignment_check_sample=AssignmentCheckSample.serialize(obj.assignment_check_sample)
            if obj.assignment_check_sample
            else None,
        )

    def deserialize(self) -> params.AnnotationParams:
        p = super(AnnotationParams, self).deserialize()
        annotation_params = params.AnnotationParams(
            task_duration_hint=p.task_duration_hint,
            pricing_config=p.pricing_config,
            overlap=p.overlap,
            control=p.control,
            worker_filter=p.worker_filter,
            aggregation_algorithm=p.aggregation_algorithm,
            pricing_strategy=p.pricing_strategy,
            task_duration_function=p.task_duration_function,
            assignment_check_sample=self.assignment_check_sample.deserialize()
            if self.assignment_check_sample
            else None,
        )
        if self.model:
            annotation_params.model = Model.deserialize(self.model)
        return annotation_params
