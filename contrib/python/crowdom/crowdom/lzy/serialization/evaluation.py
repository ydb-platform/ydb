from dataclasses import dataclass
from typing import Optional

from pure_protobuf.dataclasses_ import field, message

from ... import evaluation
from .classification import WorkerLabels
from .common import ProtobufSerializer


@message
@dataclass
class AssignmentCheckSample(ProtobufSerializer[evaluation.AssignmentCheckSample]):
    max_tasks_to_check: Optional[int] = field(1, default=None)
    assignment_accuracy_finalization_threshold: Optional[float] = field(2, default=None)

    @staticmethod
    def serialize(obj: evaluation.AssignmentCheckSample) -> 'AssignmentCheckSample':
        return AssignmentCheckSample(
            max_tasks_to_check=obj.max_tasks_to_check,
            assignment_accuracy_finalization_threshold=obj.assignment_accuracy_finalization_threshold,
        )

    def deserialize(self) -> evaluation.AssignmentCheckSample:
        return evaluation.AssignmentCheckSample(
            max_tasks_to_check=self.max_tasks_to_check,
            assignment_accuracy_finalization_threshold=self.assignment_accuracy_finalization_threshold,
        )


@message
@dataclass
class SolutionEvaluation(ProtobufSerializer[evaluation.SolutionEvaluation]):
    ok: bool = field(1)
    confidence: float = field(2)
    worker_labels: WorkerLabels = field(3)

    @staticmethod
    def serialize(obj: evaluation.SolutionEvaluation) -> 'SolutionEvaluation':
        return SolutionEvaluation(
            ok=obj.ok,
            confidence=obj.confidence,
            worker_labels=WorkerLabels.serialize(obj.worker_labels)
        )

    def deserialize(self) -> evaluation.SolutionEvaluation:
        return evaluation.SolutionEvaluation(
            ok=self.ok,
            confidence=self.confidence,
            worker_labels=self.worker_labels.deserialize(),
        )
