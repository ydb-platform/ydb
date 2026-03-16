from dataclasses import dataclass
from typing import Optional, Type

from pure_protobuf.dataclasses_ import field, message

from ... import feedback_loop
from .base import IntEnum
from .common import ProtobufSerializer
from .evaluation import SolutionEvaluation
from .mapping import Objects
from .worker import Worker


@message
class SolutionVerdict(IntEnum[feedback_loop.SolutionVerdict]):
    @staticmethod
    def enum_cls() -> Type[feedback_loop.SolutionVerdict]:
        return feedback_loop.SolutionVerdict


@message
@dataclass
class Solution(ProtobufSerializer[feedback_loop.Solution]):
    solution: Objects = field(1)
    verdict: SolutionVerdict = field(2)
    assignment_accuracy: float = field(4)
    assignment_evaluation_recall: float = field(5)
    worker: Worker = field(6)
    evaluation: Optional[SolutionEvaluation] = field(3, default=None)

    @staticmethod
    def serialize(obj: feedback_loop.Solution) -> 'Solution':
        return Solution(
            solution=Objects.serialize(obj.solution),
            verdict=SolutionVerdict.serialize(obj.verdict),
            evaluation=SolutionEvaluation.serialize(obj.evaluation) if obj.evaluation else None,
            assignment_accuracy=obj.assignment_accuracy,
            assignment_evaluation_recall=obj.assignment_evaluation_recall,
            worker=Worker.serialize(obj.worker),
        )

    def deserialize(self) -> feedback_loop.Solution:
        return feedback_loop.Solution(
            solution=self.solution.deserialize(),
            verdict=self.verdict.deserialize(),
            evaluation=self.evaluation.deserialize() if self.evaluation else None,
            assignment_accuracy=self.assignment_accuracy,
            assignment_evaluation_recall=self.assignment_evaluation_recall,
            worker=self.worker.deserialize(),
        )
