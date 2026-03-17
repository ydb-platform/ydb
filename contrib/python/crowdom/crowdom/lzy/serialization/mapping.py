from dataclasses import dataclass
from typing import List

from pure_protobuf.dataclasses_ import field, message

from ... import mapping
from .base import OptionalObject
from .common import ProtobufSerializer


@message
@dataclass
class Objects(ProtobufSerializer[mapping.Objects]):
    objects: List[OptionalObject] = field(1, default_factory=list)

    @staticmethod
    def serialize(obj: mapping.Objects) -> 'Objects':
        return Objects(objects=[OptionalObject.serialize(o) for o in obj])

    def deserialize(self) -> mapping.Objects:
        return tuple([o.deserialize() for o in self.objects])


@message
@dataclass
class TaskSingleSolution(ProtobufSerializer[mapping.TaskSingleSolution]):
    task: Objects = field(1)
    solution: Objects = field(2)

    @staticmethod
    def serialize(obj: mapping.TaskSingleSolution) -> 'TaskSingleSolution':
        task, solution = obj
        return TaskSingleSolution(task=Objects.serialize(task), solution=Objects.serialize(solution))

    def deserialize(self) -> mapping.TaskSingleSolution:
        return self.task.deserialize(), self.solution.deserialize()
