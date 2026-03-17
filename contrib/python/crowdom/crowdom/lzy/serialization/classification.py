from dataclasses import dataclass
from typing import List, Optional, Type

from pure_protobuf.dataclasses_ import field, message

from ... import classification
from .base import Label, StrEnum
from .common import ProtobufSerializer
from .worker import Worker


@message
@dataclass
class LabelProba(ProtobufSerializer[classification.LabelProba]):
    label: Label = field(1)
    proba: float = field(2)

    @staticmethod
    def serialize(obj: classification.LabelProba) -> 'LabelProba':
        label, proba = obj
        return LabelProba(label=Label.serialize(label), proba=proba)

    def deserialize(self) -> classification.LabelProba:
        return self.label.deserialize(), self.proba


@message
@dataclass
class TaskLabelsProbas(ProtobufSerializer[classification.TaskLabelsProbas]):
    items: List[LabelProba] = field(1)

    @staticmethod
    def serialize(obj: classification.TaskLabelsProbas) -> 'TaskLabelsProbas':
        return TaskLabelsProbas(items=[LabelProba.serialize((label, proba)) for label, proba in obj.items()])

    def deserialize(self) -> classification.TaskLabelsProbas:
        return {item.label.deserialize(): item.proba for item in self.items}


@message
class AggregationAlgorithm(StrEnum[classification.AggregationAlgorithm]):
    @staticmethod
    def enum_cls() -> Type[classification.AggregationAlgorithm]:
        return classification.AggregationAlgorithm


@message
@dataclass
class WorkerLabel(ProtobufSerializer[classification.WorkerLabel]):
    label: Label = field(1)
    worker: Worker = field(2)

    @staticmethod
    def serialize(obj: classification.WorkerLabel) -> 'WorkerLabel':
        label, worker = obj
        return WorkerLabel(label=Label.serialize(label), worker=Worker.serialize(worker))

    def deserialize(self) -> classification.WorkerLabel:
        return self.label.deserialize(), self.worker.deserialize()


@message
@dataclass
class WorkerWeightsItem:
    worker_id: str = field(1)
    weight: float = field(2)


@message
@dataclass
class WorkerWeights(ProtobufSerializer[classification.WorkerWeights]):
    entries: List[WorkerWeightsItem] = field(1)

    @staticmethod
    def serialize(obj: classification.WorkerWeights) -> 'WorkerWeights':
        return WorkerWeights([WorkerWeightsItem(worker_id, weight) for worker_id, weight in obj.items()])

    def deserialize(self) -> classification.WorkerWeights:
        return {entry.worker_id: entry.weight for entry in self.entries}


@message
@dataclass
class WorkerLabels(ProtobufSerializer[List[classification.WorkerLabel]]):
    labels: List[WorkerLabel] = field(1, default_factory=list)

    @staticmethod
    def serialize(obj: List[classification.WorkerLabel]) -> 'WorkerLabels':
        return WorkerLabels([WorkerLabel.serialize(label) for label in obj])

    def deserialize(self) -> List[classification.WorkerLabel]:
        return [label.deserialize() for label in self.labels]


worker_labels_empty = WorkerLabels(labels=[])


@message
@dataclass
class ResultsItem:
    labels: WorkerLabels = field(2)
    probas: Optional[TaskLabelsProbas] = field(1, default=None)


@message
@dataclass
class Results(ProtobufSerializer[classification.Results]):
    items: List[ResultsItem] = field(1)

    @staticmethod
    def serialize(obj: classification.Results) -> 'Results':
        return Results(
            [
                ResultsItem(
                    probas=TaskLabelsProbas.serialize(probas) if probas else None,
                    labels=WorkerLabels.serialize(labels),
                )
                for probas, labels in obj
            ]
        )

    def deserialize(self) -> classification.Results:
        return [
            (item.probas.deserialize() if item.probas else None, item.labels.deserialize())
            for item in self.items
        ]
