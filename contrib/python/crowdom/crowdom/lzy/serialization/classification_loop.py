from dataclasses import dataclass

from pure_protobuf.dataclasses_ import field, message, one_of, part
from pure_protobuf.oneof import OneOf_

from ... import classification_loop
from .classification import TaskLabelsProbas
from .common import ProtobufSerializer, deserialize_one_of_field


@message
@dataclass
class StaticOverlap(ProtobufSerializer[classification_loop.StaticOverlap]):
    overlap: int = field(1)

    @staticmethod
    def serialize(obj: classification_loop.StaticOverlap) -> 'StaticOverlap':
        return StaticOverlap(overlap=obj.overlap)

    def deserialize(self) -> classification_loop.StaticOverlap:
        return classification_loop.StaticOverlap(overlap=self.overlap)


@message
@dataclass
class DynamicOverlap(ProtobufSerializer[classification_loop.DynamicOverlap]):
    min_overlap: int = field(1)
    max_overlap: int = field(2)
    confidence: OneOf_ = one_of(
        unified=part(float, 3),
        per_label=part(TaskLabelsProbas, 4),
    )

    @staticmethod
    def serialize(obj: classification_loop.DynamicOverlap) -> 'DynamicOverlap':
        overlap = DynamicOverlap(min_overlap=obj.min_overlap, max_overlap=obj.max_overlap)
        if isinstance(obj.confidence, float):
            overlap.confidence.unified = obj.confidence
        elif isinstance(obj.confidence, dict):
            overlap.confidence.per_label = TaskLabelsProbas.serialize(obj.confidence)
        else:
            raise ValueError(f'unexpected confidence type: {type(obj.confidence)}')
        return overlap

    def deserialize(self) -> classification_loop.DynamicOverlap:
        return classification_loop.DynamicOverlap(
            min_overlap=self.min_overlap,
            max_overlap=self.max_overlap,
            confidence=deserialize_one_of_field(self.confidence),
        )


@message
@dataclass
class Overlap(ProtobufSerializer[classification_loop.Overlap]):
    overlap: OneOf_ = one_of(
        static=part(StaticOverlap, 1),
        dynamic=part(DynamicOverlap, 2),
    )

    @staticmethod
    def serialize(obj: classification_loop.Overlap) -> 'Overlap':
        overlap = Overlap()
        if isinstance(obj, classification_loop.StaticOverlap):
            overlap.overlap.static = StaticOverlap.serialize(obj)
        elif isinstance(obj, classification_loop.DynamicOverlap):
            overlap.overlap.dynamic = DynamicOverlap.serialize(obj)
        else:
            raise ValueError(f'unexpected overlap type: {type(obj)}')
        return overlap

    def deserialize(self) -> classification_loop.Overlap:
        return deserialize_one_of_field(self.overlap)
