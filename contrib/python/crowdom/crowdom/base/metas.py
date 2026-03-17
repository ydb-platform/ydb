from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Type, TypeVar, Generic, Optional, Tuple, Any, Set

import toloka.client.project.template_builder as tb

from .common import TextFormat, Title
from .conditions import Consequence, If
from .types import Label, Object, Class, Evaluation, SbSChoice, ImageAnnotation

T = TypeVar('T', covariant=True)
ObjectT = TypeVar('ObjectT', bound=Type[Object], covariant=True)
ClassT = TypeVar('ClassT', bound=Type[Class], covariant=True)
EvaluationT = TypeVar('EvaluationT', bound=Type[Evaluation], covariant=True)
SbSChoiceT = TypeVar('SbSChoiceT', bound=Type[SbSChoice], covariant=True)
ImageAnnotationT = TypeVar('ImageAnnotationT', bound=Type[ImageAnnotation], covariant=True)


@dataclass
class ObjectMeta(Generic[ObjectT]):
    type: ObjectT
    name: Optional[str] = None
    title: Optional[Title] = None
    required: bool = True

    def replace_name(self, name: str):
        copy = deepcopy(self)
        copy.name = name
        return copy


@dataclass
class AvailableLabels(Consequence):
    labels: List[Label]


@dataclass
class SingleLabel(Consequence):
    label: Label


def create_available_labels_if(name: str, value_labels: List[Tuple[Any, List[Class]]]) -> If:
    return If.generate_list_sequence(name, [(value, AvailableLabels(labels)) for value, labels in value_labels])


def create_input_label_as_text_if(name: str, labels: List[Class]) -> If:
    return If.generate_list_sequence(name, [(label.value, SingleLabel(label)) for label in labels])


class LabelsDisplayType(Enum):
    """
    Way to display label set as input or output argument.

    MULTI: for input - display as radio group button
           for output - display as disabled radio group button with chosen label being selected

    MONO: for input - display as text area with chosen label
          for output - display as dropdown list or text area with auto-suggest (not implemented)

    In any case, if label set size is big enough, it displays as for MONO.
    """

    MULTI = 'multi'
    MONO = 'mono'


@dataclass
class ClassMeta(ObjectMeta[ClassT]):
    available_labels: Optional[If] = None
    input_display_type: LabelsDisplayType = LabelsDisplayType.MULTI
    text_format: TextFormat = TextFormat.PLAIN


@dataclass
class ImageAnnotationMeta(ObjectMeta[ImageAnnotationT]):
    available_shapes: Set[tb.fields.ImageAnnotationFieldV1.Shape] = field(
        default_factory=set(tb.fields.ImageAnnotationFieldV1.Shape)
    )
    labels: Optional[Type[Class]] = None


class EvaluationMeta(ObjectMeta[EvaluationT]):
    ...


class SbSChoiceMeta(ObjectMeta[SbSChoiceT]):
    ...
