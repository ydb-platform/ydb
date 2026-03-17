import abc
from dataclasses import dataclass
from enum import Enum
from typing import Generic, List, Optional, Type, Union

from pure_protobuf.dataclasses_ import field, message, one_of, part
from pure_protobuf.oneof import OneOf_

from ... import base, objects
from .common import ClassT, ProtobufSerializer, types_registry, types_registry_reversed, deserialize_one_of_field


@message
@dataclass
class LocalizedStringItem:
    lang: str = field(1)
    text: str = field(2)


@message
@dataclass
class LocalizedString(ProtobufSerializer[base.LocalizedString]):
    items: List[LocalizedStringItem] = field(1)

    @staticmethod
    def serialize(obj: base.LocalizedString) -> 'LocalizedString':
        return LocalizedString([LocalizedStringItem(lang, text) for lang, text in obj.lang_to_text.items()])

    def deserialize(self) -> base.LocalizedString:
        return base.LocalizedString(lang_to_text={entry.lang: entry.text for entry in self.items})


# For library string enumerations
@message
@dataclass(frozen=True)
class StrEnum(ProtobufSerializer[Enum], Generic[ClassT]):
    value: str = field(1)

    @staticmethod
    @abc.abstractmethod
    def enum_cls() -> Type[Enum]:
        ...

    @classmethod
    def serialize(cls, obj: Enum) -> 'StrEnum':
        return cls(value=obj.value)

    def deserialize(self) -> Enum:
        return self.enum_cls()(self.value)


# For library int enumerations
@message
@dataclass(frozen=True)
class IntEnum(ProtobufSerializer[Enum], Generic[ClassT]):
    value: int = field(1)

    @staticmethod
    @abc.abstractmethod
    def enum_cls() -> Type[Enum]:
        ...

    @classmethod
    def serialize(cls, obj: Enum) -> 'IntEnum':
        return cls(value=obj.value)

    def deserialize(self) -> Enum:
        return self.enum_cls()(self.value)


@message
class TextFormat(StrEnum[base.TextFormat]):
    @staticmethod
    def enum_cls() -> Type[base.TextFormat]:
        return base.TextFormat


@message
@dataclass
class Title(ProtobufSerializer[base.Title]):
    text: LocalizedString = field(1)
    format: TextFormat = field(2)

    @staticmethod
    def serialize(obj: base.Title) -> 'Title':
        return Title(text=LocalizedString.serialize(obj.text), format=TextFormat.serialize(obj.format))

    def deserialize(self) -> base.Title:
        return base.Title(text=self.text.deserialize(), format=self.format.deserialize())


@message
class SbSChoice(StrEnum[base.SbSChoice]):
    @staticmethod
    def enum_cls() -> Type[base.SbSChoice]:
        return base.SbSChoice


@message
@dataclass
class BinaryEvaluation(ProtobufSerializer[base.BinaryEvaluation]):
    ok: bool = field(1)

    @staticmethod
    def serialize(obj: base.BinaryEvaluation) -> 'BinaryEvaluation':
        return BinaryEvaluation(obj.ok)

    def deserialize(self) -> base.BinaryEvaluation:
        return base.BinaryEvaluation(self.ok)


@message
@dataclass
class Metadata(ProtobufSerializer[base.Metadata]):
    metadata: str = field(1)

    @staticmethod
    def serialize(obj: base.Metadata) -> 'Metadata':
        return Metadata(obj.metadata)

    def deserialize(self) -> base.Metadata:
        return base.Metadata(self.metadata)


@message
@dataclass
class Text(ProtobufSerializer[objects.Text]):
    text: str = field(1)

    @staticmethod
    def serialize(obj: objects.Text) -> 'Text':
        return Text(obj.text)

    def deserialize(self) -> objects.Text:
        return objects.Text(self.text)


@message
@dataclass
class Audio(ProtobufSerializer[objects.Audio]):
    url: str = field(1)

    @staticmethod
    def serialize(obj: objects.Audio) -> 'Audio':
        return Audio(obj.url)

    def deserialize(self) -> objects.Audio:
        return objects.Audio(self.url)


@message
@dataclass
class Image(ProtobufSerializer[objects.Image]):
    url: str = field(1)

    @staticmethod
    def serialize(obj: objects.Image) -> 'Image':
        return Image(obj.url)

    def deserialize(self) -> objects.Image:
        return objects.Image(self.url)


@message
@dataclass
class Video(ProtobufSerializer[objects.Video]):
    url: str = field(1)

    @staticmethod
    def serialize(obj: objects.Video) -> 'Video':
        return Video(obj.url)

    def deserialize(self) -> objects.Video:
        return objects.Video(self.url)


# For user-defined string enumerations, including Class inheritors
@message
@dataclass
class Class(ProtobufSerializer[base.Class]):
    value: str = field(1)
    cls: str = field(2)

    @staticmethod
    def serialize(obj: base.Class) -> 'Class':
        cls = types_registry[type(obj)]
        return Class(value=obj.value, cls=cls)

    def deserialize(self) -> base.Class:
        cls = types_registry_reversed[self.cls]
        return cls(self.value)


@message
@dataclass
class Label(ProtobufSerializer[base.Label]):
    label: OneOf_ = one_of(
        cls=part(Class, 1),
        sbs_choice=part(SbSChoice, 2),
        binary_evaluation=part(BinaryEvaluation, 3),
    )

    @staticmethod
    def serialize(obj: base.Label) -> 'Label':
        label = Label()
        if isinstance(obj, base.BinaryEvaluation):
            label.label.binary_evaluation = BinaryEvaluation.serialize(obj)
        elif isinstance(obj, base.SbSChoice):
            label.label.sbs_choice = SbSChoice.serialize(obj)
        elif isinstance(obj, base.Class):
            label.label.cls = Class.serialize(obj)
        else:
            assert False, f'unexpected Label type: {type(obj)}'
        return label

    def deserialize(self) -> base.Label:
        return deserialize_one_of_field(self.label)


@message
@dataclass
class Object(ProtobufSerializer[base.Object]):
    obj: OneOf_ = one_of(
        cls=part(Class, 1),
        sbs_choice=part(SbSChoice, 2),
        binary_evaluation=part(BinaryEvaluation, 3),
        metadata=part(Metadata, 4),
        text=part(Text, 5),
        audio=part(Audio, 6),
        image=part(Image, 7),
        video=part(Video, 8),
    )

    @staticmethod
    def serialize(obj: base.Object) -> 'Object':
        obj_ = Object()
        if isinstance(obj, base.BinaryEvaluation):
            obj_.obj.binary_evaluation = BinaryEvaluation.serialize(obj)
        elif isinstance(obj, base.SbSChoice):
            obj_.obj.sbs_choice = SbSChoice.serialize(obj)
        elif isinstance(obj, base.Class):
            obj_.obj.cls = Class.serialize(obj)
        elif isinstance(obj, base.Metadata):
            obj_.obj.metadata = Metadata.serialize(obj)
        elif isinstance(obj, objects.Text):
            obj_.obj.text = Text.serialize(obj)
        elif isinstance(obj, objects.Audio):
            obj_.obj.audio = Audio.serialize(obj)
        elif isinstance(obj, objects.Image):
            obj_.obj.image = Image.serialize(obj)
        elif isinstance(obj, objects.Video):
            obj_.obj.video = Video.serialize(obj)
        else:
            assert False, f'unexpected Object type: {type(obj)}'
        return obj_

    def deserialize(self) -> base.Object:
        return deserialize_one_of_field(self.obj)


# We could leave Object's one_of field unfilled, it would also make it None.
# But, we can't make things like List[Optional[Object]] in other serializer classes,
# so we have to create wrapper for "object or None".
@message
@dataclass
class OptionalObject(ProtobufSerializer[Optional[base.Object]]):
    obj: Optional[Object] = field(1, default=None)

    @staticmethod
    def serialize(obj: Optional[base.Object]) -> 'OptionalObject':
        return OptionalObject(obj=Object.serialize(obj) if obj else None)

    def deserialize(self) -> Optional[base.Object]:
        return self.obj.deserialize() if self.obj else None


@message
@dataclass
class ObjectMeta(ProtobufSerializer[base.ObjectMeta]):
    type: str = field(1)
    name: Optional[str] = field(2, default=None)
    title: Optional[Title] = field(3, default=None)
    required: bool = field(4, default=True)

    @staticmethod
    def serialize(obj: base.ObjectMeta) -> 'ObjectMeta':
        return ObjectMeta(
            type=types_registry[obj.type],
            name=obj.name,
            title=Title.serialize(obj.title) if obj.title else None,
            required=obj.required,
        )

    def deserialize(self) -> base.ObjectMeta:
        return base.ObjectMeta(
            type=types_registry_reversed[self.type],
            name=self.name,
            title=self.title.deserialize() if self.title else None,
            required=self.required,
        )


@message
@dataclass
class AvailableLabels(ProtobufSerializer[base.AvailableLabels]):
    labels: List[Label] = field(1)

    @staticmethod
    def serialize(obj: base.AvailableLabels) -> 'AvailableLabels':
        return AvailableLabels([Label.serialize(label) for label in obj.labels])

    def deserialize(self) -> base.AvailableLabels:
        return base.AvailableLabels([label.deserialize() for label in self.labels])


@message
@dataclass
class Consequence(ProtobufSerializer[base.Consequence]):
    consequence: OneOf_ = one_of(
        available_labels=part(AvailableLabels, 1),
    )

    @staticmethod
    def serialize(obj: base.Consequence) -> 'Consequence':
        consequence = Consequence()
        if isinstance(obj, base.AvailableLabels):
            consequence.consequence.available_labels = AvailableLabels.serialize(obj)
        else:
            raise ValueError(f'unexpected consequence type: {type(obj)}')
        return consequence

    def deserialize(self) -> base.Consequence:
        return deserialize_one_of_field(self.consequence)


@message
@dataclass
class ConditionEquals(ProtobufSerializer[base.ConditionEquals]):
    what: str = field(1)
    to: OneOf_ = one_of(
        str=part(str, 2),
    )

    @staticmethod
    def serialize(obj: base.ConditionEquals) -> 'ConditionEquals':
        condition = ConditionEquals(what=obj.what)
        if isinstance(obj.to, str):
            condition.to.str = obj.to
        else:
            raise ValueError(f'unsupported equals to type: {type(obj.to)}')
        return condition

    def deserialize(self) -> base.ConditionEquals:
        return base.ConditionEquals(what=self.what, to=deserialize_one_of_field(self.to))


@message
@dataclass
class Condition(ProtobufSerializer[base.Condition]):
    condition: OneOf_ = one_of(
        equals=part(ConditionEquals, 1),
    )

    @staticmethod
    def serialize(obj: base.Condition) -> 'Condition':
        condition = Condition()
        if isinstance(obj, base.ConditionEquals):
            condition.condition.equals = ConditionEquals.serialize(obj)
        else:
            raise ValueError(f'unexpected condition type: {type(obj)}')
        return condition

    def deserialize(self) -> base.Condition:
        return deserialize_one_of_field(self.condition)


# then/else branches are originally unions, better option to pack them to one_of, but it's not possible because union
# contains If itself and we can't pass class to one_of part using quotation syntax like 'If'
@dataclass
class If(ProtobufSerializer[base.If]):
    condition: Condition = field(1)
    then_if: Optional['If'] = field(2, default=None)
    then_consequence: Optional[Consequence] = field(3, default=None)
    else_if: Optional['If'] = field(4, default=None)
    else_consequence: Optional[Consequence] = field(5, default=None)

    def __post_init__(self):
        assert (self.then_if is not None) ^ (self.then_consequence is not None)
        assert not (self.else_if is not None and self.else_consequence is not None)

    @staticmethod
    def serialize(obj: base.If) -> 'If':
        return If(
            condition=Condition.serialize(obj.condition),
            then_if=If.serialize(obj.then) if obj.then and isinstance(obj.then, base.If) else None,
            then_consequence=Consequence.serialize(obj.then)
            if obj.then and isinstance(obj.then, base.Consequence)
            else None,
            else_if=If.serialize(obj.else_) if obj.else_ and isinstance(obj.else_, base.If) else None,
            else_consequence=Consequence.serialize(obj.else_)
            if obj.else_ and isinstance(obj.else_, base.Consequence)
            else None,
        )

    def deserialize(self) -> base.If:
        then = self.then_if.deserialize() if self.then_if else self.then_consequence.deserialize()
        else_ = None
        if self.else_if:
            else_ = self.else_if.deserialize()
        if self.else_consequence:
            else_ = self.else_consequence.deserialize()
        return base.If(condition=self.condition.deserialize(), then=then, else_=else_)


# self-referencing classes are decorated with @message separately, see https://github.com/eigenein/protobuf/issues/96
If = message(If)


@message
class LabelsDisplayType(StrEnum[base.LabelsDisplayType]):
    @staticmethod
    def enum_cls() -> Type[base.LabelsDisplayType]:
        return base.LabelsDisplayType


@message
@dataclass
class ClassMeta(ObjectMeta, ProtobufSerializer[base.ClassMeta]):
    available_labels: Optional[If] = field(100, default=None)
    input_display_type: LabelsDisplayType = field(101, default=LabelsDisplayType(base.LabelsDisplayType.MULTI))
    text_format: TextFormat = field(102, default=TextFormat(base.TextFormat.PLAIN))

    @staticmethod
    def serialize(obj: base.ClassMeta) -> 'ClassMeta':
        obj_meta = ObjectMeta.serialize(obj)
        return ClassMeta(
            type=obj_meta.type,
            name=obj_meta.name,
            title=obj_meta.title,
            required=obj_meta.required,
            available_labels=If.serialize(obj.available_labels) if obj.available_labels else None,
            input_display_type=LabelsDisplayType.serialize(obj.input_display_type),
            text_format=TextFormat.serialize(obj.text_format),
        )

    def deserialize(self) -> base.ClassMeta:
        obj_meta = super(ClassMeta, self).deserialize()
        return base.ClassMeta(
            type=obj_meta.type,
            name=obj_meta.name,
            title=obj_meta.title,
            required=obj_meta.required,
            available_labels=self.available_labels.deserialize() if self.available_labels else None,
            input_display_type=self.input_display_type.deserialize(),
            text_format=self.text_format.deserialize(),
        )


@message
@dataclass
class TextValidation(ProtobufSerializer[objects.TextValidation]):
    regex: LocalizedString = field(1)
    hint: LocalizedString = field(2)

    @staticmethod
    def serialize(obj: objects.TextValidation) -> 'TextValidation':
        return TextValidation(
            regex=LocalizedString.serialize(obj.regex),
            hint=LocalizedString.serialize(obj.hint),
        )

    def deserialize(self) -> objects.TextValidation:
        return objects.TextValidation(
            regex=self.regex.deserialize(),
            hint=self.hint.deserialize(),
        )


@message
@dataclass
class TextMeta(ObjectMeta, ProtobufSerializer[objects.TextMeta]):
    format: TextFormat = field(100, default=TextFormat(base.TextFormat.PLAIN))
    validation: Optional[TextValidation] = field(101, default=None)

    @staticmethod
    def serialize(obj: objects.TextMeta) -> 'TextMeta':
        obj_meta = ObjectMeta.serialize(obj)
        return TextMeta(
            type=obj_meta.type,
            name=obj_meta.name,
            title=obj_meta.title,
            required=obj_meta.required,
            format=TextFormat.serialize(obj.format),
            validation=TextValidation.serialize(obj.validation) if obj.validation else None,
        )

    def deserialize(self) -> objects.TextMeta:
        obj_meta = super(TextMeta, self).deserialize()
        return objects.TextMeta(
            type=obj_meta.type,
            name=obj_meta.name,
            title=obj_meta.title,
            required=obj_meta.required,
            format=self.format.deserialize(),
            validation=self.validation.deserialize() if self.validation else None,
        )


@message
@dataclass
class ObjectMetaT(ProtobufSerializer[base.ObjectMeta]):
    meta: OneOf_ = one_of(
        object=part(ObjectMeta, 1),
        cls=part(ClassMeta, 2),
        text=part(TextMeta, 3),
    )

    @staticmethod
    def serialize(obj: base.ObjectMeta) -> 'ObjectMetaT':
        meta = ObjectMetaT()
        if isinstance(obj, base.ClassMeta):
            meta.meta.cls = ClassMeta.serialize(obj)
        elif isinstance(obj, objects.TextMeta):
            meta.meta.text = TextMeta.serialize(obj)
        else:
            meta.meta.object = ObjectMeta.serialize(obj)
        return meta

    def deserialize(self) -> base.ObjectMeta:
        return deserialize_one_of_field(self.meta)


@message
@dataclass(frozen=True)
class FunctionArgument(ProtobufSerializer[Union[Type[base.Object], base.ObjectMeta]]):
    arg: OneOf_ = one_of(
        type=part(str, 1),
        meta=part(ObjectMetaT, 2),
    )

    @staticmethod
    def serialize(obj: Union[Type[base.Object], base.ObjectMeta]) -> 'FunctionArgument':
        arg = FunctionArgument()
        if isinstance(obj, base.ObjectMeta):
            arg.arg.meta = ObjectMetaT.serialize(obj)
        else:
            arg.arg.type = types_registry[obj]
        return arg

    def deserialize(self) -> Union[Type[base.Object], base.ObjectMeta]:
        which_one = self.arg.which_one_of
        if which_one == 'type':
            return types_registry_reversed[self.arg.type]
        else:
            return self.arg.meta.deserialize()


@message
@dataclass
class ClassificationFunction(ProtobufSerializer[base.ClassificationFunction]):
    inputs: List[FunctionArgument] = field(1)
    cls: FunctionArgument = field(2)

    @staticmethod
    def serialize(obj: base.ClassificationFunction) -> 'ClassificationFunction':
        return ClassificationFunction(
            inputs=[FunctionArgument.serialize(input) for input in obj.inputs],
            cls=FunctionArgument.serialize(obj.cls),
        )

    def deserialize(self) -> base.ClassificationFunction:
        return base.ClassificationFunction(
            inputs=tuple(FunctionArgument.deserialize(input) for input in self.inputs),
            cls=FunctionArgument.deserialize(self.cls),
        )


sbs_choice = FunctionArgument()
sbs_choice.arg.type = types_registry[base.SbSChoice]


@message
@dataclass
class SbSFunction(ProtobufSerializer[base.SbSFunction]):
    inputs: List[FunctionArgument] = field(1)
    hints: List[FunctionArgument] = field(2, default_factory=list)
    choice: FunctionArgument = field(3, default=sbs_choice)

    @staticmethod
    def serialize(obj: base.SbSFunction) -> 'SbSFunction':
        return SbSFunction(
            inputs=[FunctionArgument.serialize(input) for input in obj.inputs],
            hints=[FunctionArgument.serialize(hint) for hint in obj.hints] if obj.hints else [],
            choice=FunctionArgument.serialize(obj.choice),
        )

    def deserialize(self) -> base.SbSFunction:
        return base.SbSFunction(
            inputs=tuple(FunctionArgument.deserialize(input) for input in self.inputs),
            hints=tuple(FunctionArgument.deserialize(hint) for hint in self.hints) if self.hints else None,
            choice=FunctionArgument.deserialize(self.choice),
        )


@message
@dataclass
class AnnotationFunction(ProtobufSerializer[base.AnnotationFunction]):
    inputs: List[FunctionArgument] = field(1)
    outputs: List[FunctionArgument] = field(2)
    evaluation: FunctionArgument = field(3)

    @staticmethod
    def serialize(obj: base.AnnotationFunction) -> 'AnnotationFunction':
        return AnnotationFunction(
            inputs=[FunctionArgument.serialize(input) for input in obj.inputs],
            outputs=[FunctionArgument.serialize(output) for output in obj.outputs],
            evaluation=FunctionArgument.serialize(obj.evaluation),
        )

    def deserialize(self) -> base.AnnotationFunction:
        return base.AnnotationFunction(
            inputs=tuple(FunctionArgument.deserialize(input) for input in self.inputs),
            outputs=tuple(FunctionArgument.deserialize(output) for output in self.outputs),
            evaluation=FunctionArgument.deserialize(self.evaluation),
        )


@message
@dataclass
class TaskFunction(ProtobufSerializer[base.TaskFunction]):
    function: OneOf_ = one_of(
        classification=part(ClassificationFunction, 1),
        sbs=part(SbSFunction, 2),
        annotation=part(AnnotationFunction, 3),
    )

    @staticmethod
    def serialize(obj: base.TaskFunction) -> 'TaskFunction':
        function = TaskFunction()
        if isinstance(obj, base.ClassificationFunction):
            function.function.classification = ClassificationFunction.serialize(obj)
        elif isinstance(obj, base.SbSFunction):
            function.function.sbs = SbSFunction.serialize(obj)
        elif isinstance(obj, base.AnnotationFunction):
            function.function.annotation = AnnotationFunction.serialize(obj)
        else:
            raise ValueError(f'unexpected function {obj}')
        return function

    def deserialize(self) -> base.TaskFunction:
        return deserialize_one_of_field(self.function)


@message
@dataclass
class TaskSpec(ProtobufSerializer[base.TaskSpec]):
    id: str = field(1)
    function: TaskFunction = field(2)
    name: LocalizedString = field(3)
    description: LocalizedString = field(4)
    instruction: LocalizedString = field(5)

    @staticmethod
    def serialize(obj: base.TaskSpec) -> 'TaskSpec':
        return TaskSpec(
            id=obj.id,
            function=TaskFunction.serialize(obj.function),
            name=LocalizedString.serialize(obj.name),
            description=LocalizedString.serialize(obj.description),
            instruction=LocalizedString.serialize(obj.instruction),
        )

    def deserialize(self) -> base.TaskSpec:
        return base.TaskSpec(
            id=self.id,
            function=self.function.deserialize(),
            name=self.name.deserialize(),
            description=self.description.deserialize(),
            instruction=self.instruction.deserialize(),
        )
