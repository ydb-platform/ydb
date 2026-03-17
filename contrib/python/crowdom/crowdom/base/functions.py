import abc
from dataclasses import dataclass
import re
from typing import Tuple, Union, Type, Optional

import pandas as pd

from .common import LocalizedString, Title
from .metas import ClassMeta, EvaluationMeta, ObjectMeta, SbSChoiceMeta
from .types import BinaryEvaluation, Class, Evaluation, Object, SbSChoice, ImageAnnotation

FunctionArgument = Union[Type[Object], ObjectMeta]
FunctionArguments = Tuple[FunctionArgument, ...]

CLASS_TASK_FIELD = 'choice'
CLASS_OBJ_FIELD = 'value'
EVALUATION_TASK_FIELD = 'eval'


class TaskFunction:
    def __post_init__(self):
        self.has_names = None
        self.has_names = self.has_names_in_metas(self.get_all_arguments())
        self.additional_validate()
        self.validate(self.get_all_arguments())

    @staticmethod
    def has_names_in_metas(metas: Tuple[ObjectMeta, ...]) -> bool:
        return any(meta.name is not None for meta in metas)

    @abc.abstractmethod
    def get_inputs(self) -> Tuple[ObjectMeta, ...]:
        ...

    @abc.abstractmethod
    def get_outputs(self) -> Tuple[ObjectMeta, ...]:
        ...

    def get_all_arguments(self) -> Tuple[ObjectMeta, ...]:
        return self.get_inputs() + self.get_outputs()

    @staticmethod
    def display_arguments(metas: Tuple[ObjectMeta, ...]) -> str:
        return str(pd.DataFrame([{'type': meta.type.__name__, 'name': meta.name} for meta in metas]))

    def print_arguments(self):
        print(self.display_arguments(self.get_all_arguments()))

    def additional_validate(self):
        pass

    @staticmethod
    def validate(metas: Tuple[ObjectMeta, ...]):
        has_name = TaskFunction.has_names_in_metas(metas)
        if has_name:
            names = [meta.name for meta in metas]
            classes_repr = f'Current names:\n{TaskFunction.display_arguments(metas)}'
            assert all(name is not None for name in names), f'You should provide names for all types. {classes_repr}'
            assert len(names) == len(set(names)), f'Names should be unique. {classes_repr}'
            pattern = re.compile(r'\w{1,30}', re.ASCII)
            for name in names:
                assert not name.startswith('_'), 'Names starting with "_" are reserved for internal usage'
                result = pattern.fullmatch(name)
                assert result is not None, f'Incorrect name: "{name}"'

    @staticmethod
    def convert_to_meta(type_or_meta: FunctionArgument, name: Optional[str] = None) -> ObjectMeta:
        if isinstance(type_or_meta, ObjectMeta):
            return type_or_meta
        elif isinstance(type_or_meta, type) and issubclass(type_or_meta, Object):
            # todo name?
            return ObjectMeta[type_or_meta](type=type_or_meta, name=name)
        raise ValueError(f'Unexpected type or instance: "{type_or_meta}"')

    @staticmethod
    def convert_to_metas(types: Tuple[FunctionArgument]) -> Tuple[ObjectMeta, ...]:
        return tuple(TaskFunction.convert_to_meta(type_or_meta) for type_or_meta in types)


@dataclass
class ClassificationFunction(TaskFunction):
    inputs: FunctionArguments
    cls: Union[Type[Class], ClassMeta]

    def get_inputs(self) -> Tuple[ObjectMeta, ...]:
        return self.convert_to_metas(self.inputs)

    def get_outputs(self) -> Tuple[ObjectMeta, ...]:
        return (self.convert_to_meta(self.cls, name=CLASS_TASK_FIELD if self.has_names else None),)

    def additional_validate(self):
        assert (
            isinstance(self.cls, ClassMeta)
            and issubclass(self.cls.type, Class)
            or isinstance(self.cls, type)
            and issubclass(self.cls, Class)
        )
        assert not isinstance(self.cls, ObjectMeta) or self.cls.required, 'choice is required'


@dataclass
class SbSFunction(TaskFunction):
    inputs: FunctionArguments
    hints: Optional[FunctionArguments] = None
    choice: Union[Type[SbSChoice], SbSChoiceMeta] = SbSChoice

    def get_inputs(self) -> Tuple[ObjectMeta, ...]:
        return self.convert_to_metas(self.inputs)

    def get_outputs(self) -> Tuple[ObjectMeta, ...]:
        return (self.convert_to_meta(self.choice, name=CLASS_TASK_FIELD if self.has_names else None),)

    def get_hints(self) -> Tuple[ObjectMeta, ...]:
        return self.convert_to_metas(self.hints or ())

    def get_all_arguments(self) -> Tuple[ObjectMeta, ...]:
        return self.get_hints() + super(SbSFunction, self).get_all_arguments()

    def additional_validate(self):
        if not (isinstance(self.choice, type) and issubclass(self.choice, SbSChoice)):
            raise NotImplementedError(f'Unexpected type or instance: "{self.choice}"')
        assert not isinstance(self.choice, ObjectMeta) or self.choice.required, 'choice is required'


@dataclass
class AnnotationFunction(TaskFunction):
    inputs: FunctionArguments
    outputs: FunctionArguments
    evaluation: Type[Evaluation] = BinaryEvaluation

    def has_image_annotation(self) -> bool:
        return any(output_meta.type == ImageAnnotation for output_meta in self.get_outputs())

    def get_inputs(self) -> Tuple[ObjectMeta, ...]:
        return self.convert_to_metas(self.inputs)

    def get_outputs(self) -> Tuple[ObjectMeta, ...]:
        return self.convert_to_metas(self.outputs)

    def get_evaluation(self) -> EvaluationMeta:
        return EvaluationMeta(
            type=self.evaluation,
            name=EVALUATION_TASK_FIELD if self.has_names else None,
            title=Title(
                text=LocalizedString(
                    {
                        'EN': 'Is the solution accurate?',
                        'RU': 'Решение верное?',
                        'TR': 'Görevin kararı doğru mu?',
                        'KK': 'Тапсырманың шешімі дұрыс па?',
                    }
                )
            ),
        )

    def get_all_arguments(self) -> Tuple[ObjectMeta, ...]:
        return super(AnnotationFunction, self).get_all_arguments() + (self.get_evaluation(),)

    def additional_validate(self):
        if not (isinstance(self.evaluation, type) and issubclass(self.evaluation, Evaluation)):
            raise NotImplementedError(f'Unexpected type: "{self.evaluation}"')

    def get_evaluation_named_meta(self) -> EvaluationMeta:
        # todo: may be incorrect sometimes
        evaluation = self.get_evaluation()
        if evaluation.name is None:
            evaluation.name = EVALUATION_TASK_FIELD
        return evaluation
