from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Type, Union

from pure_protobuf.dataclasses_ import field, message, one_of, part
from pure_protobuf.oneof import OneOf_
import toloka.client as toloka

from ... import control
from .base import StrEnum
from .common import ProtobufSerializer, deserialize_one_of_field


@message
class ComparisonType(StrEnum[control.ComparisonType]):
    @staticmethod
    def enum_cls() -> Type[control.ComparisonType]:
        return control.ComparisonType


@message
@dataclass
class PredicateValue(ProtobufSerializer[Union[float, timedelta]]):
    value: OneOf_ = one_of(
        float_=part(float, 1),
        timedelta=part(timedelta, 2),
    )

    @staticmethod
    def serialize(obj: Union[float, timedelta]) -> 'PredicateValue':
        value = PredicateValue()
        if isinstance(obj, timedelta):
            value.value.timedelta = obj
        else:
            value.value.float_ = obj
        return value

    def deserialize(self) -> Union[float, timedelta]:
        return deserialize_one_of_field(self.value)


@message
@dataclass
class ThresholdComparisonPredicate(ProtobufSerializer[control.ThresholdComparisonPredicate]):
    threshold: PredicateValue = field(1)
    comparison: ComparisonType = field(2)

    @staticmethod
    def serialize(obj: control.ThresholdComparisonPredicate) -> 'ThresholdComparisonPredicate':
        return ThresholdComparisonPredicate(
            threshold=PredicateValue.serialize(obj.threshold),
            comparison=ComparisonType.serialize(obj.comparison),
        )

    def deserialize(self) -> control.ThresholdComparisonPredicate:
        return control.ThresholdComparisonPredicate(
            threshold=self.threshold.deserialize(),
            comparison=self.comparison.deserialize(),
        )


@message
class BooleanOperator(StrEnum[control.BooleanOperator]):
    @staticmethod
    def enum_cls() -> Type[control.BooleanOperator]:
        return control.BooleanOperator


# better option is to pack predicate types to one_of, but it's not possible because expression predicate
# contains list of Predicates itself, but we can't pass class to one_of part using quotation syntax like 'Predicate'
@dataclass
class Predicate(ProtobufSerializer[control.Predicate]):
    assignment_accuracy: Optional[ThresholdComparisonPredicate] = field(1, default=None)
    assignment_duration: Optional[ThresholdComparisonPredicate] = field(2, default=None)
    always_true: bool = field(3, default=False)
    expression_boolean_operator: Optional[BooleanOperator] = field(4, default=None)
    expression_predicates: List['Predicate'] = field(5, default_factory=list)

    @staticmethod
    def serialize(obj: control.Predicate) -> 'Predicate':
        predicate = Predicate()
        if isinstance(obj, control.AssignmentAccuracyPredicate):
            predicate.assignment_accuracy = ThresholdComparisonPredicate.serialize(obj)
        elif isinstance(obj, control.AssignmentDurationPredicate):
            predicate.assignment_duration = ThresholdComparisonPredicate.serialize(obj)
        elif isinstance(obj, control.AlwaysTruePredicate):
            predicate.always_true = True
        elif isinstance(obj, control.PredicateExpression):
            predicate.expression_boolean_operator = BooleanOperator.serialize(obj.boolean_operator)
            predicate.expression_predicates = [Predicate.serialize(p) for p in obj.predicates]
        else:
            raise ValueError(f'unexpected predicate type: {type(obj)}')
        return predicate

    def deserialize(self) -> control.Predicate:
        threshold_proto = self.assignment_accuracy or self.assignment_duration
        if threshold_proto:
            threshold = ThresholdComparisonPredicate.deserialize(threshold_proto)
            type = (
                control.AssignmentAccuracyPredicate if self.assignment_accuracy else control.AssignmentDurationPredicate
            )
            return type(threshold=threshold.threshold, comparison=threshold.comparison)

        if self.always_true:
            return control.AlwaysTruePredicate()

        assert self.expression_boolean_operator and self.expression_predicates

        return control.PredicateExpression(
            boolean_operator=BooleanOperator.deserialize(self.expression_boolean_operator),
            predicates=[Predicate.deserialize(p) for p in self.expression_predicates],
        )


Predicate = message(Predicate)


@message
@dataclass
class BlockUser(ProtobufSerializer[control.BlockUser]):
    scope: str = field(1)
    private_comment: str = field(2)
    duration: Optional[timedelta] = field(3, default=None)

    @staticmethod
    def serialize(obj: control.BlockUser) -> 'BlockUser':
        return BlockUser(
            scope=obj.scope.value,
            private_comment=obj.private_comment,
            duration=obj.duration,
        )

    def deserialize(self) -> control.BlockUser:
        return control.BlockUser(
            scope=toloka.user_restriction.UserRestriction.Scope(self.scope),
            private_comment=self.private_comment,
            duration=self.duration,
        )


@message
@dataclass
class GiveBonusToUser(ProtobufSerializer[control.GiveBonusToUser]):
    amount_usd: float = field(1)

    @staticmethod
    def serialize(obj: control.GiveBonusToUser) -> 'GiveBonusToUser':
        return GiveBonusToUser(amount_usd=obj.amount_usd)

    def deserialize(self) -> control.GiveBonusToUser:
        return control.GiveBonusToUser(amount_usd=self.amount_usd)


@message
@dataclass
class SetAssignmentStatus(ProtobufSerializer[control.SetAssignmentStatus]):
    status: str = field(1)

    @staticmethod
    def serialize(obj: control.SetAssignmentStatus) -> 'SetAssignmentStatus':
        return SetAssignmentStatus(status=obj.status.value)

    def deserialize(self) -> control.SetAssignmentStatus:
        return control.SetAssignmentStatus(status=toloka.Assignment.Status(self.status))


@message
@dataclass
class Action(ProtobufSerializer[control.Action]):
    action: OneOf_ = one_of(
        block_user=part(BlockUser, 1),
        give_bonus_to_user=part(GiveBonusToUser, 2),
        set_assignment_status=part(SetAssignmentStatus, 3),
    )

    @staticmethod
    def serialize(obj: control.Action) -> 'Action':
        action = Action()
        if isinstance(obj, control.BlockUser):
            action.action.block_user = BlockUser.serialize(obj)
        elif isinstance(obj, control.GiveBonusToUser):
            action.action.give_bonus_to_user = GiveBonusToUser.serialize(obj)
        elif isinstance(obj, control.SetAssignmentStatus):
            action.action.set_assignment_status = SetAssignmentStatus.serialize(obj)
        else:
            raise ValueError(f'unexpected action type: {type(obj)}')
        return action

    def deserialize(self) -> control.Action:
        return deserialize_one_of_field(self.action)


@message
@dataclass
class Rule(ProtobufSerializer[control.Rule]):
    predicate: Predicate = field(1)
    action: Action = field(2)

    @staticmethod
    def serialize(obj: control.Rule) -> 'Rule':
        return Rule(predicate=Predicate.serialize(obj.predicate), action=Action.serialize(obj.action))

    def deserialize(self) -> control.Rule:
        return control.Rule(predicate=self.predicate.deserialize(), action=self.action.deserialize())


@message
@dataclass
class Control(ProtobufSerializer[control.Control]):
    rules: List[Rule] = field(1, default_factory=list)

    @staticmethod
    def serialize(obj: control.Control) -> 'Control':
        return Control([Rule.serialize(rule) for rule in obj.rules])

    def deserialize(self) -> control.Control:
        return control.Control([rule.deserialize() for rule in self.rules])
