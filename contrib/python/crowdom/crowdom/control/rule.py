import abc
from dataclasses import dataclass
import datetime
from decimal import Decimal
import enum
from typing import List, Union, Optional, Dict, Any


import toloka.client as toloka
from toloka.client import TolokaClient
from toloka.client.user_restriction import (
    UserRestriction,
    PoolUserRestriction,
    ProjectUserRestriction,
    AllProjectsUserRestriction,
)
from toloka.client.assignment import Assignment, AssignmentPatch
from toloka.client.user_bonus import UserBonus


class Predicate:
    @abc.abstractmethod
    def check(self, **kwargs) -> bool:
        ...


class ComparisonType(enum.Enum):
    GREATER_OR_EQUAL = '>='
    GREATER = '>'
    LESS_OR_EQUAL = '<='
    LESS = '<'


PredicateValue = Union[float, datetime.timedelta]


@dataclass
class ThresholdComparisonPredicate(Predicate):
    threshold: PredicateValue
    comparison: ComparisonType

    def check(self, value: PredicateValue, **kwargs) -> bool:
        threshold = self.get_threshold(**kwargs)
        return {
            ComparisonType.GREATER_OR_EQUAL: lambda: value >= threshold,
            ComparisonType.GREATER: lambda: value > threshold,
            ComparisonType.LESS_OR_EQUAL: lambda: value <= threshold,
            ComparisonType.LESS: lambda: value < threshold,
        }[self.comparison]()

    def get_threshold(self, **kwargs) -> PredicateValue:
        return self.threshold


@dataclass
class AssignmentAccuracyPredicate(ThresholdComparisonPredicate):
    ...


@dataclass
class AssignmentDurationPredicate(ThresholdComparisonPredicate):
    def get_threshold(self, assignment_duration_hint: datetime.timedelta, **kwargs) -> datetime.timedelta:
        return self.threshold * assignment_duration_hint


@dataclass
class AlwaysTruePredicate(Predicate):
    def check(self, **kwargs) -> bool:
        return True


class BooleanOperator(enum.Enum):
    AND = 'and'
    OR = 'or'


@dataclass
class PredicateExpression(Predicate):
    boolean_operator: BooleanOperator
    predicates: List[Predicate]

    def __post_init__(self):
        assert self.predicates, 'List of predicates should be non-empty'
        predicate_type = type(self.predicates[0])
        assert not issubclass(predicate_type, PredicateExpression), 'Nested expressions are not allowed'
        for predicate in self.predicates:
            assert isinstance(predicate, predicate_type), (
                'All predicates should be of same type. ' f'Expected {predicate_type}, got {type(predicate)}'
            )

    def check(self, **kwargs) -> bool:
        if self.boolean_operator == BooleanOperator.AND:
            return all(predicate.check(**kwargs) for predicate in self.predicates)
        elif self.boolean_operator == BooleanOperator.OR:
            return any(predicate.check(**kwargs) for predicate in self.predicates)
        else:
            raise ValueError(f'unsupported boolean operator for predicates: {self.boolean_operator}')


class Action:
    @abc.abstractmethod
    def perform(self, **kwargs):
        ...


@dataclass
class BlockUser(Action):
    scope: UserRestriction.Scope
    private_comment: str
    duration: Optional[datetime.timedelta] = None

    def perform(
        self,
        client: TolokaClient,
        user_id: str,
        assignment_status: toloka.Assignment.Status,
        block_start: Optional[datetime.datetime] = None,
        pool_id: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs,
    ) -> UserRestriction:
        params = {'user_id': user_id, 'private_comment': self.private_comment}
        user_restriction: UserRestriction
        if self.duration:
            start = block_start or datetime.datetime.now()
            will_expire = start + self.duration
            params['will_expire'] = will_expire
        if self.scope == UserRestriction.Scope.POOL:
            user_restriction = PoolUserRestriction(pool_id=pool_id, **params)
        elif self.scope == UserRestriction.Scope.PROJECT:
            user_restriction = ProjectUserRestriction(project_id=project_id, **params)
        elif self.scope == UserRestriction.Scope.ALL:
            user_restriction = AllProjectsUserRestriction(**params)
        else:
            raise ValueError(f'unsupported scope for block: {self.scope}')
        if (
            assignment_status != toloka.Assignment.Status.ACCEPTED
            and assignment_status != toloka.Assignment.Status.REJECTED
        ):
            client.set_user_restriction(user_restriction=user_restriction)
        return user_restriction


@dataclass
class GiveBonusToUser(Action):
    amount_usd: float

    def perform(self, user_id: str, assignment_id: str, **kwargs) -> UserBonus:
        return UserBonus(
            user_id=user_id, amount=Decimal(self.amount_usd), assignment_id=assignment_id, without_message=True
        )  # we may want to send the message here


@dataclass
class SetAssignmentStatus(Action):
    status: Assignment.Status

    def perform(
        self,
        client: TolokaClient,
        assignment: toloka.Assignment,
        public_comment: str,
        **kwargs,
    ) -> Assignment.Status:
        if not assignment.id:
            # model workers have stub assignment
            return self.status
        if (
            assignment.status == toloka.Assignment.Status.REJECTED
            or assignment.status == toloka.Assignment.Status.ACCEPTED
        ):
            return assignment.status
        comment = public_comment if self.status == Assignment.REJECTED else ''
        client.patch_assignment(
            assignment_id=assignment.id, patch=AssignmentPatch(public_comment=comment, status=self.status)
        )
        return self.status


@dataclass
class Rule:
    predicate: Predicate
    action: Action

    def apply(self, **kwargs):
        if self.predicate.check(**kwargs):
            return self.action.perform(**kwargs)


# List of all rules for pool
@dataclass
class Control:
    rules: List[Rule]

    def filter_rules(self, predicate_type: type, action_type: type) -> List[Rule]:
        assert issubclass(predicate_type, Predicate)
        assert issubclass(action_type, Action)
        assert not issubclass(predicate_type, PredicateExpression), 'You should filter by inner predicate type'

        def is_suitable(predicate: Predicate) -> bool:
            if not isinstance(predicate, PredicateExpression):
                return isinstance(predicate, predicate_type)
            return all(isinstance(inner, predicate_type) for inner in predicate.predicates)

        return [rule for rule in self.rules if is_suitable(rule.predicate) and isinstance(rule.action, action_type)]

    def to_toloka_quality_control(
        self, control_tasks_count: int, assignment_duration_hint: datetime.timedelta
    ) -> List[Dict[str, Any]]:
        converted_rules = []
        for rule in self.filter_rules(AssignmentDurationPredicate, BlockUser):
            threshold = None
            fast_count, total_count = 1, 1

            if isinstance(rule.predicate, AssignmentDurationPredicate):
                threshold = rule.predicate.threshold
            elif isinstance(rule.predicate, PredicateExpression):
                for predicate in rule.predicate.predicates:
                    assert isinstance(predicate, AssignmentDurationPredicate)
                    if predicate.comparison == ComparisonType('<=') or predicate.comparison == ComparisonType('<'):
                        threshold = predicate.threshold
            else:
                assert False, 'unreachable'

            assert threshold is not None
            assert assignment_duration_hint is not None
            assert isinstance(rule.action, BlockUser)

            assignment_threshold = assignment_duration_hint * threshold

            converted_rules.append(
                dict(
                    collector=toloka.collectors.AssignmentSubmitTime(
                        fast_submit_threshold_seconds=max(int(assignment_threshold.total_seconds()), 1),
                        history_size=10,
                    ),
                    conditions=[
                        toloka.conditions.TotalSubmittedCount >= total_count,
                        toloka.conditions.FastSubmittedCount >= fast_count,
                    ],
                    action=toloka.actions.RestrictionV2(
                        private_comment=rule.action.private_comment,
                        duration=int(rule.action.duration.total_seconds() // 60),
                        duration_unit=toloka.user_restriction.DurationUnit.MINUTES,
                        scope=toloka.UserRestriction.PROJECT,
                    ),
                )
            )

        for rule in self.filter_rules(AssignmentAccuracyPredicate, BlockUser):
            assert isinstance(rule.action, BlockUser)
            condition_assignment_answers = toloka.conditions.TotalAnswersCount >= control_tasks_count

            def convert_predicate_to_condition(accuracy_predicate: AssignmentAccuracyPredicate):
                value = accuracy_predicate.threshold * 100
                if accuracy_predicate.comparison == ComparisonType.LESS:
                    return toloka.conditions.CorrectAnswersRate < value
                elif accuracy_predicate.comparison == ComparisonType.LESS_OR_EQUAL:
                    return toloka.conditions.CorrectAnswersRate <= value
                elif accuracy_predicate.comparison == ComparisonType.GREATER:
                    return toloka.conditions.CorrectAnswersRate > value
                elif accuracy_predicate.comparison == ComparisonType.GREATER_OR_EQUAL:
                    return toloka.conditions.CorrectAnswersRate >= value
                else:
                    raise ValueError(f'unknown comparison type: {accuracy_predicate.comparison}')

            if isinstance(rule.predicate, PredicateExpression):
                predicate_conditions = [
                    convert_predicate_to_condition(predicate) for predicate in rule.predicate.predicates
                ]
            elif isinstance(rule.predicate, AssignmentAccuracyPredicate):
                predicate_conditions = [convert_predicate_to_condition(rule.predicate)]

            conditions = [condition_assignment_answers] + predicate_conditions

            converted_rules.append(
                dict(
                    collector=toloka.collectors.GoldenSet(history_size=control_tasks_count),
                    conditions=conditions,
                    action=toloka.actions.RestrictionV2(
                        private_comment=rule.action.private_comment,
                        duration=int(rule.action.duration.total_seconds() // 60),
                        duration_unit=toloka.user_restriction.DurationUnit.MINUTES,
                        scope=toloka.UserRestriction.PROJECT,
                    ),
                )
            )
        return converted_rules
