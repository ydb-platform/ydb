from dataclasses import dataclass
import datetime
from typing import List, Optional

from toloka.client.assignment import Assignment
from toloka.client.user_restriction import UserRestriction
from pandas import Timedelta

from .rule import (
    Rule,
    AssignmentAccuracyPredicate,
    AssignmentDurationPredicate,
    ComparisonType,
    SetAssignmentStatus,
    GiveBonusToUser,
    BlockUser,
    PredicateExpression,
    BooleanOperator,
)


def get_speed_rules(
    duration: Timedelta,
    less_or_equal: float,
    reject: bool = False,
    greater: Optional[float] = None,
) -> List[Rule]:
    rules = []
    if greater is None:
        predicate = AssignmentDurationPredicate(
            threshold=less_or_equal,
            comparison=ComparisonType.LESS_OR_EQUAL,
        )
        private_comment = f'Fast submits, <= {less_or_equal}'
    else:
        predicate = PredicateExpression(
            boolean_operator=BooleanOperator.AND,
            predicates=[
                AssignmentDurationPredicate(
                    threshold=less_or_equal,
                    comparison=ComparisonType.LESS_OR_EQUAL,
                ),
                AssignmentDurationPredicate(
                    threshold=greater,
                    comparison=ComparisonType.GREATER,
                ),
            ],
        )
        private_comment = f'Fast submits, {greater} < time <= {less_or_equal}'

    rules.append(
        Rule(
            predicate=predicate,
            action=BlockUser(
                scope=UserRestriction.Scope.POOL,
                private_comment=private_comment,
                duration=duration.to_pytimedelta(),
            ),
        )
    )

    if reject:
        rules.append(
            Rule(
                predicate=predicate,
                action=SetAssignmentStatus(status=Assignment.REJECTED),
            )
        )

    return rules


@dataclass(order=True)
class BlockTimePicker:
    ratio: float = 0.05
    time_value: str = '30m'
    reject: bool = False

    @property
    def duration(self) -> Timedelta:
        return Timedelta(self.time_value)

    def __post_init__(self):
        assert 0 <= self.ratio <= 1


class RuleBuilder:
    def __init__(self):
        self.rules: List[Rule] = []
        self.reward_added: bool = False
        self.speed_control_added: bool = False
        self.control_task_control_added = False

    def add_static_reward(self, threshold: float) -> 'RuleBuilder':
        assert not self.reward_added
        assert 1.0 >= threshold >= 0.0
        self.rules.extend(
            [
                Rule(
                    predicate=AssignmentAccuracyPredicate(
                        threshold=threshold, comparison=ComparisonType.GREATER_OR_EQUAL
                    ),
                    action=SetAssignmentStatus(status=Assignment.ACCEPTED),
                ),
                Rule(
                    predicate=AssignmentAccuracyPredicate(threshold=threshold, comparison=ComparisonType.LESS),
                    action=SetAssignmentStatus(status=Assignment.REJECTED),
                ),
            ]
        )
        self.reward_added = True
        return self

    def add_dynamic_reward(
        self,
        min_bonus_amount_usd: float,
        max_bonus_amount_usd: float,
        min_accuracy_for_bonus: float = 0.0,
        min_accuracy_for_accept: float = 0.0,
        bonus_granularity_num: int = 3,
    ) -> 'RuleBuilder':
        assert not self.reward_added
        assert 0 <= min_accuracy_for_accept <= min_accuracy_for_bonus <= 1.0
        assert 0.01 <= min_bonus_amount_usd <= max_bonus_amount_usd

        if bonus_granularity_num == 1:
            assert min_accuracy_for_bonus != 0.0

        min_accuracy_for_accept_int = round(min_accuracy_for_accept * 100)
        min_accuracy_for_bonus_int = round(min_accuracy_for_bonus * 100)

        min_bonus_amount_cents = round(min_bonus_amount_usd * 100)
        max_bonus_amount_cents = round(max_bonus_amount_usd * 100)

        assert bonus_granularity_num - 1 <= max_bonus_amount_cents - min_bonus_amount_cents

        self.rules.append(
            Rule(
                predicate=AssignmentAccuracyPredicate(
                    threshold=min_accuracy_for_accept, comparison=ComparisonType.GREATER_OR_EQUAL
                ),
                action=SetAssignmentStatus(status=Assignment.ACCEPTED),
            )
        )

        if min_accuracy_for_accept_int != 0:
            self.rules.append(
                Rule(
                    predicate=AssignmentAccuracyPredicate(
                        threshold=min_accuracy_for_accept, comparison=ComparisonType.LESS
                    ),
                    action=SetAssignmentStatus(status=Assignment.REJECTED),
                )
            )

        accuracy_delta = (100 - min_accuracy_for_bonus_int) / bonus_granularity_num

        if bonus_granularity_num > 1:
            bonus_amount_delta = (max_bonus_amount_cents - min_bonus_amount_cents) / (bonus_granularity_num - 1)
        else:
            bonus_amount_delta = 0.0

        for step in range(bonus_granularity_num):
            left = (min_accuracy_for_bonus_int + step * accuracy_delta) / 100
            right = (min_accuracy_for_bonus_int + (step + 1) * accuracy_delta) / 100

            bonus_amount_usd = round(min_bonus_amount_cents + bonus_amount_delta * step) / 100

            predicates = [AssignmentAccuracyPredicate(threshold=left, comparison=ComparisonType.GREATER_OR_EQUAL)]

            # for better float comparison in edge case
            if step != bonus_granularity_num - 1:
                predicates.append(AssignmentAccuracyPredicate(threshold=right, comparison=ComparisonType.LESS))

            self.rules.append(
                Rule(
                    predicate=PredicateExpression(boolean_operator=BooleanOperator.AND, predicates=predicates),
                    action=GiveBonusToUser(amount_usd=bonus_amount_usd),
                )
            )
        self.reward_added = True
        return self

    def add_speed_control(
        self,
        ratio_rand: float,
        ratio_poor: float,
    ) -> 'RuleBuilder':
        return self.add_complex_speed_control(
            speed_blocks=[BlockTimePicker(ratio_rand, '1d', True), BlockTimePicker(ratio_poor, '8h', False)],
        )

    def add_complex_speed_control(
        self,
        speed_blocks: List[BlockTimePicker],
    ) -> 'RuleBuilder':
        assert not self.speed_control_added

        if not speed_blocks:
            return self

        speed_blocks = sorted(speed_blocks)
        block = speed_blocks[0]
        speed_rules = get_speed_rules(block.duration, block.ratio, block.reject)

        for prev, block in zip(speed_blocks, speed_blocks[1:]):
            speed_rules.extend(get_speed_rules(block.duration, block.ratio, block.reject, prev.ratio))

        self.rules.extend(speed_rules)
        self.speed_control_added = True
        return self

    def add_control_task_control(
        self,
        control_task_count: int,
        control_task_correct_count_for_hard_block: int,
        control_task_correct_count_for_soft_block: int,
    ) -> 'RuleBuilder':
        control_task_correct_ratio_for_hard_block = control_task_correct_count_for_hard_block / control_task_count
        control_task_correct_ratio_for_soft_block = control_task_correct_count_for_soft_block / control_task_count
        if control_task_correct_count_for_hard_block:
            self.rules.append(
                Rule(
                    predicate=AssignmentAccuracyPredicate(
                        threshold=control_task_correct_ratio_for_hard_block, comparison=ComparisonType.LESS
                    ),
                    action=BlockUser(
                        private_comment=f'Control tasks: [0, {control_task_correct_count_for_hard_block}) done correctly',
                        duration=datetime.timedelta(hours=8),
                        scope=UserRestriction.POOL,
                    ),
                ),  # maybe block in project
            )
        self.rules.append(
            Rule(
                predicate=PredicateExpression(
                    predicates=[
                        AssignmentAccuracyPredicate(
                            threshold=control_task_correct_ratio_for_hard_block,
                            comparison=ComparisonType.GREATER_OR_EQUAL,
                        ),
                        AssignmentAccuracyPredicate(
                            threshold=control_task_correct_ratio_for_soft_block, comparison=ComparisonType.LESS
                        ),
                    ],
                    boolean_operator=BooleanOperator.AND,
                ),
                action=BlockUser(
                    private_comment=f'Control tasks: [{control_task_correct_count_for_hard_block}, {control_task_correct_count_for_soft_block}) done correctly',
                    duration=datetime.timedelta(hours=1),
                    scope=UserRestriction.POOL,
                ),
            ),
        )

        self.control_task_control_added = True
        return self

    def build(self) -> List[Rule]:
        assert self.reward_added
        return self.rules
