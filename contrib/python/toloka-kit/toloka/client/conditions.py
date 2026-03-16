__all__ = [
    'RuleConditionKey',
    'RuleCondition',
    'ComparableRuleCondition',
    'IdentityRuleCondition',
    'AcceptedAssignmentsCount',
    'AcceptedAssignmentsRate',
    'AssessmentEvent',
    'AssignmentsAcceptedCount',
    'CorrectAnswersRate',
    'FailRate',
    'FastSubmittedCount',
    'GoldenSetAnswersCount',
    'GoldenSetCorrectAnswersRate',
    'GoldenSetIncorrectAnswersRate',
    'IncomeSumForLast24Hours',
    'IncorrectAnswersRate',
    'NextAssignmentAvailable',
    'PendingAssignmentsCount',
    'PoolAccessRevokedReason',
    'RejectedAssignmentsCount',
    'RejectedAssignmentsRate',
    'SkillId',
    'SkippedInRowCount',
    'StoredResultsCount',
    'SubmittedAssignmentsCount',
    'SuccessRate',
    'TotalAnswersCount',
    'TotalAssignmentsCount',
    'TotalSubmittedCount'
]
from enum import unique
from typing import Any

from .primitives.base import BaseTolokaObject
from .primitives.operators import IdentityConditionMixin, ComparableConditionMixin
from ..util._codegen import attribute
from ..util._extendable_enum import ExtendableStrEnum


@unique
class RuleConditionKey(ExtendableStrEnum):
    ACCEPTED_ASSIGNMENTS_COUNT = 'accepted_assignments_count'
    ACCEPTED_ASSIGNMENTS_RATE = 'accepted_assignments_rate'
    ASSESSMENT_EVENT = 'assessment_event'
    ASSIGNMENTS_ACCEPTED_COUNT = 'assignments_accepted_count'
    CORRECT_ANSWERS_RATE = 'correct_answers_rate'
    FAIL_RATE = 'fail_rate'
    FAST_SUBMITTED_COUNT = 'fast_submitted_count'
    GOLDEN_SET_ANSWERS_COUNT = 'golden_set_answers_count'
    GOLDEN_SET_CORRECT_ANSWERS_RATE = 'golden_set_correct_answers_rate'
    GOLDEN_SET_INCORRECT_ANSWERS_RATE = 'golden_set_incorrect_answers_rate'
    INCOME_SUM_FOR_LAST_24_HOURS = 'income_sum_for_last_24_hours'
    INCORRECT_ANSWERS_RATE = 'incorrect_answers_rate'
    NEXT_ASSIGNMENT_AVAILABLE = 'next_assignment_available'
    PENDING_ASSIGNMENTS_COUNT = 'pending_assignments_count'
    POOL_ACCESS_REVOKED_REASON = 'pool_access_revoked_reason'
    REJECTED_ASSIGNMENTS_COUNT = 'rejected_assignments_count'
    REJECTED_ASSIGNMENTS_RATE = 'rejected_assignments_rate'
    SKILL_ID = 'skill_id'
    SKIPPED_IN_ROW_COUNT = 'skipped_in_row_count'
    STORED_RESULTS_COUNT = 'stored_results_count'
    SUBMITTED_ASSIGNMENTS_COUNT = 'submitted_assignments_count'
    SUCCESS_RATE = 'success_rate'
    TOTAL_ANSWERS_COUNT = 'total_answers_count'
    TOTAL_ASSIGNMENTS_COUNT = 'total_assignments_count'
    TOTAL_SUBMITTED_COUNT = 'total_submitted_count'


class RuleCondition(BaseTolokaObject, spec_enum=RuleConditionKey, spec_field='key'):
    operator: Any
    value: Any


class ComparableRuleCondition(RuleCondition, ComparableConditionMixin):
    pass


class IdentityRuleCondition(RuleCondition, IdentityConditionMixin):
    pass


class AcceptedAssignmentsCount(ComparableRuleCondition, spec_value=RuleConditionKey.ACCEPTED_ASSIGNMENTS_COUNT):
    """The number of accepted assignments of a task suite.

    `AcceptedAssignmentsCount` is used with collectors:
    - [AssignmentsAssessment](toloka.client.collectors.AssignmentsAssessment.md)

    See also:
    - [AssignmentsAcceptedCount](toloka.client.conditions.AssignmentsAcceptedCount.md) — The number of assignments accepted from a Toloker.

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentsAssessment(),
        >>>     conditions=[toloka.client.conditions.AcceptedAssignmentsCount < toloka.client.conditions.RejectedAssignmentsCount],
        >>>     action=toloka.client.actions.ChangeOverlap(delta=1, open_pool=True),
        >>> )
        ...
    """

    value: int


class AcceptedAssignmentsRate(ComparableRuleCondition, spec_value=RuleConditionKey.ACCEPTED_ASSIGNMENTS_RATE):
    """The percentage of accepted assignments out of all checked assignments from a Toloker.

    `AcceptedAssignmentsRate` is used with collectors:
    - [AcceptanceRate](toloka.client.collectors.AcceptanceRate.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AcceptanceRate(),
        >>>     conditions=[
        >>>         toloka.client.conditions.TotalAssignmentsCount > 10,
        >>>         toloka.client.conditions.AcceptedAssignmentsRate > 90,
        >>>     ],
        >>>     action=toloka.client.actions.SetSkill(skill_id='11294', skill_value=1)
        >>> )
        ...
    """

    value: float


class AssessmentEvent(IdentityRuleCondition, spec_value=RuleConditionKey.ASSESSMENT_EVENT):
    """An assignment status change event.

    Possible values:
        * `ACCEPT` — An assignment was accepted.
        * `ACCEPT_AFTER_REJECT` — An assignment with the previously set `REJECTED` status was accepted.
        * `REJECT` — An assignment was rejected.

    `AssessmentEvent` condition can be used with the `==` operator only.

    `AssessmentEvent` is used with collectors:
    - [AssignmentsAssessment](toloka.client.collectors.AssignmentsAssessment.md).

    Example:
        The example shows how to automatically increase the overlap of a task suite when an assignment was rejected.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentsAssessment(),
        >>>     conditions=[toloka.client.conditions.AssessmentEvent == toloka.client.conditions.AssessmentEvent.REJECT],
        >>>     action=toloka.client.actions.ChangeOverlap(delta=1, open_pool=True),
        >>> )
        ...
    """

    @unique
    class Type(ExtendableStrEnum):
        ACCEPT = 'ACCEPT'
        ACCEPT_AFTER_REJECT = 'ACCEPT_AFTER_REJECT'
        REJECT = 'REJECT'

    ACCEPT = Type.ACCEPT
    ACCEPT_AFTER_REJECT = Type.ACCEPT_AFTER_REJECT
    REJECT = Type.REJECT

    value: Type = attribute(autocast=True)


class AssignmentsAcceptedCount(ComparableRuleCondition, spec_value=RuleConditionKey.ASSIGNMENTS_ACCEPTED_COUNT):
    """The number of assignments accepted from a Toloker.

    `AssignmentsAcceptedCount` is used with collectors:
    - [AnswerCount](toloka.client.collectors.AnswerCount.md)

    See also:
    - [AcceptedAssignmentsCount](toloka.client.conditions.AcceptedAssignmentsCount.md) — The number of accepted assignments of a task suite.

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AnswerCount(),
        >>>     conditions=[toloka.client.conditions.AssignmentsAcceptedCount > 0],
        >>>     action=toloka.client.actions.SetSkill(skill_id='11294', skill_value=1),
        >>> )
        ...
    """

    value: int


class CorrectAnswersRate(ComparableRuleCondition, spec_value=RuleConditionKey.CORRECT_ANSWERS_RATE):
    """The percentage of correct responses.

    `CorrectAnswersRate` is used with collectors:
    - [GoldenSet](toloka.client.collectors.GoldenSet.md)
    - [MajorityVote](toloka.client.collectors.MajorityVote.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.MajorityVote(answer_threshold=2),
        >>>     conditions=[
        >>>         toloka.client.conditions.TotalAnswersCount > 9,
        >>>         toloka.client.conditions.CorrectAnswersRate < 60,
        >>>     ],
        >>>     action=toloka.client.actions.RejectAllAssignments(public_comment='Too low quality')
        >>> )
        ...
    """

    value: float


class FailRate(ComparableRuleCondition, spec_value=RuleConditionKey.FAIL_RATE):
    """Deprecated. The percentage of unsolved captchas.

    `FailRate` is used with collectors:
    - [Captcha](toloka.client.collectors.Captcha.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.set_captcha_frequency('MEDIUM')
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.Captcha(history_size=5),
        >>>     conditions=[
        >>>         toloka.client.conditions.FailRate > 40,
        >>>         toloka.client.conditions.StoredResultsCount >= 3
        >>>     ],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope='PROJECT', duration=15, duration_unit='DAYS'
        >>>     )
        >>> )
        ...
    """

    value: float


class FastSubmittedCount(ComparableRuleCondition, spec_value=RuleConditionKey.FAST_SUBMITTED_COUNT):
    """The number of assignments completed by a Toloker too fast.

    `FastSubmittedCount` is used with collectors:
    - [AssignmentSubmitTime](toloka.client.collectors.AssignmentSubmitTime.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentSubmitTime(
        >>>         history_size=5, fast_submit_threshold_seconds=20
        >>>     ),
        >>>     conditions=[toloka.client.conditions.FastSubmittedCount > 3],
        >>>     action=toloka.client.actions.RejectAllAssignments(public_comment='Too fast responses.')
        >>> )
        ...
    """

    value: int


class GoldenSetAnswersCount(ComparableRuleCondition, spec_value=RuleConditionKey.GOLDEN_SET_ANSWERS_COUNT):
    """The number of completed control tasks.

    `GoldenSetAnswersCount` is used with collectors:
    - [GoldenSet](toloka.client.collectors.GoldenSet.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.GoldenSet(history_size=5),
        >>>     conditions=[
        >>>         toloka.client.conditions.GoldenSetCorrectAnswersRate > 80,
        >>>         toloka.client.conditions.GoldenSetAnswersCount >= 5,
        >>>     ],
        >>>     action=toloka.client.actions.ApproveAllAssignments()
        >>> )
        ...
    """

    value: int


class GoldenSetCorrectAnswersRate(ComparableRuleCondition, spec_value=RuleConditionKey.GOLDEN_SET_CORRECT_ANSWERS_RATE):
    """The percentage of correct responses to control tasks.

    `GoldenSetCorrectAnswersRate` is used with collectors:
    - [GoldenSet](toloka.client.collectors.GoldenSet.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.GoldenSet(history_size=5),
        >>>     conditions=[
        >>>         toloka.client.conditions.GoldenSetCorrectAnswersRate > 80,
        >>>         toloka.client.conditions.GoldenSetAnswersCount >= 5,
        >>>     ],
        >>>     action=toloka.client.actions.ApproveAllAssignments()
        >>> )
        ...
    """

    value: float


class GoldenSetIncorrectAnswersRate(ComparableRuleCondition, spec_value=RuleConditionKey.GOLDEN_SET_INCORRECT_ANSWERS_RATE):
    """The percentage of incorrect responses to control tasks.

    `GoldenSetIncorrectAnswersRate` is used with collectors:
    - [GoldenSet](toloka.client.collectors.GoldenSet.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.GoldenSet(history_size=5),
        >>>     conditions=[
        >>>         toloka.client.conditions.GoldenSetIncorrectAnswersRate >= 40,
        >>>         toloka.client.conditions.GoldenSetAnswersCount >= 5,
        >>>     ],
        >>>     action=toloka.client.actions.RejectAllAssignments()
        >>> )
        ...
    """

    value: float


class IncomeSumForLast24Hours(ComparableRuleCondition, spec_value=RuleConditionKey.INCOME_SUM_FOR_LAST_24_HOURS):
    """The Toloker's earnings for completed tasks in the pool during the last 24 hours.

    `IncomeSumForLast24Hours` is used with collectors:
    - [Income](toloka.client.collectors.Income.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.Income(),
        >>>     conditions=[toloka.client.conditions.IncomeSumForLast24Hours > 0.8],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope='PROJECT', duration=1, duration_unit='DAYS',
        >>>         private_comment='Earnings limit is reached',
        >>>     )
        >>> )
        ...
    """

    value: float


class IncorrectAnswersRate(ComparableRuleCondition, spec_value=RuleConditionKey.INCORRECT_ANSWERS_RATE):
    """The percentage of incorrect responses.

    `IncorrectAnswersRate` is used with collectors:
    - [MajorityVote](toloka.client.collectors.MajorityVote.md)
    - [GoldenSet](toloka.client.collectors.GoldenSet.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.MajorityVote(answer_threshold=2),
        >>>     conditions=[
        >>>         toloka.client.conditions.TotalAnswersCount > 9,
        >>>         toloka.client.conditions.IncorrectAnswersRate > 60,
        >>>     ],
        >>>     action=toloka.client.actions.RejectAllAssignments(public_comment='Too low quality')
        >>> )
        ...
    """

    value: float


class NextAssignmentAvailable(ComparableRuleCondition, spec_value=RuleConditionKey.NEXT_ASSIGNMENT_AVAILABLE):
    value: bool


class PendingAssignmentsCount(ComparableRuleCondition, spec_value=RuleConditionKey.PENDING_ASSIGNMENTS_COUNT):
    """The number of pending assignments that must be checked.

    `PendingAssignmentsCount` is used with collectors:
    - [AssignmentsAssessment](toloka.client.collectors.AssignmentsAssessment.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentsAssessment(),
        >>>     conditions=[toloka.client.conditions.PendingAssignmentsCount < 5],
        >>>     action=toloka.client.actions.ChangeOverlap(delta=1, open_pool=True),
        >>> )
        ...
    """

    value: int


class PoolAccessRevokedReason(IdentityRuleCondition, spec_value=RuleConditionKey.POOL_ACCESS_REVOKED_REASON):
    """The reason why a Toloker has lost access to a pool.

    Possible values:
        * `SKILL_CHANGE` — The Toloker no longer meets one or more filters.
        * `RESTRICTION` — The Toloker's access to tasks is blocked by a quality control rule.

    `PoolAccessRevokedReason` is used with collectors:
    - [UsersAssessment](toloka.client.collectors.UsersAssessment.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.UsersAssessment(),
        >>>     conditions=[toloka.client.conditions.PoolAccessRevokedReason ==
        >>>         toloka.client.conditions.PoolAccessRevokedReason.RESTRICTION],
        >>>     action=toloka.client.actions.ChangeOverlap(delta=1, open_pool=True),
        >>> )
        ...
    """

    @unique
    class Type(ExtendableStrEnum):
        SKILL_CHANGE = 'SKILL_CHANGE'
        RESTRICTION = 'RESTRICTION'

    SKILL_CHANGE = Type.SKILL_CHANGE
    RESTRICTION = Type.RESTRICTION

    value: Type = attribute(autocast=True)


class RejectedAssignmentsCount(ComparableRuleCondition, spec_value=RuleConditionKey.REJECTED_ASSIGNMENTS_COUNT):
    """The number of rejected assignments of a task suite.

    `RejectedAssignmentsCount` is used with collectors:
    - [AssignmentsAssessment](toloka.client.collectors.AssignmentsAssessment.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentsAssessment(),
        >>>     conditions=[toloka.client.conditions.AcceptedAssignmentsCount < toloka.client.conditions.RejectedAssignmentsCount],
        >>>     action=toloka.client.actions.ChangeOverlap(delta=1, open_pool=True),
        >>> )
        ...
    """

    value: int


class RejectedAssignmentsRate(ComparableRuleCondition, spec_value=RuleConditionKey.REJECTED_ASSIGNMENTS_RATE):
    """A percentage of rejected assignments submitted by a Toloker.

    `RejectedAssignmentsRate` is used with collectors:
    - [AcceptanceRate](toloka.client.collectors.AcceptanceRate.md)

    See also:
    - [RejectedAssignmentsCount](toloka.client.conditions.RejectedAssignmentsCount.md) — The number of rejected assignments of a task suite.

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AcceptanceRate(),
        >>>     conditions=[
        >>>         toloka.client.conditions.TotalAssignmentsCount > 2,
        >>>         toloka.client.conditions.RejectedAssignmentsRate > 40,
        >>>     ],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope='PROJECT', duration=15, duration_unit='DAYS'
        >>>     )
        >>> )
        ...
    """

    value: float


class SkillId(IdentityRuleCondition, spec_value=RuleConditionKey.SKILL_ID):
    """The ID of a changed skill which caused access blocking.

    `SkillId` provides details if the [PoolAccessRevokedReason](toloka.client.conditions.PoolAccessRevokedReason.md) condition equals `SKILL_CHANGE`.

    `SkillId` is used with collectors:
    - [UsersAssessment](toloka.client.collectors.UsersAssessment.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.UsersAssessment(),
        >>>     conditions=[toloka.client.conditions.PoolAccessRevokedReason ==
        >>>         toloka.client.conditions.PoolAccessRevokedReason.SKILL_CHANGE,
        >>>         toloka.client.conditions.SkillId == '11294'
        >>>     ],
        >>>     action=toloka.client.actions.ChangeOverlap(delta=1, open_pool=True),
        >>> )
        ...
    """

    value: str


class SkippedInRowCount(ComparableRuleCondition, spec_value=RuleConditionKey.SKIPPED_IN_ROW_COUNT):
    """The number of tasks skipped in a row by a Toloker.

    `SkippedInRowCount` is used with collectors:
    - [SkippedInRowAssignments](toloka.client.collectors.SkippedInRowAssignments.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.SkippedInRowAssignments(),
        >>>     conditions=[toloka.client.conditions.SkippedInRowCount > 3],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope='PROJECT', duration=15, duration_unit='DAYS'
        >>>     )
        >>> )
        ...
    """

    value: int


class StoredResultsCount(ComparableRuleCondition, spec_value=RuleConditionKey.STORED_RESULTS_COUNT):
    """Deprecated. The number of times a Toloker entered captcha.

    `StoredResultsCount` is used with collectors:
    - [Captcha](toloka.client.collectors.Captcha.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.set_captcha_frequency('MEDIUM')
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.Captcha(history_size=5),
        >>>     conditions=[
        >>>         toloka.client.conditions.FailRate > 40,
        >>>         toloka.client.conditions.StoredResultsCount >= 3
        >>>     ],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope='PROJECT', duration=15, duration_unit='DAYS'
        >>>     )
        >>> )
        ...
    """

    value: int


class SubmittedAssignmentsCount(ComparableRuleCondition, spec_value=RuleConditionKey.SUBMITTED_ASSIGNMENTS_COUNT):
    value: int


class SuccessRate(ComparableRuleCondition, spec_value=RuleConditionKey.SUCCESS_RATE):
    """Deprecated. A percentage of solved captchas out of all entered captchas.

    `SuccessRate` is used with collectors:
    - [Captcha](toloka.client.collectors.Captcha.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.set_captcha_frequency('MEDIUM')
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.Captcha(history_size=5),
        >>>     conditions=[
        >>>         toloka.client.conditions.SuccessRate < 40,
        >>>         toloka.client.conditions.StoredResultsCount >= 3
        >>>     ],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope='PROJECT', duration=15, duration_unit='DAYS'
        >>>     )
        >>> )
        ...
    """

    value: float


class TotalAnswersCount(ComparableRuleCondition, spec_value=RuleConditionKey.TOTAL_ANSWERS_COUNT):
    """The number of completed tasks.

    `TotalAnswersCount` is used with collectors:
    - [GoldenSet](toloka.client.collectors.GoldenSet.md)
    - [MajorityVote](toloka.client.collectors.MajorityVote.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.MajorityVote(answer_threshold=2),
        >>>     conditions=[
        >>>         toloka.client.conditions.TotalAnswersCount > 9,
        >>>         toloka.client.conditions.IncorrectAnswersRate > 60,
        >>>     ],
        >>>     action=toloka.client.actions.RejectAllAssignments()
        >>> )
        ...
    """

    value: int


class TotalAssignmentsCount(ComparableRuleCondition, spec_value=RuleConditionKey.TOTAL_ASSIGNMENTS_COUNT):
    """The number of checked assignments out of all assignments submitted by a Toloker.

    `TotalAssignmentsCount` is used with collectors:
    - [AcceptanceRate](toloka.client.collectors.AcceptanceRate.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AcceptanceRate(),
        >>>     conditions=[
        >>>         toloka.client.conditions.TotalAssignmentsCount > 10,
        >>>         toloka.client.conditions.AcceptedAssignmentsRate > 90,
        >>>     ],
        >>>     action=toloka.client.actions.SetSkill(skill_id='11294', skill_value=1)
        >>> )
        ...
    """

    value: int


class TotalSubmittedCount(ComparableRuleCondition, spec_value=RuleConditionKey.TOTAL_SUBMITTED_COUNT):
    """The number of assignments submitted by a Toloker.

    `TotalSubmittedCount` is used with collectors:
    - [AssignmentSubmitTime](toloka.client.collectors.AssignmentSubmitTime.md)

    Example:
        >>> pool = toloka.client.pool.Pool()
        >>> pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentSubmitTime(
        >>>         fast_submit_threshold_seconds=20
        >>>     ),
        >>>     conditions=[toloka.client.conditions.FastSubmittedCount > 3,
        >>>         toloka.client.conditions.TotalSubmittedCount <= 5],
        >>>     action=toloka.client.actions.RejectAllAssignments()
        >>> )
        ...
    """

    value: int
