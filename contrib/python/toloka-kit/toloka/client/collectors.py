__all__ = [
    'CollectorConfig',
    'AcceptanceRate',
    'AnswerCount',
    'AssignmentsAssessment',
    'AssignmentSubmitTime',
    'Captcha',
    'GoldenSet',
    'Income',
    'MajorityVote',
    'SkippedInRowAssignments',
    'Training',
    'UsersAssessment'
]
"""
https://toloka.ai/docs/guide/control
"""

import logging

from enum import unique
from typing import ClassVar, FrozenSet, List, Optional
from uuid import UUID

from .conditions import RuleCondition
from .conditions import RuleConditionKey
from .primitives.base import BaseParameters
from ..util._docstrings import inherit_docstrings
from ..util._extendable_enum import ExtendableStrEnum


logger = logging.getLogger(__file__)


def _captcha_deprecation_warning(*args, **kwargs):
    logger.warning(
        'CAPTCHA-based quality control settings are deprecated and will be removed in future. '
        'CAPTCHAs are now included automatically for better quality control.'
    )


class CollectorConfig(BaseParameters, spec_enum='Type', spec_field='type'):
    """Base class for all collectors.

    Attributes:
        uuid: The ID of a collector.
            Note that when you clone a pool, both pools start using the same collector, because it is not cloned.
            Usually, it is not an intended behavior. For example, in this case one collector gathers history size from both pools.
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]]

    @unique
    class Type(ExtendableStrEnum):
        GOLDEN_SET = 'GOLDEN_SET'
        MAJORITY_VOTE = 'MAJORITY_VOTE'
        CAPTCHA = 'CAPTCHA'
        INCOME = 'INCOME'
        SKIPPED_IN_ROW_ASSIGNMENTS = 'SKIPPED_IN_ROW_ASSIGNMENTS'
        ANSWER_COUNT = 'ANSWER_COUNT'
        ASSIGNMENT_SUBMIT_TIME = 'ASSIGNMENT_SUBMIT_TIME'
        ACCEPTANCE_RATE = 'ACCEPTANCE_RATE'
        ASSIGNMENTS_ASSESSMENT = 'ASSIGNMENTS_ASSESSMENT'
        USERS_ASSESSMENT = 'USERS_ASSESSMENT'
        TRAINING = 'TRAINING'

    def validate_condition(self, conditions: List[RuleCondition]):
        incompatible_conditions = [c for c in conditions if c.key not in self._compatible_conditions]
        if incompatible_conditions:
            raise ValueError(f'Incompatible conditions {incompatible_conditions}')

    uuid: UUID


@inherit_docstrings
class AcceptanceRate(CollectorConfig, spec_value=CollectorConfig.Type.ACCEPTANCE_RATE):
    """Counts accepted and rejected Toloker's assignments.

    If non-automatic acceptance is set in the pool, you may use this collector to:
    - Set a Toloker's skill.
    - Block access for Tolokers with too many rejected responses.

    The collector can be used with conditions:
    * [TotalAssignmentsCount](toloka.client.conditions.TotalAssignmentsCount.md) — Total count of checked assignments submitted by a Toloker.
    * [AcceptedAssignmentsRate](toloka.client.conditions.AcceptedAssignmentsRate.md) — A percentage of accepted assignments.
    * [RejectedAssignmentsRate](toloka.client.conditions.RejectedAssignmentsRate.md) — A percentage of rejected assignments.

    The collector can be used with actions:
    * [RestrictionV2](toloka.client.actions.RestrictionV2.md) blocks access to projects or pools.
    * [ApproveAllAssignments](toloka.client.actions.ApproveAllAssignments.md) accepts all Toloker's assignments.
    * [RejectAllAssignments](toloka.client.actions.RejectAllAssignments.md) rejects all Toloker's assignments.
    * [SetSkill](toloka.client.actions.SetSkill.md) sets Toloker's skill value.
    * [SetSkillFromOutputField](toloka.client.actions.SetSkillFromOutputField.md) sets Toloker's skill value using an output field.

    Attributes:
        parameters.history_size: The maximum number of recent assignments used to calculate the statistics.
            If `history_size` is omitted, all Toloker's assignments are counted.

    Example:
        The example shows how to block a Toloker if they make too many mistakes.
        If more than 35% of responses are rejected, then the Toloker is restricted to access the project.
        The rule is applied after collecting 3 or more responses.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AcceptanceRate(),
        >>>     conditions=[
        >>>         toloka.client.conditions.TotalAssignmentsCount > 2,
        >>>         toloka.client.conditions.RejectedAssignmentsRate > 35,
        >>>     ],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope=toloka.client.user_restriction.UserRestriction.PROJECT,
        >>>         duration=15,
        >>>         duration_unit='DAYS',
        >>>         private_comment='The Toloker often makes mistakes',
        >>>     )
        >>> )
        ...
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.TOTAL_ASSIGNMENTS_COUNT,
        RuleConditionKey.ACCEPTED_ASSIGNMENTS_RATE,
        RuleConditionKey.REJECTED_ASSIGNMENTS_RATE,
    ])

    class Parameters(CollectorConfig.Parameters):
        history_size: Optional[int] = None


@inherit_docstrings
class AnswerCount(CollectorConfig, spec_value=CollectorConfig.Type.ANSWER_COUNT):
    """Counts assignments submitted by a Toloker.

    Collector use cases.
    - To involve as many Tolokers as possible limit assignments to 1.
    - To improve protection from robots set the limit higher, such as 10% of the pool's tasks.
    - You can filter Tolokers who complete your tasks, so they don't check the tasks in the checking project.

    The collector can be used with conditions:
    * [AssignmentsAcceptedCount](toloka.client.conditions.AssignmentsAcceptedCount.md) — The number of accepted assignments.

    The collector can be used with actions:
    * [RestrictionV2](toloka.client.actions.RestrictionV2.md) blocks access to projects or pools.
    * [ApproveAllAssignments](toloka.client.actions.ApproveAllAssignments.md) accepts all Toloker's assignments.
    * [RejectAllAssignments](toloka.client.actions.RejectAllAssignments.md) rejects all Toloker's assignments.
    * [SetSkill](toloka.client.actions.SetSkill.md) sets Toloker's skill value.

    Example:
        The example shows how to mark Tolokers completing any task in the pool so that you can filter them later in the checking project.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AnswerCount(),
        >>>     conditions=[toloka.client.conditions.AssignmentsAcceptedCount > 0],
        >>>     action=toloka.client.actions.SetSkill(skill_id='11294', skill_value=1),
        >>> )
        ...
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.ASSIGNMENTS_ACCEPTED_COUNT,
    ])


@inherit_docstrings
class AssignmentsAssessment(CollectorConfig, spec_value=CollectorConfig.Type.ASSIGNMENTS_ASSESSMENT):
    """Counts accepted and rejected assignments for every task suite.

    Collector use cases.
    - To reassign rejected task suite to other Tolokers increase
    the overlap of the task suite. It is essential if the default overlap value is 1.
    - You accept an assignment and don't need to collect more responses for that task suite. To save money stop assigning the task suite.

    The collector can be used with conditions:
    * [PendingAssignmentsCount](toloka.client.conditions.PendingAssignmentsCount.md) — The number of pending assignments that must be checked.
    * [AcceptedAssignmentsCount](toloka.client.conditions.AcceptedAssignmentsCount.md) — The number of accepted assignments for a task suite.
    * [RejectedAssignmentsCount](toloka.client.conditions.RejectedAssignmentsCount.md) — The number of rejected assignments for a task suite.
    * [AssessmentEvent](toloka.client.conditions.AssessmentEvent.md) — An assignment status change event.

    The collector can be used with actions:
    * [ChangeOverlap](toloka.client.actions.ChangeOverlap.md) changes the overlap of a task suite.

    Example:
        The example shows how to reassign rejected task suites to other Tolokers.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentsAssessment(),
        >>>     conditions=[toloka.client.conditions.AssessmentEvent == toloka.client.conditions.AssessmentEvent.REJECT],
        >>>     action=toloka.client.actions.ChangeOverlap(delta=1, open_pool=True),
        >>> )
        ...
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.PENDING_ASSIGNMENTS_COUNT,
        RuleConditionKey.ACCEPTED_ASSIGNMENTS_COUNT,
        RuleConditionKey.REJECTED_ASSIGNMENTS_COUNT,
        RuleConditionKey.ASSESSMENT_EVENT,
    ])


@inherit_docstrings
class AssignmentSubmitTime(CollectorConfig, spec_value=CollectorConfig.Type.ASSIGNMENT_SUBMIT_TIME):
    """Counts fast responses.

    Collector use cases.
    - To find Tolokers who respond suspiciously quickly.
    - To improve protection against robots.

    The collector can be used with conditions:
    * [TotalSubmittedCount](toloka.client.conditions.TotalSubmittedCount.md) — The number of assignments completed by a specific Toloker.
    * [FastSubmittedCount](toloka.client.conditions.FastSubmittedCount.md) — The number of assignments completed too fast.

    The collector can be used with actions:
    * [RestrictionV2](toloka.client.actions.RestrictionV2.md) blocks access to projects or pools.
    * [ApproveAllAssignments](toloka.client.actions.ApproveAllAssignments.md) accepts all Toloker's assignments.
    * [RejectAllAssignments](toloka.client.actions.RejectAllAssignments.md) rejects all Toloker's assignments.
    * [SetSkill](toloka.client.actions.SetSkill.md) sets Toloker's skill value.

    Attributes:
        parameters.fast_submit_threshold_seconds: Fast response threshold in seconds.
            Any response submitted in less time than threshold is considered a fast response.
        parameters.history_size: The maximum number of recent assignments used to calculate the statistics.
            If `history_size` is omitted, all Toloker's assignments in the pool are counted.

    Example:
        The example shows how to reject all assignments if a Toloker sent at least 4 responses during 20 seconds after getting every task suite.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentSubmitTime(history_size=5, fast_submit_threshold_seconds=20),
        >>>     conditions=[toloka.client.conditions.FastSubmittedCount > 3],
        >>>     action=toloka.client.actions.RejectAllAssignments(public_comment='Too fast responses.')
        >>> )
        ...
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.TOTAL_SUBMITTED_COUNT,
        RuleConditionKey.FAST_SUBMITTED_COUNT,
    ])

    class Parameters(CollectorConfig.Parameters):
        fast_submit_threshold_seconds: int
        history_size: Optional[int] = None


@inherit_docstrings
class Captcha(CollectorConfig, spec_value=CollectorConfig.Type.CAPTCHA):
    """Deprecated. Collects captcha statistics for every Toloker.

    Captcha provides an advanced protection against robots. It is used with conditions:
    * [StoredResultsCount](toloka.client.conditions.StoredResultsCount.md) — How many times the Toloker entered a captcha.
    * [SuccessRate](toloka.client.conditions.SuccessRate.md) — The percentage of solved captchas.
    * [FailRate](toloka.client.conditions.FailRate.md) — The percentage of unsolved captchas.

    The collector can be used with actions:
    * [RestrictionV2](toloka.client.actions.RestrictionV2.md) blocks access to projects or pools.
    * [ApproveAllAssignments](toloka.client.actions.ApproveAllAssignments.md) accepts all Toloker's assignments.
    * [RejectAllAssignments](toloka.client.actions.RejectAllAssignments.md) rejects all Toloker's assignments.
    * [SetSkill](toloka.client.actions.SetSkill.md) sets Toloker's skill value.
    * [SetSkillFromOutputField](toloka.client.actions.SetSkillFromOutputField.md) sets Toloker's skill value using an output field.

    Attributes:
        parameters.history_size: The maximum number of recent captchas used to calculate the statistics.
            If `history_size` is omitted, all captchas are counted.

    Example:
        The example shows how to block Toloker's access to the project for 15 days if they solve 60% of captchas or less.
        The rule is applied after entering at least 3 captchas.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.set_captcha_frequency('MEDIUM')
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.Captcha(history_size=5),
        >>>     conditions=[
        >>>         toloka.client.conditions.SuccessRate < 60,
        >>>         toloka.client.conditions.StoredResultsCount >= 3,
        >>>     ],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope=toloka.client.user_restriction.UserRestriction.PROJECT,
        >>>         duration=15,
        >>>         duration_unit='DAYS',
        >>>         private_comment='Toloker often makes mistakes in captcha',
        >>>     )
        >>> )
        ...
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.STORED_RESULTS_COUNT,
        RuleConditionKey.SUCCESS_RATE,
        RuleConditionKey.FAIL_RATE,
    ])

    class Parameters(CollectorConfig.Parameters):
        history_size: Optional[int] = None

    __attrs_post_init__ = _captcha_deprecation_warning


@inherit_docstrings
class GoldenSet(CollectorConfig, spec_value=CollectorConfig.Type.GOLDEN_SET):
    """Collects control and training task statistics for a Toloker.

    Use control tasks to assign a skill to Tolokers based on their responses and block Tolokers who submit incorrect responses.

    It is better **not** to use this collector if:
    - There are a lot of response options.
    - Tolokers need to attach files to assignments.
    - Tolokers need to transcribe text.
    - Tolokers need to select objects on a photo.
    - Tasks don't have a correct or incorrect responses. For example, you ask about Toloker preferences.

    The collector can be used with conditions:
    * [TotalAnswersCount](toloka.client.conditions.TotalAnswersCount.md) — The number of completed control and training tasks.
    * [CorrectAnswersRate](toloka.client.conditions.CorrectAnswersRate.md) — The percentage of correct responses to control and training tasks.
    * [IncorrectAnswersRate](toloka.client.conditions.IncorrectAnswersRate.md) — The percentage of incorrect responses to control and training tasks.
    * [GoldenSetAnswersCount](toloka.client.conditions.GoldenSetAnswersCount.md) — The number of completed control tasks.
    * [GoldenSetCorrectAnswersRate](toloka.client.conditions.GoldenSetCorrectAnswersRate.md) — The percentage of correct responses to control tasks.
    * [GoldenSetIncorrectAnswersRate](toloka.client.conditions.GoldenSetIncorrectAnswersRate.md) — The percentage of incorrect responses to control tasks.

    The collector can be used with actions:
    * [RestrictionV2](toloka.client.actions.RestrictionV2.md) blocks access to projects or pools.
    * [ApproveAllAssignments](toloka.client.actions.ApproveAllAssignments.md) accepts all Toloker's assignments.
    * [RejectAllAssignments](toloka.client.actions.RejectAllAssignments.md) rejects all Toloker's assignments.
    * [SetSkill](toloka.client.actions.SetSkill.md) sets Toloker's skill value.
    * [SetSkillFromOutputField](toloka.client.actions.SetSkillFromOutputField.md) sets Toloker's skill value using an output field.

    Attributes:
        parameters.history_size: The maximum number of recent control or training tasks used to calculate the statistics.
            If `history_size` is omitted, all Toloker's control or training tasks in the pool are counted.

    Example:
        The example shows how to accept all assignments if more than 80% of responses to control tasks are correct.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.GoldenSet(history_size=5),
        >>>     conditions=[
        >>>         toloka.client.conditions.GoldenSetCorrectAnswersRate > 80,
        >>>         toloka.client.conditions.GoldenSetAnswersCount >= 5,
        >>>     ],
        >>>     action=toloka.client.actions.ApproveAllAssignments()
        >>> )
        ...
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.TOTAL_ANSWERS_COUNT,
        RuleConditionKey.CORRECT_ANSWERS_RATE,
        RuleConditionKey.INCORRECT_ANSWERS_RATE,
        RuleConditionKey.GOLDEN_SET_ANSWERS_COUNT,
        RuleConditionKey.GOLDEN_SET_CORRECT_ANSWERS_RATE,
        RuleConditionKey.GOLDEN_SET_INCORRECT_ANSWERS_RATE,
    ])

    class Parameters(CollectorConfig.Parameters):
        history_size: Optional[int] = None


@inherit_docstrings
class Income(CollectorConfig, spec_value=CollectorConfig.Type.INCOME):
    """Counts Toloker's daily earnings in the pool.

    Helpful when you need to:
    - Get responses from as many Tolokers as possible.

    The collector can be used with conditions:
    * [IncomeSumForLast24Hours](toloka.client.conditions.IncomeSumForLast24Hours.md) — The Toloker earnings for completed tasks in the pool during the last 24 hours.

    The collector can be used with actions:
    * [RestrictionV2](toloka.client.actions.RestrictionV2.md) blocks access to projects or pools.
    * [ApproveAllAssignments](toloka.client.actions.ApproveAllAssignments.md) accepts all Toloker's assignments.
    * [RejectAllAssignments](toloka.client.actions.RejectAllAssignments.md) rejects all Toloker's assignments.
    * [SetSkill](toloka.client.actions.SetSkill.md) sets Toloker's skill value.

    Example:
        The example shows how to block Toloker's access to the project for 1 day if their earnings reach 1 dollar.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.Income(),
        >>>     conditions=[toloka.client.conditions.IncomeSumForLast24Hours > 1],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope=toloka.client.user_restriction.UserRestriction.PROJECT,
        >>>         duration=1,
        >>>         duration_unit='DAYS',
        >>>         private_comment='Earnings limit is reached',
        >>>     )
        >>> )
        ...
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.INCOME_SUM_FOR_LAST_24_HOURS,
    ])


@inherit_docstrings
class MajorityVote(CollectorConfig, spec_value=CollectorConfig.Type.MAJORITY_VOTE):
    """Counts correct responses determined by the majority vote method.

    A response chosen by the majority is considered to be correct, and other responses are considered to be incorrect.
    Depending on the percentage of correct responses, you can either increase a Toloker's skill value, or to block the Toloker.

    The collector can be used with conditions:
    * [TotalAnswersCount](toloka.client.conditions.TotalAnswersCount.md) — The number of completed tasks by the Toloker.
    * [CorrectAnswersRate](toloka.client.conditions.CorrectAnswersRate.md) — The percentage of correct responses.
    * [IncorrectAnswersRate](toloka.client.conditions.IncorrectAnswersRate.md) — The percentage of incorrect responses.

    The collector can be used with actions:
    * [RestrictionV2](toloka.client.actions.RestrictionV2.md) blocks access to projects or pools.
    * [ApproveAllAssignments](toloka.client.actions.ApproveAllAssignments.md) accepts all Toloker's assignments.
    * [RejectAllAssignments](toloka.client.actions.RejectAllAssignments.md) rejects all Toloker's assignments.
    * [SetSkill](toloka.client.actions.SetSkill.md) sets Toloker's skill value.
    * [SetSkillFromOutputField](toloka.client.actions.SetSkillFromOutputField.md) sets Toloker's skill value using an output field.

    Attributes:
        parameters.answer_threshold: The number of Tolokers considered the majority.
        parameters.history_size: The maximum number of recent Toloker's responses to calculate the statistics. If it is omitted, calculation is based on all collected responses.

    Example:
        The example shows how to reject all Toloker's responses if they significantly differ from the majority. The rule is applied after collecting at least 10 responses.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.MajorityVote(answer_threshold=2),
        >>>     conditions=[
        >>>         toloka.client.conditions.TotalAnswersCount > 9,
        >>>         toloka.client.conditions.CorrectAnswersRate < 60,
        >>>     ],
        >>>     action=toloka.client.actions.RejectAllAssignments(public_comment='Too low quality')
        >>> )
        ...
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.TOTAL_ANSWERS_COUNT,
        RuleConditionKey.CORRECT_ANSWERS_RATE,
        RuleConditionKey.INCORRECT_ANSWERS_RATE,
    ])

    class Parameters(CollectorConfig.Parameters):
        answer_threshold: int
        history_size: Optional[int] = None

    parameters: Parameters


@inherit_docstrings
class SkippedInRowAssignments(CollectorConfig, spec_value=CollectorConfig.Type.SKIPPED_IN_ROW_ASSIGNMENTS):
    """Counts task suites skipped in a row by a Toloker.

    Skipping tasks is considered an indirect indicator of quality of responses. You can block access to a pool or project if a Toloker skips multiple task suites in a row.

    The collector can be used with conditions:
    * [SkippedInRowCount](toloka.client.conditions.SkippedInRowCount.md) — How many tasks in a row a Toloker skipped.

    The collector can be used with actions:
    * [RestrictionV2](toloka.client.actions.RestrictionV2.md) blocks access to projects or pools.
    * [ApproveAllAssignments](toloka.client.actions.ApproveAllAssignments.md) accepts all Toloker's assignments.
    * [RejectAllAssignments](toloka.client.actions.RejectAllAssignments.md) rejects all Toloker's assignments.
    * [SetSkill](toloka.client.actions.SetSkill.md) sets Toloker's skill value.

    Example:
        The example shows how to block Toloker's access to the project for 15 days if he skipped more than 3 task suites in a row.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.SkippedInRowAssignments(),
        >>>     conditions=[toloka.client.conditions.SkippedInRowCount > 3],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope=toloka.client.user_restriction.UserRestriction.PROJECT,
        >>>         duration=15,
        >>>         duration_unit='DAYS',
        >>>         private_comment='Skips too many task suites in a row',
        >>>     )
        >>> )
        ...
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.SKIPPED_IN_ROW_COUNT,
    ])


@inherit_docstrings
class Training(CollectorConfig, spec_value=CollectorConfig.Type.TRAINING):

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.SUBMITTED_ASSIGNMENTS_COUNT,
        RuleConditionKey.TOTAL_ANSWERS_COUNT,
        RuleConditionKey.CORRECT_ANSWERS_RATE,
        RuleConditionKey.INCORRECT_ANSWERS_RATE,
        RuleConditionKey.NEXT_ASSIGNMENT_AVAILABLE,
    ])


@inherit_docstrings
class UsersAssessment(CollectorConfig, spec_value=CollectorConfig.Type.USERS_ASSESSMENT):
    """This collector helps you to reassign task suites completed by blocked Tolokers.

    The collector can be used with conditions:
    * [PoolAccessRevokedReason](toloka.client.conditions.PoolAccessRevokedReason.md) — The reason why the Toloker has lost access to the pool.
    * [SkillId](toloka.client.conditions.SkillId.md) — The ID of a skill if reason is `SKILL_CHANGE`.

    The collector can be used with actions:
    * [ChangeOverlap](toloka.client.actions.ChangeOverlap.md) changes the overlap of a task suite.

    Example:
        The example shows how to reassign rejected assignments to other Tolokers.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.UsersAssessment(),
        >>>     conditions=[toloka.client.conditions.PoolAccessRevokedReason == toloka.client.conditions.PoolAccessRevokedReason.RESTRICTION],
        >>>     action=toloka.client.actions.ChangeOverlap(delta=1, open_pool=True),
        >>> )
        ...
    """

    _compatible_conditions: ClassVar[FrozenSet[RuleConditionKey]] = frozenset([
        RuleConditionKey.POOL_ACCESS_REVOKED_REASON,
        RuleConditionKey.SKILL_ID,
    ])
