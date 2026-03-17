__all__ = [
    'RuleType',
    'RuleAction',
    'Restriction',
    'RestrictionV2',
    'SetSkillFromOutputField',
    'ChangeOverlap',
    'SetSkill',
    'RejectAllAssignments',
    'ApproveAllAssignments'
]
from enum import unique

from .conditions import RuleConditionKey
from .primitives.base import BaseParameters
from .user_restriction import DurationUnit, UserRestriction
from ..util._codegen import attribute
from ..util._extendable_enum import ExtendableStrEnum


@unique
class RuleType(ExtendableStrEnum):
    RESTRICTION = 'RESTRICTION'
    RESTRICTION_V2 = 'RESTRICTION_V2'
    SET_SKILL_FROM_OUTPUT_FIELD = 'SET_SKILL_FROM_OUTPUT_FIELD'
    CHANGE_OVERLAP = 'CHANGE_OVERLAP'
    SET_SKILL = 'SET_SKILL'
    REJECT_ALL_ASSIGNMENTS = 'REJECT_ALL_ASSIGNMENTS'
    APPROVE_ALL_ASSIGNMENTS = 'APPROVE_ALL_ASSIGNMENTS'


class RuleAction(BaseParameters, spec_enum=RuleType, spec_field='type'):
    """Base class for all actions in quality controls configurations
    """

    pass


class Restriction(RuleAction, spec_value=RuleType.RESTRICTION):
    """Restricts Toloker's access to projects or pools.

    To have better control over restriction period use [RestrictionV2](toloka.client.actions.RestrictionV2.md).

    Attributes:
        parameters.scope:
            * `POOL` — A Toloker can't access the pool if the action is applied.
            * `PROJECT` — A Toloker can't access the entire project containing the pool.
            * `ALL_PROJECTS` — A Toloker can't access any requester's project.
        parameters.duration_days: A blocking period in days. If the `duration_days` is omitted, then the block is permanent.
        parameters.private_comment: A private comment. It is visible only to the requester.

    Example:
        >>> new_pool = toloka.client.pool.Pool()    # pool creation is simplified
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentSubmitTime(history_size=5, fast_submit_threshold_seconds=20),
        >>>     conditions=[toloka.client.conditions.FastSubmittedCount > 1],
        >>>     action=toloka.client.actions.Restriction(
        >>>         scope='PROJECT',
        >>>         duration_days=10,
        >>>     )
        >>> )
        ...
    """

    class Parameters(RuleAction.Parameters):
        scope: UserRestriction.Scope = attribute(autocast=True)
        duration_days: int
        private_comment: str


class RestrictionV2(RuleAction, spec_value=RuleType.RESTRICTION_V2):
    """Restricts Toloker's access to projects or pools.

    Attributes:
        parameters.scope:
            * `POOL` — A Toloker can't access the pool if the action is applied.
            * `PROJECT` — A Toloker can't access the entire project containing the pool.
            * `ALL_PROJECTS` — A Toloker can't access any requester's project.
        parameters.duration: The duration of the blocking period measured in `duration_unit`.
        parameters.duration_unit:
            * `MINUTES`;
            * `HOURS`;
            * `DAYS`;
            * `PERMANENT` — blocking is permanent. In this case the `duration` is ignored and may be omitted.
        parameters.private_comment: A private comment. It is visible only to the requester.

    Example:
        The following quality control rule blocks access to the project for 10 days, if a Toloker answers too fast.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentSubmitTime(history_size=5, fast_submit_threshold_seconds=20),
        >>>     conditions=[toloka.client.conditions.FastSubmittedCount > 1],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope='PROJECT',
        >>>         duration=10,
        >>>         duration_unit='DAYS',
        >>>         private_comment='Fast responses',
        >>>     )
        >>> )
        ...
    """

    class Parameters(RuleAction.Parameters):
        scope: UserRestriction.Scope = attribute(autocast=True)
        duration: int
        duration_unit: DurationUnit = attribute(autocast=True)
        private_comment: str


class SetSkillFromOutputField(RuleAction, spec_value=RuleType.SET_SKILL_FROM_OUTPUT_FIELD):
    """Sets Toloker's skill value to the percentage of correct or incorrect answers.

    You can use this action with [MajorityVote](toloka.client.collectors.MajorityVote.md) and [GoldenSet](toloka.client.collectors.GoldenSet.md) collectors.

    Attributes:
        parameters.skill_id: The ID of the skill to update.
        parameters.from_field: The value to assign to the skill:
            * `correct_answers_rate` — Percentage of correct answers.
            * `incorrect_answers_rate` — Percentage of incorrect answers.

    Example:
        In the following example, a `MajorityVote` collector is used to update a skill value.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.MajorityVote(answer_threshold=2, history_size=10),
        >>>     conditions=[
        >>>         toloka.client.conditions.TotalAnswersCount > 2,
        >>>     ],
        >>>     action=toloka.client.actions.SetSkillFromOutputField(
        >>>         skill_id=some_skill_id,
        >>>         from_field='correct_answers_rate',
        >>>     ),
        >>> )
        ...
    """

    class Parameters(RuleAction.Parameters):
        skill_id: str
        from_field: RuleConditionKey = attribute(autocast=True)


class ChangeOverlap(RuleAction, spec_value=RuleType.CHANGE_OVERLAP):
    """Increases the overlap of a task.

    You can use this rule only with [UsersAssessment](toloka.client.collectors.UsersAssessment) and [AssignmentsAssessment](toloka.client.collectors.AssignmentsAssessment) collectors.

    Attributes:
        parameters.delta: An overlap increment.
        parameters.open_pool:
            * `True` — Open the pool after changing the overlap value.
            * `False` — Don't reopen the pool if it is closed.

    Example:
        The example shows how to increase task overlap when you reject assignments.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentsAssessment(),
        >>>     conditions=[toloka.client.conditions.AssessmentEvent == toloka.client.conditions.AssessmentEvent.REJECT],
        >>>     action=toloka.client.actions.ChangeOverlap(delta=1, open_pool=True),
        >>> )
        ...
    """

    class Parameters(RuleAction.Parameters):
        delta: int
        open_pool: bool


class SetSkill(RuleAction, spec_value=RuleType.SET_SKILL):
    """Sets Toloker's skill value.

    Attributes:
        parameters.skill_id: The ID of the skill.
        parameters.skill_value: The new value of the skill.

    Example:
        When an answer is accepted, the Toloker gets a skill. Later you can filter Tolokers by that skill.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AnswerCount(),
        >>>     conditions=[toloka.client.conditions.AssignmentsAcceptedCount > 0],
        >>>     action=toloka.client.actions.SetSkill(skill_id='11294', skill_value=1),
        >>> )
        ...
    """

    class Parameters(RuleAction.Parameters):
        skill_id: str
        skill_value: int


class RejectAllAssignments(RuleAction, spec_value=RuleType.REJECT_ALL_ASSIGNMENTS):
    """Rejects all Toloker's assignments in the pool. This action is available for pools with non-automatic acceptance.

    Attributes:
        parameters.public_comment: The reason of the rejection. It is visible both to the requester and to the Toloker.

    Example:
        Reject all assignments if a Toloker sends responses too fast. Note, that the pool must be configured with non-automatic response acceptance.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentSubmitTime(history_size=5, fast_submit_threshold_seconds=20),
        >>>     conditions=[toloka.client.conditions.FastSubmittedCount > 3],
        >>>     action=toloka.client.actions.RejectAllAssignments(public_comment='Too fast responses.')
        >>> )
        ...
    """

    class Parameters(RuleAction.Parameters):
        public_comment: str


class ApproveAllAssignments(RuleAction, spec_value=RuleType.APPROVE_ALL_ASSIGNMENTS):
    """Accepts all Toloker's assignments in the pool.

    Example:
        Accept all assignments if a Toloker gives correct responses for control tasks. Note, that the pool must be configured with non-automatic response acceptance.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.GoldenSet(history_size=5),
        >>>     conditions=[toloka.client.conditions.GoldenSetCorrectAnswersRate > 90],
        >>>     action=toloka.client.actions.ApproveAllAssignments()
        >>> )
        ...
    """

    pass
