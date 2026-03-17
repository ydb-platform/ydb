__all__ = [
    'Training',
]
import datetime
import toloka.client.owner
import toloka.client.primitives.base
import toloka.util._extendable_enum
import typing


class Training(toloka.client.primitives.base.BaseTolokaObject):
    """A training.

    A training is a pool containing tasks with known solutions and hints for Tolokers. Use trainings:
    - To train Tolokers so they solve general tasks better.
    - To select Tolokers who successfully completed training tasks and to give them access to a general pool.

    To link a training to a general pool set the
    [Pool](toloka.client.pool.Pool.md).[quality_control](toloka.client.quality_control.QualityControl.md).[training_requirement](toloka.client.quality_control.QualityControl.TrainingRequirement.md)
    parameter.

    For more information, see [Adding a training](https://toloka.ai/docs/guide/train).

    Attributes:
        project_id: The ID of the project containing the training.
        private_name: The training name. It is visible to the requester only.
        may_contain_adult_content: The presence of adult content.
        assignment_max_duration_seconds: Time limit to complete a task suite.
            Take into account loading a page with a task suite and sending responses to the server. It is recommended that you set at least 60 seconds.
        mix_tasks_in_creation_order:
            * `True` — Tasks are grouped in suites in the order they were created.
            * `False` — Tasks are chosen for a task suite in a random order.

            Default: `True`.
        shuffle_tasks_in_task_suite:
            * `True` — Tasks from a task suite are shuffled on the page.
            * `False` — Tasks from a task suite are placed on the page in the order they were created.

            Default: `True`.
        training_tasks_in_task_suite_count: The number of training tasks in one task suite.
        task_suites_required_to_pass: The number of task suites that must be completed by a Toloker to get a training skill.
        retry_training_after_days: The training can be completed again after the specified number of days to update the training skill.
            If the parameter is not specified, then the training skill is issued for an unlimited time.
        inherited_instructions:
            * `True` — Project instructions are used in the training.
            * `False` — Instruction, specified in the `public_instructions` parameter, are used.

            Default: `False`.
        public_instructions: Instructions for Tolokers used when the `inherited_instructions` parameter is `False`. Describe in the instructions how to complete training tasks.
            You can use HTML markup inside `public_instructions`.
        metadata: A dictionary with metadata.
        owner: The training owner.
        id: The ID of the training. Read-only field.
        status: The training status. Read-only field.
        last_close_reason: A reason why the training was closed last time. Read-only field.
        created: The UTC date and time when the training was created. Read-only field.
        last_started: The UTC date and time when the training was started last time. Read-only field.
        last_stopped: The UTC date and time when the training was stopped last time. Read-only field.
    """

    class CloseReason(toloka.util._extendable_enum.ExtendableStrEnum):
        """A reason for closing a training.

        Attributes:
            MANUAL: A training was closed by a requester.
            COMPLETED: All linked pool tasks were completed.
            ASSIGNMENTS_LIMIT_EXCEEDED: A limit of 2 millions assignments is reached.
            BLOCKED: The requester's account was blocked.
        """

        MANUAL = 'MANUAL'
        EXPIRED = 'EXPIRED'
        COMPLETED = 'COMPLETED'
        NOT_ENOUGH_BALANCE = 'NOT_ENOUGH_BALANCE'
        ASSIGNMENTS_LIMIT_EXCEEDED = 'ASSIGNMENTS_LIMIT_EXCEEDED'
        BLOCKED = 'BLOCKED'
        FOR_UPDATE = 'FOR_UPDATE'

    class Status(toloka.util._extendable_enum.ExtendableStrEnum):
        """The status of a training.

        Attributes:
            OPEN: The training is open.
            CLOSED: The training is closed.
            ARCHIVED: The training is archived.
        """

        OPEN = 'OPEN'
        CLOSED = 'CLOSED'
        ARCHIVED = 'ARCHIVED'
        LOCKED = 'LOCKED'

    def is_open(self) -> bool: ...

    def is_closed(self) -> bool: ...

    def is_archived(self) -> bool: ...

    def is_locked(self) -> bool: ...

    def __init__(
        self,
        *,
        project_id: typing.Optional[str] = None,
        private_name: typing.Optional[str] = None,
        may_contain_adult_content: typing.Optional[bool] = None,
        assignment_max_duration_seconds: typing.Optional[int] = None,
        mix_tasks_in_creation_order: typing.Optional[bool] = None,
        shuffle_tasks_in_task_suite: typing.Optional[bool] = None,
        training_tasks_in_task_suite_count: typing.Optional[int] = None,
        task_suites_required_to_pass: typing.Optional[int] = None,
        retry_training_after_days: typing.Optional[int] = None,
        inherited_instructions: typing.Optional[bool] = None,
        public_instructions: typing.Optional[str] = None,
        metadata: typing.Optional[typing.Dict[str, typing.List[str]]] = None,
        owner: typing.Optional[toloka.client.owner.Owner] = None,
        id: typing.Optional[str] = None,
        status: typing.Optional[Status] = None,
        last_close_reason: typing.Optional[CloseReason] = None,
        created: typing.Optional[datetime.datetime] = None,
        last_started: typing.Optional[datetime.datetime] = None,
        last_stopped: typing.Optional[datetime.datetime] = None
    ) -> None:
        """Method generated by attrs for class Training.
        """
        ...

    @typing.overload
    def set_owner(self, owner: toloka.client.owner.Owner):
        """A shortcut setter for owner
        """
        ...

    @typing.overload
    def set_owner(
        self,
        *,
        id: typing.Optional[str] = None,
        myself: typing.Optional[bool] = None,
        company_id: typing.Optional[str] = None
    ):
        """A shortcut setter for owner
        """
        ...

    _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
    project_id: typing.Optional[str]
    private_name: typing.Optional[str]
    may_contain_adult_content: typing.Optional[bool]
    assignment_max_duration_seconds: typing.Optional[int]
    mix_tasks_in_creation_order: typing.Optional[bool]
    shuffle_tasks_in_task_suite: typing.Optional[bool]
    training_tasks_in_task_suite_count: typing.Optional[int]
    task_suites_required_to_pass: typing.Optional[int]
    retry_training_after_days: typing.Optional[int]
    inherited_instructions: typing.Optional[bool]
    public_instructions: typing.Optional[str]
    metadata: typing.Optional[typing.Dict[str, typing.List[str]]]
    owner: typing.Optional[toloka.client.owner.Owner]
    id: typing.Optional[str]
    status: typing.Optional[Status]
    last_close_reason: typing.Optional[CloseReason]
    created: typing.Optional[datetime.datetime]
    last_started: typing.Optional[datetime.datetime]
    last_stopped: typing.Optional[datetime.datetime]
