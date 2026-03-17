__all__ = ['Training']
import datetime
from enum import unique
from typing import Dict, List

from .owner import Owner
from .primitives.base import BaseTolokaObject
from ..util._codegen import attribute, codegen_attr_attributes_setters
from ..util._extendable_enum import ExtendableStrEnum


@codegen_attr_attributes_setters
class Training(BaseTolokaObject):
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

    @unique
    class CloseReason(ExtendableStrEnum):
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

    @unique
    class Status(ExtendableStrEnum):
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

    project_id: str
    private_name: str
    may_contain_adult_content: bool
    assignment_max_duration_seconds: int
    mix_tasks_in_creation_order: bool
    shuffle_tasks_in_task_suite: bool
    training_tasks_in_task_suite_count: int
    task_suites_required_to_pass: int
    retry_training_after_days: int
    inherited_instructions: bool
    public_instructions: str

    metadata: Dict[str, List[str]]
    owner: Owner

    # Readonly
    id: str = attribute(readonly=True)
    status: Status = attribute(readonly=True)
    last_close_reason: CloseReason = attribute(readonly=True)
    created: datetime.datetime = attribute(readonly=True)
    last_started: datetime.datetime = attribute(readonly=True)
    last_stopped: datetime.datetime = attribute(readonly=True)

    def is_open(self) -> bool:
        return self.status == Training.Status.OPEN

    def is_closed(self) -> bool:
        return self.status == Training.Status.CLOSED

    def is_archived(self) -> bool:
        return self.status == Training.Status.ARCHIVED

    def is_locked(self) -> bool:
        return self.status == Training.Status.LOCKED
