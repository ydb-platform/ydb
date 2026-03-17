__all__ = [
    'Assignment',
    'AssignmentPatch',
    'GetAssignmentsTsvParameters',
]
import datetime
import decimal
import toloka.client.owner
import toloka.client.primitives.base
import toloka.client.primitives.parameter
import toloka.client.solution
import toloka.client.task
import toloka.util._extendable_enum
import typing


class Assignment(toloka.client.primitives.base.BaseTolokaObject):
    """Information about an assigned task suite.

    Attributes:
        id: The ID of the assignment.
        task_suite_id: The ID of the assigned task suite.
        pool_id: The ID of the pool containing the task suite.
        user_id: The ID of the Toloker who was assigned the task suite.
        status: Status of the assignment.
            * `ACTIVE` — The task suite is assigned but it isn't completed yet.
            * `SUBMITTED` — The task suite is completed but it isn't checked.
            * `ACCEPTED` — The task suite is accepted by the requester.
            * `REJECTED` — The task suite is rejected by the requester.
            * `SKIPPED` — The task suite is skipped by the Toloker.
            * `EXPIRED` — Time for completing the tasks has expired.
        reward: Payment received by the Toloker.
        bonus_ids: IDs of bonuses issued for the task.
        tasks: Data for the tasks.
        automerged: Flag of the response received as a result of merging identical tasks. Value:
            * `True` — The response was recorded when identical tasks were merged.
            * `False` — Normal Toloker response.
        created: The date and time when the task suite was assigned to a Toloker.
        submitted: The date and time when the task suite was completed by a Toloker.
        accepted: The date and time when the responses for the task suite were accepted by the requester.
        rejected: The date and time when the responses for the task suite were rejected by the requester.
        skipped: The date and time when the task suite was skipped by the Toloker.
        expired: The date and time when the time for completing the task suite expired.
        first_declined_solution_attempt: For training tasks. The Toloker's first responses in the training task
            (only if these were the wrong answers). If the Toloker answered correctly on the first try, the
            first_declined_solution_attempt array is omitted.
            Arrays with the responses (output_values) are arranged in the same order as the task data in the tasks array.
        solutions: Toloker responses. Arranged in the same order as the data for tasks in the tasks array.
        mixed: Type of operation for creating a task suite:
            * `True` — Smart mixing was used.
            * `False` — The tasks were grouped manually, smart mixing was not used.
        owner: Properties of Requester.
        public_comment: A public comment that is set when accepting or rejecting the assignment.

    Example:
        >>> for assignment in toloka_client.get_assignments(pool_id='1240045', status='SUBMITTED'):
        >>>     print(assignment.id)
        >>>     for solution in assignment.solutions:
        >>>         print(solution)
        ...
    """

    class Status(toloka.util._extendable_enum.ExtendableStrEnum):
        """The status of an assigned task suite.

        Attributes:
            ACTIVE: The task suite is assigned but it isn't completed yet.
            SUBMITTED: The task suite is completed but it isn't checked.
            ACCEPTED: The task suite is accepted by the requester.
            REJECTED: The task suite is rejected by the requester.
            SKIPPED: The task suite is skipped by the Toloker.
            EXPIRED: Time for completing the tasks has expired.
        """

        ACTIVE = 'ACTIVE'
        SUBMITTED = 'SUBMITTED'
        ACCEPTED = 'ACCEPTED'
        REJECTED = 'REJECTED'
        SKIPPED = 'SKIPPED'
        EXPIRED = 'EXPIRED'

    def __init__(
        self,
        *,
        id: typing.Optional[str] = None,
        task_suite_id: typing.Optional[str] = None,
        pool_id: typing.Optional[str] = None,
        user_id: typing.Optional[str] = None,
        status: typing.Union[Status, str, None] = None,
        reward: typing.Optional[decimal.Decimal] = None,
        bonus_ids: typing.Optional[typing.List[str]] = None,
        tasks: typing.Optional[typing.List[toloka.client.task.Task]] = None,
        automerged: typing.Optional[bool] = None,
        created: typing.Optional[datetime.datetime] = None,
        submitted: typing.Optional[datetime.datetime] = None,
        accepted: typing.Optional[datetime.datetime] = None,
        rejected: typing.Optional[datetime.datetime] = None,
        skipped: typing.Optional[datetime.datetime] = None,
        expired: typing.Optional[datetime.datetime] = None,
        first_declined_solution_attempt: typing.Optional[typing.List[toloka.client.solution.Solution]] = None,
        solutions: typing.Optional[typing.List[toloka.client.solution.Solution]] = None,
        mixed: typing.Optional[bool] = None,
        owner: typing.Optional[toloka.client.owner.Owner] = None,
        public_comment: typing.Optional[str] = None
    ) -> None:
        """Method generated by attrs for class Assignment.
        """
        ...

    _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
    id: typing.Optional[str]
    task_suite_id: typing.Optional[str]
    pool_id: typing.Optional[str]
    user_id: typing.Optional[str]
    status: typing.Optional[Status]
    reward: typing.Optional[decimal.Decimal]
    bonus_ids: typing.Optional[typing.List[str]]
    tasks: typing.Optional[typing.List[toloka.client.task.Task]]
    automerged: typing.Optional[bool]
    created: typing.Optional[datetime.datetime]
    submitted: typing.Optional[datetime.datetime]
    accepted: typing.Optional[datetime.datetime]
    rejected: typing.Optional[datetime.datetime]
    skipped: typing.Optional[datetime.datetime]
    expired: typing.Optional[datetime.datetime]
    first_declined_solution_attempt: typing.Optional[typing.List[toloka.client.solution.Solution]]
    solutions: typing.Optional[typing.List[toloka.client.solution.Solution]]
    mixed: typing.Optional[bool]
    owner: typing.Optional[toloka.client.owner.Owner]
    public_comment: typing.Optional[str]


class AssignmentPatch(toloka.client.primitives.base.BaseTolokaObject):
    """The new status of an assignment.

    It is used in the [patch_assignment](toloka.client.TolokaClient.patch_assignment.md) method to accept or reject an assignment and to leave a comment.

    Attributes:
        public_comment: The public comment.
        status: The new status of an assignment:
            * `ACCEPTED` — Accepted by the requester.
            * `REJECTED` — Rejected by the requester.
    """

    def __init__(
        self,
        *,
        public_comment: typing.Optional[str] = None,
        status: typing.Optional[Assignment.Status] = None
    ) -> None:
        """Method generated by attrs for class AssignmentPatch.
        """
        ...

    _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
    public_comment: typing.Optional[str]
    status: typing.Optional[Assignment.Status]


class GetAssignmentsTsvParameters(toloka.client.primitives.parameter.Parameters):
    """Parameters for downloading assignments.

    These parameters are used in the [TolokaClient.get_assignments_df](toloka.client.TolokaClient.get_assignments_df.md) method.

    Attributes:
        status: Statuses of assignments to download.
        start_time_from: Download assignments submitted after the specified date and time.
        start_time_to: Download assignments submitted before the specified date and time.
        exclude_banned: Exclude answers from banned Tolokers, even if their assignments have suitable status.
        field: Names of `Assignment` fields to be downloaded. Fields other then from `Assignment` class are always downloaded.
    """

    class Status(toloka.util._extendable_enum.ExtendableStrEnum):
        """An enumeration.
        """

        ACTIVE = 'ACTIVE'
        SUBMITTED = 'SUBMITTED'
        APPROVED = 'APPROVED'
        REJECTED = 'REJECTED'
        SKIPPED = 'SKIPPED'
        EXPIRED = 'EXPIRED'

    class Field(toloka.util._extendable_enum.ExtendableStrEnum):
        """An enumeration.
        """

        LINK = 'ASSIGNMENT:link'
        ASSIGNMENT_ID = 'ASSIGNMENT:assignment_id'
        TASK_ID = 'ASSIGNMENT:task_id'
        TASK_SUITE_ID = 'ASSIGNMENT:task_suite_id'
        WORKER_ID = 'ASSIGNMENT:worker_id'
        STATUS = 'ASSIGNMENT:status'
        STARTED = 'ASSIGNMENT:started'
        SUBMITTED = 'ASSIGNMENT:submitted'
        ACCEPTED = 'ASSIGNMENT:accepted'
        REJECTED = 'ASSIGNMENT:rejected'
        SKIPPED = 'ASSIGNMENT:skipped'
        EXPIRED = 'ASSIGNMENT:expired'
        REWARD = 'ASSIGNMENT:reward'

    @classmethod
    def structure(cls, data): ...

    def unstructure(self) -> dict: ...

    def __init__(
        self,
        *,
        status: typing.Optional[typing.List[Status]] = ...,
        start_time_from: typing.Optional[datetime.datetime] = None,
        start_time_to: typing.Optional[datetime.datetime] = None,
        exclude_banned: typing.Optional[bool] = None,
        field: typing.Optional[typing.List[Field]] = ...
    ) -> None:
        """Method generated by attrs for class GetAssignmentsTsvParameters.
        """
        ...

    _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
    status: typing.Optional[typing.List[Status]]
    start_time_from: typing.Optional[datetime.datetime]
    start_time_to: typing.Optional[datetime.datetime]
    exclude_banned: typing.Optional[bool]
    field: typing.Optional[typing.List[Field]]
