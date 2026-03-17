__all__ = [
    'OperationType',
    'Operation',
    'AnalyticsOperation',
    'PoolOperation',
    'PoolArchiveOperation',
    'PoolCloneOperation',
    'PoolCloseOperation',
    'PoolOpenOperation',
    'TrainingOperation',
    'TrainingArchiveOperation',
    'TrainingCloneOperation',
    'TrainingCloseOperation',
    'TrainingOpenOperation',
    'ProjectArchiveOperation',
    'TasksCreateOperation',
    'TaskSuiteCreateBatchOperation',
    'AggregatedSolutionOperation',
    'UserBonusCreateBatchOperation'
]
import datetime
from enum import unique
from typing import Any, ClassVar

from .exceptions import FailedOperation
from .primitives.base import BaseTolokaObject
from ..util._codegen import attribute
from ..util._docstrings import inherit_docstrings
from ..util._extendable_enum import ExtendableStrEnum


@unique
class OperationType(ExtendableStrEnum):
    PSEUDO = 'PSEUDO.PSEUDO'
    ANALYTICS = 'ANALYTICS'
    KNOWN_SOLUTIONS_GENERATE = 'KNOWN_SOLUTIONS.GENERATE'
    POOL_ARCHIVE = 'POOL.ARCHIVE'
    POOL_CLONE = 'POOL.CLONE'
    POOL_CLOSE = 'POOL.CLOSE'
    POOL_OPEN = 'POOL.OPEN'
    PROJECT_ARCHIVE = 'PROJECT.ARCHIVE'
    SOLUTION_AGGREGATE = 'SOLUTION.AGGREGATE'
    TASK_BATCH_CREATE = 'TASK.BATCH_CREATE'
    TASK_SUITE_BATCH_CREATE = 'TASK_SUITE.BATCH_CREATE'
    TRAINING_ARCHIVE = 'TRAINING.ARCHIVE'
    TRAINING_CLONE = 'TRAINING.CLONE'
    TRAINING_CLOSE = 'TRAINING.CLOSE'
    TRAINING_OPEN = 'TRAINING.OPEN'
    USER_BONUS_BATCH_CREATE = 'USER_BONUS.BATCH_CREATE'


class Operation(BaseTolokaObject, spec_enum=OperationType, spec_field='type'):
    """A base class for Toloka operations.

    Some API requests start asynchronous operations in Toloka. Classes derived from `Operation` are used to track them.
    The examples of asynchronous operations are opening a pool, archiving a project, loading multiple tasks.

    Attributes:
        id: The ID of the operation.
        status: The status of the operation.
        submitted: The UTC date and time when the operation was requested.
        parameters: Parameters of the request that started the operation.
        started: The UTC date and time when the operation started.
        finished: The UTC date and time when the operation finished.
        progress: The operation progress as a percentage.
        details: Details of the operation completion.
    """

    @unique
    class Status(ExtendableStrEnum):
        """The status of an operation.

        Attributes:
            PENDING: The operation is not started yet.
            RUNNING: The operation is in progress.
            SUCCESS: The operation completed successfully.
            FAIL: The operation completed with errors.
        """

        PENDING = 'PENDING'
        RUNNING = 'RUNNING'
        SUCCESS = 'SUCCESS'
        FAIL = 'FAIL'

    PENDING = Status.PENDING
    RUNNING = Status.RUNNING
    SUCCESS = Status.SUCCESS
    FAIL = Status.FAIL

    class Parameters(BaseTolokaObject):
        """A base class for operation parameters.
        """

        pass

    PSEUDO_OPERATION_ID: ClassVar[str] = 'PSEUDO_ID'
    DEFAULT_PSEUDO_OPERATION_TYPE: ClassVar[OperationType] = OperationType.PSEUDO

    id: str
    status: Status = attribute(autocast=True)
    submitted: datetime.datetime
    parameters: Parameters
    started: datetime.datetime
    finished: datetime.datetime
    progress: int
    details: Any  # TODO: cannot structure dict.

    def is_completed(self):
        """Checks whether the operation is completed either successfully or not."""
        return self.status in [Operation.Status.SUCCESS, Operation.Status.FAIL]

    def raise_on_fail(self):
        """Raises `FailedOperation` exception if the operation status is `FAIL`. Otherwise does nothing."""
        if self.status == Operation.Status.FAIL:
            raise FailedOperation(operation=self)


# Analytics operations


@inherit_docstrings
class AnalyticsOperation(Operation, spec_value=OperationType.ANALYTICS):
    """Analytics operation.

    The operation is returned by the [get_analytics](toloka.client.TolokaClient.get_analytics.md) method.
    """

    pass


# Pool operations


@inherit_docstrings
class PoolOperation(Operation):
    """A base class for pool operations.

    Attributes:
        parameters: Parameters containing the ID of the pool.
    """

    class Parameters(Operation.Parameters):
        pool_id: str

    parameters: Parameters


@inherit_docstrings
class PoolArchiveOperation(PoolOperation, spec_value=OperationType.POOL_ARCHIVE):
    """Pool archiving operation.

    The operation is returned by the [archive_pool_async](toloka.client.TolokaClient.archive_pool_async.md) method.
    """

    pass


@inherit_docstrings
class PoolCloneOperation(PoolOperation, spec_value=OperationType.POOL_CLONE):
    """Pool cloning operation.

    The operation is returned by the [clone_pool_async](toloka.client.TolokaClient.clone_pool_async.md) method.

    Note, that `parameters.pool_id` contains the ID of the pool that is cloned.
    While `details.pool_id` contains the ID of the new pool created after cloning.

    Attributes:
        details: The details of the operation.
    """

    class Details(PoolOperation.Parameters):
        """
        Attributes:
            pool_id: The ID of the new pool created after cloning.
        """
        pool_id: str

    details: Details


@inherit_docstrings
class PoolCloseOperation(PoolOperation, spec_value=OperationType.POOL_CLOSE):
    """Pool closing operation.

    The operation is returned by the [close_pool_async](toloka.client.TolokaClient.close_pool_async.md) method.
    """

    pass


@inherit_docstrings
class PoolOpenOperation(PoolOperation, spec_value=OperationType.POOL_OPEN):
    """Pool opening operation.

    The operation is returned by the [open_pool_async](toloka.client.TolokaClient.open_pool_async.md) method.
    """

    pass


# Training operations


@inherit_docstrings
class TrainingOperation(Operation):
    """A base class for operations with trainings.

    Attributes:
        parameters: Parameters containing the ID of the training.
    """

    class Parameters(Operation.Parameters):
        """
        Attributes:
            training_id: The ID of the training.
        """
        training_id: str

    parameters: Parameters


@inherit_docstrings
class TrainingArchiveOperation(TrainingOperation, spec_value=OperationType.TRAINING_ARCHIVE):
    """Training archiving operation.

    The operation is returned by the [archive_training_async](toloka.client.TolokaClient.archive_training_async.md) method.
    """

    pass


@inherit_docstrings
class TrainingCloneOperation(TrainingOperation, spec_value=OperationType.TRAINING_CLONE):
    """Training cloning operation.

    The operation is returned by the [clone_training_async](toloka.client.TolokaClient.clone_training_async.md) method.

    Note, that `parameters.training_id` contains the ID of the training that is cloned.
    While `details.training_id` contains the ID of the new training created after cloning.

    Attributes:
        details: The details of the operation.
    """

    class Details(TrainingOperation.Parameters):
        """
        Attributes:
            training_id: The ID of the new training created after cloning.
        """
        training_id: str

    details: Details


@inherit_docstrings
class TrainingCloseOperation(TrainingOperation, spec_value=OperationType.TRAINING_CLOSE):
    """Training closing operation.

    The operation is returned by the [close_training_async](toloka.client.TolokaClient.close_training_async.md) method.
    """

    pass


@inherit_docstrings
class TrainingOpenOperation(TrainingOperation, spec_value=OperationType.TRAINING_OPEN):
    """Training opening operation.

    The operation is returned by the [open_training_async](toloka.client.TolokaClient.open_training_async.md) method.
    """

    pass


# Project operations


@inherit_docstrings
class ProjectArchiveOperation(Operation, spec_value=OperationType.PROJECT_ARCHIVE):
    """Project archiving operation.

    The operation is returned by the [archive_project_async](toloka.client.TolokaClient.archive_project_async.md) method.

    Attributes:
        parameters: Parameters with the ID of the project.
    """

    class Parameters(Operation.Parameters):
        """
        Attributes:
            project_id: The ID of the project.
        """
        project_id: str

    parameters: Parameters


# Task operations


@inherit_docstrings
class TasksCreateOperation(Operation, spec_value=OperationType.TASK_BATCH_CREATE):
    """Task creating operation.

    The operation is returned by the [create_tasks_async](toloka.client.TolokaClient.create_tasks_async.md) method.

    Attributes:
        parameters: Parameters passed to the `create_tasks_async` method.
        finished: The UTC date and time when the operation was completed.
        details: Details of the operation completion.

    """

    class Parameters(Operation.Parameters):
        """Parameters passed to the [create_tasks_async](toloka.client.TolokaClient.create_tasks_async.md) method.

        Attributes:
            skip_invalid_items: Task validation parameter.
            allow_defaults: Active overlap parameter.
            open_pool: Opening the pool immediately.
        """

        skip_invalid_items: bool
        allow_defaults: bool
        open_pool: bool

    parameters: Parameters
    finished: datetime.datetime
    details: Any


# TaskSuit operations


@inherit_docstrings
class TaskSuiteCreateBatchOperation(Operation, spec_value=OperationType.TASK_SUITE_BATCH_CREATE):
    """Task suite creating operation.

    The operation is returned by the [create_task_suites_async](toloka.client.TolokaClient.create_task_suites_async.md) method.

    Attributes:
        parameters: Parameters passed to the `create_task_suites_async` method.
        finished: The UTC date and time when the operation was completed.
        details: Details of the operation completion.
    """

    class Parameters(Operation.Parameters):
        """Parameters passed to the [create_task_suites_async](toloka.client.TolokaClient.create_task_suites_async.md) method.

        Attributes:
            skip_invalid_items: Task validation parameter.
            allow_defaults: Active overlap parameter.
            open_pool: Opening the pool immediately.
        """
        skip_invalid_items: bool
        allow_defaults: bool
        open_pool: bool

    parameters: Parameters
    finished: datetime.datetime
    details: Any


# Aggregation


@inherit_docstrings
class AggregatedSolutionOperation(Operation, spec_value=OperationType.SOLUTION_AGGREGATE):
    """Response aggregation operation.

    The operation is returned by the [aggregate_solutions_by_pool](toloka.client.TolokaClient.aggregate_solutions_by_pool.md) method.

    Attributes:
        parameters: Parameters containing the ID of the pool.
    """

    class Parameters(Operation.Parameters):
        pool_id: str

    parameters: Parameters


# UserBonus


@inherit_docstrings
class UserBonusCreateBatchOperation(Operation, spec_value=OperationType.USER_BONUS_BATCH_CREATE):
    """Issuing payments operation.

    The operation is returned by the [create_user_bonuses_async](toloka.client.TolokaClient.create_user_bonuses_async.md) method.

    Attributes:
        parameters: Parameters of the `create_user_bonuses_async` request that started the operation.
        details: The details of the operation.
    """

    class Parameters(Operation.Parameters):
        skip_invalid_items: bool

    class Details(PoolOperation.Parameters):
        """The details of the `UserBonusCreateBatchOperation` operation.

        Attributes:
            total_count: The total number of `UserBonus` objects in the request.
            valid_count: The number of `UserBonus` objects that passed validation.
            not_valid_count: The number of `UserBonus` objects that didn't pass validation.
            success_count: The number of `UserBonus` that were issued to Tolokers.
            failed_count: The number of `UserBonus` that were not issued to Tolokers.
        """
        total_count: int
        valid_count: int
        not_valid_count: int
        success_count: int
        failed_count: int

    parameters: Parameters
    details: Details
