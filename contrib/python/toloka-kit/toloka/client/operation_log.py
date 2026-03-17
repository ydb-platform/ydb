__all__ = ['OperationLogItem']
from typing import Any, Dict

from .primitives.base import BaseTolokaObject


# TODO: Add children through spec_field type and success.
#  Add simple way of getting different objects of interest for every child.
#  e.g. user_bonus_id for type=USER_BONUS_PERSIST, success=True (https://toloka.ai/docs/api/api-reference/#get-/operations/-id-/log)
class OperationLogItem(BaseTolokaObject):
    """An operation log item.

    If the operation was successful, the log contains the IDs of created objects, otherwise it contains validation errors details.

    Attributes:
        type: The type of the operation.
            * `USER_BONUS_PERSIST` — Successfully issuing a bonus.
            * `TASK_CREATE` — Successfully creating a task.
            * `TASK_SUITE_CREATE` — Successfully creating a task suite.
            * `USER_BONUS_VALIDATE` — Issuing a bonus that failed.
            * `TASK_VALIDATE` — Creating a task that failed.
            * `TASK_SUITE_VALIDATE` — Creating a task suite that failed.
        success: The operation result: success or failure.
        input: Input operation data.
        output: Operation output data. The content depends on the log item `type`.
    """

    type: str
    success: bool

    input: Dict[str, Any]
    output: Dict[str, Any]
