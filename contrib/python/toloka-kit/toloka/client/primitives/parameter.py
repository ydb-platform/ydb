__all__ = [
    'Parameters',
    'IdempotentOperationParameters',
]
import uuid

from .base import BaseTolokaObject
from ...util._codegen import attribute


class Parameters(BaseTolokaObject, kw_only=False):

    def unstructure(self) -> dict:
        return super().unstructure() or {}


class IdempotentOperationParameters(Parameters):
    """Parameters for idempotent operations such as tasks, task suites and user bonuses creation.

    Works only with `async_mode = True`.

    Attributes:
        operation_id: The UUID of the operation that conforms to the [RFC4122 standard](https://tools.ietf.org/html/rfc4122).
            The UUID is used if `async_mode` is `True`.

            Specify UUID to avoid accidental errors like Toloka operation duplication caused by network problems.
            If you send several requests with the same `operation_id`, Toloka performs the operation only once.
        async_mode: Request processing mode:
            * `True` — Asynchronous operation is started internally.
            * `False` — The request is processed synchronously.

            Default value: `True`.
    """
    operation_id: uuid.UUID = attribute(factory=uuid.uuid4)
    async_mode: bool = attribute(default=True)
