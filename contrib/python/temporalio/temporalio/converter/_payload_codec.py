"""PayloadCodec and failure payload traversal."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Sequence

import temporalio.api.common.v1
import temporalio.api.failure.v1


class PayloadCodec(ABC):
    """Codec for encoding/decoding to/from bytes.

    Commonly used for compression or encryption.
    """

    @abstractmethod
    async def encode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        """Encode the given payloads.

        Args:
            payloads: Payloads to encode. This value should not be mutated.

        Returns:
            Encoded payloads. Note, this does not have to be the same number as
            payloads given, but must be at least one and cannot be more than was
            given.
        """
        raise NotImplementedError

    @abstractmethod
    async def decode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        """Decode the given payloads.

        Args:
            payloads: Payloads to decode. This value should not be mutated.

        Returns:
            Decoded payloads. Note, this does not have to be the same number as
            payloads given, but must be at least one and cannot be more than was
            given.
        """
        raise NotImplementedError

    async def encode_wrapper(self, payloads: temporalio.api.common.v1.Payloads) -> None:
        """:py:meth:`encode` for the
        :py:class:`temporalio.api.common.v1.Payloads` wrapper.

        This replaces the payloads within the wrapper.
        """
        new_payloads = await self.encode(payloads.payloads)
        del payloads.payloads[:]
        # TODO(cretz): Copy too expensive?
        payloads.payloads.extend(new_payloads)

    async def decode_wrapper(self, payloads: temporalio.api.common.v1.Payloads) -> None:
        """:py:meth:`decode` for the
        :py:class:`temporalio.api.common.v1.Payloads` wrapper.

        This replaces the payloads within.
        """
        new_payloads = await self.decode(payloads.payloads)
        del payloads.payloads[:]
        # TODO(cretz): Copy too expensive?
        payloads.payloads.extend(new_payloads)

    async def encode_failure(self, failure: temporalio.api.failure.v1.Failure) -> None:
        """Encode payloads of a failure. Intended as a helper method, not for overriding.
        It is not guaranteed that all failures will be encoded with this method rather
        than encoding the underlying payloads.
        """
        await _apply_to_failure_payloads(failure, self.encode_wrapper)

    async def decode_failure(self, failure: temporalio.api.failure.v1.Failure) -> None:
        """Decode payloads of a failure. Intended as a helper method, not for overriding.
        It is not guaranteed that all failures will be decoded with this method rather
        than decoding the underlying payloads.
        """
        await _apply_to_failure_payloads(failure, self.decode_wrapper)


async def _apply_to_failure_payloads(
    failure: temporalio.api.failure.v1.Failure,
    cb: Callable[[temporalio.api.common.v1.Payloads], Awaitable[None]],
) -> None:
    if failure.HasField("encoded_attributes"):
        # Wrap in payloads and merge back
        payloads = temporalio.api.common.v1.Payloads(
            payloads=[failure.encoded_attributes]
        )
        await cb(payloads)
        failure.encoded_attributes.CopyFrom(payloads.payloads[0])
    if failure.HasField(
        "application_failure_info"
    ) and failure.application_failure_info.HasField("details"):
        await cb(failure.application_failure_info.details)
    elif failure.HasField(
        "timeout_failure_info"
    ) and failure.timeout_failure_info.HasField("last_heartbeat_details"):
        await cb(failure.timeout_failure_info.last_heartbeat_details)
    elif failure.HasField(
        "canceled_failure_info"
    ) and failure.canceled_failure_info.HasField("details"):
        await cb(failure.canceled_failure_info.details)
    elif failure.HasField(
        "reset_workflow_failure_info"
    ) and failure.reset_workflow_failure_info.HasField("last_heartbeat_details"):
        await cb(failure.reset_workflow_failure_info.last_heartbeat_details)
    if failure.HasField("cause"):
        await _apply_to_failure_payloads(failure.cause, cb)
