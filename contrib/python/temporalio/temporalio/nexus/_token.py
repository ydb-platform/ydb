from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Generic

from nexusrpc import OutputT
from typing_extensions import Self


class OperationTokenType(IntEnum):
    """Type discriminator for Nexus operation tokens."""

    WORKFLOW = 1
    ACTIVITY = 2
    UPDATE_WORKFLOW = 3


if TYPE_CHECKING:
    import temporalio.client


@dataclass(frozen=True, kw_only=True)
class OperationToken:
    """Serializable token identifying a Nexus operation target."""

    version: int | None = None
    type: OperationTokenType
    namespace: str
    workflow_id: str | None = None
    activity_id: str | None = None
    run_id: str | None = None
    update_id: str | None = None

    def encode(self) -> str:
        """Convert handle to a base64url-encoded token string."""
        token_details: dict[str, Any] = {
            "t": self.type,
            "ns": self.namespace,
        }
        if self.workflow_id is not None:
            token_details["wid"] = self.workflow_id
        if self.activity_id is not None:
            token_details["aid"] = self.activity_id
        if self.version is not None:
            token_details["v"] = self.version
        if self.run_id is not None:
            token_details["rid"] = self.run_id
        if self.update_id is not None:
            token_details["uid"] = self.update_id
        return _base64url_encode_no_padding(
            json.dumps(
                token_details,
                separators=(",", ":"),
            ).encode("utf-8")
        )

    @classmethod
    def decode(cls, token: str) -> Self:
        """Decodes and validates a token from its base64url-encoded string representation."""
        if not token:
            raise TypeError("invalid token: token is empty")
        try:
            decoded_bytes = _base64url_decode_no_padding(token)
        except Exception as err:
            raise TypeError("failed to decode token as base64url") from err
        try:
            token_details = json.loads(decoded_bytes.decode("utf-8"))
        except Exception as err:
            raise TypeError("failed to unmarshal operation token") from err

        if not isinstance(token_details, dict):
            raise TypeError(f"invalid token: expected dict, got {type(token_details)}")

        raw_token_type = token_details.get("t")
        if not isinstance(raw_token_type, int):
            raise TypeError(
                f"invalid token: expected token type to be an int, got {type(raw_token_type)}"
            )

        try:
            token_type = OperationTokenType(raw_token_type)
        except ValueError as err:
            raise TypeError(
                f"invalid token: unknown token type, got {raw_token_type}.",
                f"Valid values: {', '.join([f'{t.value} ({t.name})' for t in OperationTokenType])}",
            ) from err

        version = token_details.get("v")
        if version is not None and not isinstance(version, int):
            raise TypeError(
                f"invalid token: expected version to be an int or null, got {type(version)}"
            )

        workflow_id = token_details.get("wid")
        if workflow_id is not None and not isinstance(workflow_id, str):
            raise TypeError(
                f"invalid token: expected workflow id to be a string, got {type(workflow_id)}"
            )

        if (
            token_type == OperationTokenType.WORKFLOW
            or token_type == OperationTokenType.UPDATE_WORKFLOW
        ):
            if not workflow_id:
                raise TypeError(
                    f"invalid token: expected non-empty workflow id for token type `{token_type.name}`"
                )

        activity_id = token_details.get("aid")
        if activity_id is not None and not isinstance(activity_id, str):
            raise TypeError(
                f"invalid token: expected activity id to be a string, got {type(activity_id)}"
            )

        if token_type == OperationTokenType.ACTIVITY and not activity_id:
            raise TypeError(
                f"invalid token: expected non-empty activity id for token type `{token_type.name}`"
            )

        update_id = token_details.get("uid")
        if not isinstance(update_id, str | None):
            raise TypeError(
                f"invalid token: expected update_id to be a string or None, got {type(update_id)}"
            )

        if token_type == OperationTokenType.UPDATE_WORKFLOW and not update_id:
            raise TypeError(
                "invalid token: expected non-empty update id for token type `UPDATE_WORKFLOW`"
            )

        namespace = token_details.get("ns")
        if not isinstance(namespace, str):
            # Allow empty string for ns, but it must be present and a string
            raise TypeError(
                f"invalid token: expected namespace to be a string, got {type(namespace)}"
            )

        run_id = token_details.get("rid")
        if not isinstance(run_id, str | None):
            raise TypeError(
                f"invalid token: expected run_id to be a string or None, got {type(run_id)}"
            )

        return cls(
            type=OperationTokenType(token_type),
            namespace=namespace,
            workflow_id=workflow_id,
            activity_id=activity_id,
            run_id=run_id,
            version=version,
            update_id=update_id,
        )


@dataclass(frozen=True)
class WorkflowHandle(Generic[OutputT]):
    """A handle to a workflow that is backing a Nexus operation.

    Do not instantiate this directly. Use
    :py:func:`temporalio.nexus.WorkflowRunOperationContext.start_workflow` to create a
    handle.
    """

    namespace: str
    workflow_id: str
    # Version of the token. Treated as v1 if missing. This field is not included in the
    # serialized token; it's only used to reject newer token versions on load.
    version: int | None = None

    def _to_client_workflow_handle(
        self,
        client: temporalio.client.Client,
        result_type: type[OutputT] | None = None,
    ) -> temporalio.client.WorkflowHandle[Any, OutputT]:
        """Create a :py:class:`temporalio.client.WorkflowHandle` from the token."""
        if client.namespace != self.namespace:
            raise ValueError(
                f"Client namespace {client.namespace} does not match "
                f"operation token namespace {self.namespace}"
            )
        return client.get_workflow_handle(self.workflow_id, result_type=result_type)

    @classmethod
    def _unsafe_from_client_workflow_handle(
        cls, workflow_handle: temporalio.client.WorkflowHandle[Any, OutputT]
    ) -> WorkflowHandle[OutputT]:
        """Create a :py:class:`WorkflowHandle` from a :py:class:`temporalio.client.WorkflowHandle`.

        This is a private method not intended to be used by users. It does not check
        that the supplied client.WorkflowHandle references a workflow that has been
        instrumented to supply the result of a Nexus operation.
        """
        return cls(
            namespace=workflow_handle._client.namespace,
            workflow_id=workflow_handle.id,
        )

    def to_token(self) -> str:
        """Convert handle to a base64url-encoded token string."""
        return OperationToken(
            type=OperationTokenType.WORKFLOW,
            namespace=self.namespace,
            workflow_id=self.workflow_id,
        ).encode()

    @classmethod
    def from_token(cls, token: str) -> WorkflowHandle[OutputT]:
        """Decodes and validates a token from its base64url-encoded string representation."""
        op_token = OperationToken.decode(token)
        if op_token.type != OperationTokenType.WORKFLOW:
            raise TypeError(
                f"invalid workflow token type: {op_token.type}, expected: {OperationTokenType.WORKFLOW}"
            )

        if not op_token.workflow_id:
            raise TypeError("invalid workflow token: missing workflow id")

        if op_token.version is not None and op_token.version != 0:
            raise TypeError(
                "invalid workflow token: 'v' field, if present, must be 0 or null/absent"
            )

        return cls(
            namespace=op_token.namespace,
            workflow_id=op_token.workflow_id,
            version=op_token.version,
        )


def _base64url_encode_no_padding(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


_base64_url_alphabet = set(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
)


def _base64url_decode_no_padding(s: str) -> bytes:
    if invalid_chars := set(s) - _base64_url_alphabet:
        raise ValueError(
            f"invalid base64URL encoded string: contains invalid characters: {invalid_chars}"
        )
    padding = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + padding)
