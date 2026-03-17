from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Literal, Optional

from nexusrpc import OutputT

OperationTokenType = Literal[1]
OPERATION_TOKEN_TYPE_WORKFLOW: OperationTokenType = 1

if TYPE_CHECKING:
    import temporalio.client


@dataclass(frozen=True)
class WorkflowHandle(Generic[OutputT]):
    """A handle to a workflow that is backing a Nexus operation.

    .. warning::
        This API is experimental and unstable.

    Do not instantiate this directly. Use
    :py:func:`temporalio.nexus.WorkflowRunOperationContext.start_workflow` to create a
    handle.
    """

    namespace: str
    workflow_id: str
    # Version of the token. Treated as v1 if missing. This field is not included in the
    # serialized token; it's only used to reject newer token versions on load.
    version: Optional[int] = None

    def _to_client_workflow_handle(
        self,
        client: temporalio.client.Client,
        result_type: Optional[type[OutputT]] = None,
    ) -> temporalio.client.WorkflowHandle[Any, OutputT]:
        """Create a :py:class:`temporalio.client.WorkflowHandle` from the token."""
        if client.namespace != self.namespace:
            raise ValueError(
                f"Client namespace {client.namespace} does not match "
                f"operation token namespace {self.namespace}"
            )
        return client.get_workflow_handle(self.workflow_id, result_type=result_type)

    # TODO(nexus-preview): The return type here should be dictated by the input workflow
    # handle type.
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
        return _base64url_encode_no_padding(
            json.dumps(
                {
                    "t": OPERATION_TOKEN_TYPE_WORKFLOW,
                    "ns": self.namespace,
                    "wid": self.workflow_id,
                },
                separators=(",", ":"),
            ).encode("utf-8")
        )

    @classmethod
    def from_token(cls, token: str) -> WorkflowHandle[OutputT]:
        """Decodes and validates a token from its base64url-encoded string representation."""
        if not token:
            raise TypeError("invalid workflow token: token is empty")
        try:
            decoded_bytes = _base64url_decode_no_padding(token)
        except Exception as err:
            raise TypeError("failed to decode token as base64url") from err
        try:
            workflow_operation_token = json.loads(decoded_bytes.decode("utf-8"))
        except Exception as err:
            raise TypeError("failed to unmarshal workflow operation token") from err

        if not isinstance(workflow_operation_token, dict):
            raise TypeError(
                f"invalid workflow token: expected dict, got {type(workflow_operation_token)}"
            )

        token_type = workflow_operation_token.get("t")
        if token_type != OPERATION_TOKEN_TYPE_WORKFLOW:
            raise TypeError(
                f"invalid workflow token type: {token_type}, expected: {OPERATION_TOKEN_TYPE_WORKFLOW}"
            )

        version = workflow_operation_token.get("v")
        if version is not None and version != 0:
            raise TypeError(
                "invalid workflow token: 'v' field, if present, must be 0 or null/absent"
            )

        workflow_id = workflow_operation_token.get("wid")
        if not workflow_id or not isinstance(workflow_id, str):
            raise TypeError(
                "invalid workflow token: missing, empty, or non-string workflow ID (wid)"
            )

        namespace = workflow_operation_token.get("ns")
        if namespace is None or not isinstance(namespace, str):
            # Allow empty string for ns, but it must be present and a string
            raise TypeError(
                "invalid workflow token: missing or non-string namespace (ns)"
            )

        return cls(
            namespace=namespace,
            workflow_id=workflow_id,
            version=version,
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
