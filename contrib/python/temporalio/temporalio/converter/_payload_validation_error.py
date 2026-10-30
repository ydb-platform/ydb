"""Payload validation error helpers."""

from typing import Any

import temporalio.exceptions


def create_payload_validation_error(
    details: Any,
) -> temporalio.exceptions.ApplicationError:
    """Create an error indicating that a converted payload failed validation.

    Args:
        details: Structured details describing the validation failure, or
            ``None`` to omit details from the error.

    Returns:
        A non-retryable application error with the reserved payload validation
        failure type.
    """
    error_details: tuple[Any, ...] = () if details is None else (details,)
    return temporalio.exceptions.ApplicationError(
        "Payload validation failed",
        *error_details,
        type="PayloadValidationError",
        non_retryable=True,
    )
