from stripe._requestor_options import RequestorOptions
from typing import Mapping, Optional, Dict, Tuple, Any
from typing_extensions import NotRequired, TypedDict, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._stripe_context import StripeContext


class RequestOptions(TypedDict):
    api_key: NotRequired["str|None"]
    stripe_version: NotRequired["str|None"]
    stripe_account: NotRequired["str|None"]
    stripe_context: NotRequired["str | StripeContext | None"]
    max_network_retries: NotRequired["int|None"]
    idempotency_key: NotRequired["str|None"]
    content_type: NotRequired["str|None"]
    headers: NotRequired["Mapping[str, str]|None"]


def merge_options(
    requestor: RequestorOptions,
    request: Optional[RequestOptions],
) -> RequestOptions:
    """
    Merge a client and request object, giving precedence to the values from
    the request object.
    """
    if request is None:
        return {
            "api_key": requestor.api_key,
            "stripe_account": requestor.stripe_account,
            "stripe_context": requestor.stripe_context,
            "stripe_version": requestor.stripe_version,
            "max_network_retries": requestor.max_network_retries,
            "idempotency_key": None,
            "content_type": None,
            "headers": None,
        }

    return {
        "api_key": request.get("api_key") or requestor.api_key,
        "stripe_account": request.get("stripe_account")
        or requestor.stripe_account,
        "stripe_context": request.get("stripe_context")
        or requestor.stripe_context,
        "stripe_version": request.get("stripe_version")
        or requestor.stripe_version,
        "max_network_retries": request.get("max_network_retries")
        if request.get("max_network_retries") is not None
        else requestor.max_network_retries,
        "idempotency_key": request.get("idempotency_key"),
        "content_type": request.get("content_type"),
        "headers": request.get("headers"),
    }


PERSISTENT_OPTIONS_KEYS = {
    "api_key",
    "stripe_version",
    "stripe_account",
    "stripe_context",
}
"""
These are the keys in RequestOptions that should persist across requests made
by the same requestor.
"""


def extract_options_from_dict(
    d: Optional[Mapping[str, Any]],
) -> Tuple[RequestOptions, Dict[str, Any]]:
    """
    Extracts a RequestOptions object from a dict, and returns a tuple of
    the RequestOptions object and the remaining dict.
    """
    if not d:
        return {}, {}
    options: RequestOptions = {}
    d_copy = dict(d)
    for key in [
        "api_key",
        "stripe_version",
        "stripe_account",
        "stripe_context",
        "max_network_retries",
        "idempotency_key",
        "content_type",
        "headers",
    ]:
        if key in d_copy:
            options[key] = d_copy.pop(key)

    return options, d_copy
