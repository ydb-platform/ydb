from __future__ import annotations

import json
from typing import Any, Literal, TypeVar

from django.core.serializers.json import DjangoJSONEncoder
from django.http import HttpResponse
from django.http.response import HttpResponseBase, HttpResponseRedirectBase

HTMX_STOP_POLLING = 286

SwapMethod = Literal[
    "innerHTML",
    "outerHTML",
    "beforebegin",
    "afterbegin",
    "beforeend",
    "afterend",
    "delete",
    "none",
]


class HttpResponseStopPolling(HttpResponse):
    status_code = HTMX_STOP_POLLING

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._reason_phrase = "Stop Polling"


class HttpResponseClientRedirect(HttpResponseRedirectBase):
    status_code = 200

    def __init__(self, redirect_to: str, *args: Any, **kwargs: Any) -> None:
        if kwargs.get("preserve_request"):
            raise ValueError(
                "The 'preserve_request' argument is not supported for "
                "HttpResponseClientRedirect.",
            )
        super().__init__(redirect_to, *args, **kwargs)
        self["HX-Redirect"] = self["Location"]
        del self["Location"]

    @property
    def url(self) -> str:
        return self["HX-Redirect"]


class HttpResponseClientRefresh(HttpResponse):
    def __init__(self) -> None:
        super().__init__()
        self["HX-Refresh"] = "true"


class HttpResponseLocation(HttpResponseRedirectBase):
    status_code = 200

    def __init__(
        self,
        redirect_to: str,
        *args: Any,
        source: str | None = None,
        event: str | None = None,
        target: str | None = None,
        swap: SwapMethod | None = None,
        select: str | None = None,
        values: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(redirect_to, *args, **kwargs)
        spec: dict[str, str | dict[str, str]] = {
            "path": self["Location"],
        }
        del self["Location"]
        if source is not None:
            spec["source"] = source
        if event is not None:
            spec["event"] = event
        if target is not None:
            spec["target"] = target
        if swap is not None:
            spec["swap"] = swap
        if select is not None:
            spec["select"] = select
        if headers is not None:
            spec["headers"] = headers
        if values is not None:
            spec["values"] = values
        self["HX-Location"] = json.dumps(spec)


_HttpResponse = TypeVar("_HttpResponse", bound=HttpResponseBase)


def push_url(response: _HttpResponse, url: str | Literal[False]) -> _HttpResponse:
    response["HX-Push-Url"] = "false" if url is False else url
    return response


def replace_url(response: _HttpResponse, url: str | Literal[False]) -> _HttpResponse:
    response["HX-Replace-Url"] = "false" if url is False else url
    return response


def reswap(response: _HttpResponse, method: SwapMethod) -> _HttpResponse:
    response["HX-Reswap"] = method
    return response


def retarget(response: _HttpResponse, target: str) -> _HttpResponse:
    response["HX-Retarget"] = target
    return response


def reselect(response: _HttpResponse, selector: str) -> _HttpResponse:
    response["HX-Reselect"] = selector
    return response


def trigger_client_event(
    response: _HttpResponse,
    name: str,
    params: dict[str, Any] | None = None,
    *,
    after: Literal["receive", "settle", "swap"] = "receive",
    encoder: type[json.JSONEncoder] = DjangoJSONEncoder,
) -> _HttpResponse:
    params = params or {}

    if after == "receive":
        header = "HX-Trigger"
    elif after == "settle":
        header = "HX-Trigger-After-Settle"
    elif after == "swap":
        header = "HX-Trigger-After-Swap"
    else:
        raise ValueError(
            "Value for 'after' must be one of: 'receive', 'settle', or 'swap'."
        )

    if header in response:
        value = response[header]
        try:
            data = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{header!r} value should be valid JSON.") from exc
        data[name] = params
    else:
        data = {name: params}

    response[header] = json.dumps(data, cls=encoder)

    return response
