import email
from datetime import datetime
from email.message import Message
from typing import (
    Any,
    Dict,
    List,
    Literal,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import parse_qsl

import httpx


class MultiItems(dict):
    def get_list(self, key: str) -> List[Any]:
        try:
            return [self[key]]
        except KeyError:  # pragma: no cover
            return []

    def multi_items(self) -> List[Tuple[str, Any]]:
        return list(self.items())


def _parse_multipart_form_data(
    content: bytes, *, content_type: str, encoding: str
) -> Tuple[MultiItems, MultiItems]:
    form_data = b"\r\n".join(
        (
            b"MIME-Version: 1.0",
            b"Content-Type: " + content_type.encode(encoding),
            b"\r\n" + content,
        )
    )
    data = MultiItems()
    files = MultiItems()
    for payload in email.message_from_bytes(form_data).get_payload():
        payload = cast(Message, payload)
        name = payload.get_param("name", header="Content-Disposition")
        filename = payload.get_filename()
        content_type = payload.get_content_type()
        value = payload.get_payload(decode=True)
        assert isinstance(value, bytes)
        if content_type.startswith("text/") and filename is None:
            # Text field
            data[name] = value.decode(payload.get_content_charset() or "utf-8")
        else:
            # File field
            files[name] = filename, value

    return data, files


def _parse_urlencoded_data(content: bytes, *, encoding: str) -> MultiItems:
    return MultiItems(
        (key, value)
        for key, value in parse_qsl(content.decode(encoding), keep_blank_values=True)
    )


def decode_data(request: httpx.Request) -> Tuple[MultiItems, MultiItems]:
    content = request.read()
    content_type = request.headers.get("Content-Type", "")

    if content_type.startswith("multipart/form-data"):
        data, files = _parse_multipart_form_data(
            content,
            content_type=content_type,
            encoding=request.headers.encoding,
        )
    else:
        data = _parse_urlencoded_data(
            content,
            encoding=request.headers.encoding,
        )
        files = MultiItems()

    return data, files


Self = TypeVar("Self", bound="SetCookie")


class SetCookie(
    NamedTuple(
        "SetCookie",
        [
            ("header_name", Literal["Set-Cookie"]),
            ("header_value", str),
        ],
    )
):
    def __new__(
        cls: Type[Self],
        name: str,
        value: str,
        *,
        path: Optional[str] = None,
        domain: Optional[str] = None,
        expires: Optional[Union[str, datetime]] = None,
        max_age: Optional[int] = None,
        http_only: bool = False,
        same_site: Optional[Literal["Strict", "Lax", "None"]] = None,
        secure: bool = False,
        partitioned: bool = False,
    ) -> Self:
        """
        https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Set-Cookie#syntax
        """
        attrs: Dict[str, Union[str, bool]] = {name: value}
        if path is not None:
            attrs["Path"] = path
        if domain is not None:
            attrs["Domain"] = domain
        if expires is not None:
            if isinstance(expires, datetime):  # pragma: no branch
                expires = expires.strftime("%a, %d %b %Y %H:%M:%S GMT")
            attrs["Expires"] = expires
        if max_age is not None:
            attrs["Max-Age"] = str(max_age)
        if http_only:
            attrs["HttpOnly"] = True
        if same_site is not None:
            attrs["SameSite"] = same_site
            if same_site == "None":  # pragma: no branch
                secure = True
        if secure:
            attrs["Secure"] = True
        if partitioned:
            attrs["Partitioned"] = True

        string = "; ".join(
            _name if _value is True else f"{_name}={_value}"
            for _name, _value in attrs.items()
        )
        self = super().__new__(cls, "Set-Cookie", string)
        return self
