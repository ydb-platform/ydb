import os
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from markupsafe import escape
from starlette.requests import Request
from starlette.responses import Response
from starlette_admin._types import RequestAction
from starlette_admin.exceptions import FormValidationError

if TYPE_CHECKING:
    from starlette_admin.fields import BaseField


def prettify_class_name(name: str) -> str:
    return re.sub(r"(?<=.)([A-Z])", r" \1", name)


def slugify_class_name(name: str) -> str:
    return "".join(["-" + c.lower() if c.isupper() else c for c in name]).lstrip("-")


def is_empty_file(file: Any) -> bool:
    pos = file.tell()
    file.seek(0, os.SEEK_END)
    size = file.tell()
    file.seek(pos)
    return size == 0


def get_file_icon(mime_type: str) -> str:
    mapping = {
        "image": "fa-file-image",
        "audio": "fa-file-audio",
        "video": "fa-file-video",
        "application/pdf": "fa-file-pdf",
        "application/msword": "fa-file-word",
        "application/vnd.ms-word": "fa-file-word",
        "application/vnd.oasis.opendocument.text": "fa-file-word",
        "application/vnd.openxmlformatsfficedocument.wordprocessingml": "fa-file-word",
        "application/vnd.ms-excel": "fa-file-excel",
        "application/vnd.openxmlformatsfficedocument.spreadsheetml": "fa-file-excel",
        "application/vnd.oasis.opendocument.spreadsheet": "fa-file-excel",
        "application/vnd.ms-powerpoint": "fa-file-powerpoint",
        "application/vnd.openxmlformatsfficedocument.presentationml": (
            "fa-file-powerpoint"
        ),
        "application/vnd.oasis.opendocument.presentation": "fa-file-powerpoint",
        "text/plain": "fa-file-text",
        "text/html": "fa-file-code",
        "text/csv": "fa-file-csv",
        "application/json": "fa-file-code",
        "application/gzip": "fa-file-archive",
        "application/zip": "fa-file-archive",
    }
    if mime_type:
        for key, _ in mapping.items():
            if key in mime_type:
                return mapping[key]
    return "fa-file"


def html_params(kwargs: Dict[str, Any]) -> str:
    """Converts a dictionary of HTML attribute name-value pairs into a string of HTML parameters."""
    params = []
    for k, v in kwargs.items():
        if v is None or v is False:
            continue
        if v is True:
            params.append(k)
        else:
            params.append('{}="{}"'.format(str(k).replace("_", "-"), escape(v)))
    return " ".join(params)


def extract_fields(
    fields: Sequence["BaseField"], action: RequestAction = RequestAction.LIST
) -> Sequence["BaseField"]:
    """Extract fields based on the requested action and exclude flags."""
    arr = []
    for field in fields:
        if (
            (action == RequestAction.LIST and field.exclude_from_list)
            or (action == RequestAction.DETAIL and field.exclude_from_detail)
            or (action == RequestAction.CREATE and field.exclude_from_create)
            or (action == RequestAction.EDIT and field.exclude_from_edit)
        ):
            continue
        arr.append(field)
    return arr


def pydantic_error_to_form_validation_errors(exc: Any) -> FormValidationError:
    """Convert Pydantic Error to FormValidationError"""
    from pydantic import ValidationError

    assert isinstance(exc, ValidationError)
    errors: Dict[Union[str, int], Any] = {}
    for pydantic_error in exc.errors():
        loc: Tuple[Union[int, str], ...] = pydantic_error["loc"]
        _d = errors
        for i in range(len(loc)):
            if i == len(loc) - 1:
                _d[loc[i]] = pydantic_error["msg"]
            elif loc[i] not in _d:
                _d[loc[i]] = {}
            _d = _d[loc[i]]
    return FormValidationError(errors)


def wrap_endpoint_with_kwargs(
    endpoint: Callable[..., Awaitable[Response]], **kwargs: Any
) -> Callable[[Request], Awaitable[Response]]:
    """
    Wraps an endpoint function with additional keyword arguments.
    """

    async def wrapper(request: Request) -> Response:
        return await endpoint(request=request, **kwargs)

    return wrapper


T = TypeVar("T")


def not_none(value: Optional[T]) -> T:
    """
    Safely retrieve a value that might be None and raise a ValueError if it is None.

    Args:
        value (Optional[T]): The value that might be None.

    Returns:
        T: The value if it is not None.

    Raises:
        ValueError: If the value is None.
    """
    if value is not None:
        return value
    raise ValueError("Value can not be None")  # pragma: no cover
