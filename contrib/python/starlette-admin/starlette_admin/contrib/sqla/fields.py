from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Union

from starlette.requests import Request
from starlette_admin._types import RequestAction
from starlette_admin.contrib.sqla.exceptions import NotSupportedValue
from starlette_admin.fields import FileField as BaseFileField
from starlette_admin.fields import ImageField as BaseImageField
from starlette_admin.fields import StringField
from starlette_admin.tools import iterencode


@dataclass(init=False)
class MultiplePKField(StringField):
    """Virtual field to represent multiple primary keys as a single field.

    This field joins the values of multiple primary key columns into a
    single string, encoding/decoding each value.
    """

    def __init__(self, pk_attrs: Sequence[str]):
        self.pk_attrs = pk_attrs
        name = ",".join(pk_attrs)
        super().__init__(
            name,
            exclude_from_list=True,
            exclude_from_detail=True,
            exclude_from_edit=True,
            exclude_from_create=True,
        )

    async def parse_obj(self, request: Request, obj: Any) -> Any:
        """Encode the primary keys values into a single string"""
        return iterencode(str(getattr(obj, n)) for n in self.name.split(","))


@dataclass
class FileField(BaseFileField):
    """This field will automatically work with sqlalchemy_file.FileField"""

    async def serialize_value(
        self, request: Request, value: Any, action: RequestAction
    ) -> Any:
        try:
            return _serialize_sqlalchemy_file_library(
                request, value, action, self.multiple
            )
        except (
            ImportError,
            ModuleNotFoundError,
            NotSupportedValue,
        ):  # pragma: no cover
            return super().serialize_value(request, value, action)


@dataclass
class ImageField(BaseImageField):
    """This field will automatically work with sqlalchemy_file.ImageField"""

    async def serialize_value(
        self, request: Request, value: Any, action: RequestAction
    ) -> Any:
        try:
            return _serialize_sqlalchemy_file_library(
                request, value, action, self.multiple
            )
        except (
            ImportError,
            ModuleNotFoundError,
            NotSupportedValue,
        ):  # pragma: no cover
            return super().serialize_value(request, value, action)


def _serialize_sqlalchemy_file_library(
    request: Request, value: Any, action: RequestAction, is_multiple: bool
) -> Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]:
    from sqlalchemy_file import File

    if isinstance(value, File) or (
        isinstance(value, list) and all(isinstance(f, File) for f in value)
    ):
        data = []
        for item in value if isinstance(value, list) else [value]:
            path = item["path"]
            if (
                action == RequestAction.LIST
                and getattr(item, "thumbnail", None) is not None
            ):
                """Use thumbnail on list page if available"""
                path = item["thumbnail"]["path"]
            storage, file_id = path.split("/")
            data.append(
                {
                    "content_type": item["content_type"],
                    "filename": item["filename"],
                    "url": str(
                        request.url_for(
                            request.app.state.ROUTE_NAME + ":api:file",
                            storage=storage,
                            file_id=file_id,
                        )
                    ),
                }
            )
        return data if is_multiple else data[0]
    raise NotSupportedValue  # pragma: no cover
