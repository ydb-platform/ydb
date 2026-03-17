from dataclasses import dataclass
from typing import Any, Dict, Optional

from mongoengine import GridFSProxy
from starlette.requests import Request
from starlette_admin._types import RequestAction
from starlette_admin.fields import FileField as BaseFileField
from starlette_admin.fields import ImageField as BaseImageField


@dataclass
class FileField(BaseFileField):
    async def serialize_value(
        self, request: Request, value: Any, action: RequestAction
    ) -> Any:
        return _serialize_file_field(request, value, action)


@dataclass
class ImageField(BaseImageField):
    async def serialize_value(
        self, request: Request, value: Any, action: RequestAction
    ) -> Any:
        return _serialize_file_field(request, value, action)


def _serialize_file_field(
    request: Request, value: GridFSProxy, action: RequestAction
) -> Optional[Dict[str, str]]:
    if value.grid_id:
        id = value.grid_id
        if (
            action == RequestAction.LIST
            and getattr(value, "thumbnail_id", None) is not None
        ):
            """Use thumbnail on list page if available"""
            id = value.thumbnail_id
        return {
            "filename": getattr(value, "filename", "unamed"),
            "content_type": getattr(value, "content_type", "application/octet-stream"),
            "url": str(
                request.url_for(
                    request.app.state.ROUTE_NAME + ":api:file",
                    db=value.db_alias,
                    col=value.collection_name,
                    pk=id,
                )
            ),
        }
    return None
