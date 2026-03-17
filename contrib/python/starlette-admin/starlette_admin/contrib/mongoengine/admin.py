import gridfs
from bson import ObjectId
from mongoengine.connection import get_db
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route
from starlette_admin.base import BaseAdmin


class Admin(BaseAdmin):
    def mount_to(self, app: Starlette, redirect_slashes: bool = True) -> None:
        self.routes.append(
            Route(
                "/api/file/{db}/{col}/{pk}",
                _serve_file,
                methods=["GET"],
                name="api:file",
            )
        )
        super().mount_to(app, redirect_slashes)


def _serve_file(request: Request) -> Response:
    pk = request.path_params.get("pk")
    col = request.path_params.get("col")
    db = request.path_params.get("db")
    fs = gridfs.GridFS(get_db(db), col)  # type: ignore
    try:
        file = fs.get(ObjectId(pk))
        return StreamingResponse(
            file,
            media_type=file.content_type,
            headers={"Content-Disposition": f"attachment;filename={file.filename}"},
        )
    except Exception:
        raise HTTPException(404)  # noqa B904
