from typing import Optional, Sequence, Union

from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import (
    FileResponse,
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from starlette.routing import Route
from starlette_admin.auth import BaseAuthProvider
from starlette_admin.base import BaseAdmin
from starlette_admin.contrib.sqla.middleware import DBSessionMiddleware
from starlette_admin.i18n import I18nConfig, TimezoneConfig
from starlette_admin.i18n import lazy_gettext as _
from starlette_admin.views import CustomView


class Admin(BaseAdmin):
    def __init__(
        self,
        engine: Union[Engine, AsyncEngine],
        title: str = _("Admin"),
        base_url: str = "/admin",
        route_name: str = "admin",
        logo_url: Optional[str] = None,
        login_logo_url: Optional[str] = None,
        templates_dir: str = "templates",
        statics_dir: str = "statics",
        index_view: Optional[CustomView] = None,
        auth_provider: Optional[BaseAuthProvider] = None,
        middlewares: Optional[Sequence[Middleware]] = None,
        debug: bool = False,
        i18n_config: Optional[I18nConfig] = None,
        timezone_config: Optional[TimezoneConfig] = None,
        favicon_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            title=title,
            base_url=base_url,
            route_name=route_name,
            logo_url=logo_url,
            login_logo_url=login_logo_url,
            templates_dir=templates_dir,
            statics_dir=statics_dir,
            index_view=index_view,
            auth_provider=auth_provider,
            middlewares=middlewares,
            debug=debug,
            i18n_config=i18n_config,
            timezone_config=timezone_config,
            favicon_url=favicon_url,
        )
        self.middlewares = [] if self.middlewares is None else list(self.middlewares)
        self.middlewares.insert(0, Middleware(DBSessionMiddleware, engine=engine))

    def mount_to(self, app: Starlette, redirect_slashes: bool = True) -> None:
        try:
            """Automatically add route to serve sqlalchemy_file files"""
            __import__("sqlalchemy_file")
            self.routes.append(
                Route(
                    "/api/file/{storage}/{file_id}",
                    _serve_file,
                    methods=["GET"],
                    name="api:file",
                )
            )
        except ImportError:  # pragma: no cover
            pass
        super().mount_to(app, redirect_slashes=redirect_slashes)


def _serve_file(request: Request) -> Response:
    from libcloud.storage.types import ObjectDoesNotExistError
    from sqlalchemy_file.storage import StorageManager

    try:
        storage = request.path_params.get("storage")
        file_id = request.path_params.get("file_id")
        file = StorageManager.get_file(f"{storage}/{file_id}")
        if file.object.driver.name == "Local Storage":
            """If file is stored in local storage, just return a
            FileResponse with the fill full path."""
            return FileResponse(
                file.get_cdn_url(), media_type=file.content_type, filename=file.filename  # type: ignore
            )
        if file.get_cdn_url() is not None:  # pragma: no cover
            """If file has public url, redirect to this url"""
            return RedirectResponse(file.get_cdn_url())  # type: ignore
        """Otherwise, return a streaming response"""
        return StreamingResponse(
            file.object.as_stream(),
            media_type=file.content_type,
            headers={"Content-Disposition": f"attachment;filename={file.filename}"},
        )
    except ObjectDoesNotExistError:
        return JSONResponse({"detail": "Not found"}, status_code=404)
