from __future__ import annotations

from typing import Any, Mapping

import jinja2
from starlette.background import BackgroundTask
from starlette.datastructures import URL
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.types import Receive, Scope, Send


class _TemplateResponse(HTMLResponse):
    def __init__(
        self,
        template: jinja2.Template,
        content: str,
        context: dict,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        media_type: str | None = None,
        background: BackgroundTask | None = None,
    ):
        self.template = template
        self.context = context
        super().__init__(content, status_code, headers, media_type, background)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        request = self.context.get("request", {})
        extensions = request.get("extensions", {})
        if "http.response.debug" in extensions:
            await send(
                {
                    "type": "http.response.debug",
                    "info": {
                        "template": self.template,
                        "context": self.context,
                    },
                }
            )
        await super().__call__(scope, receive, send)


class Jinja2Templates:
    def __init__(self, directory: str) -> None:
        @jinja2.pass_context
        def url_for(context: dict, __name: str, **path_params: Any) -> URL:
            request: Request = context["request"]
            return request.url_for(__name, **path_params)

        loader = jinja2.FileSystemLoader(directory)
        self.env = jinja2.Environment(loader=loader, autoescape=True, enable_async=True)
        self.env.globals["url_for"] = url_for

    async def TemplateResponse(
        self,
        request: Request,
        name: str,
        context: dict | None = None,
        status_code: int = 200,
    ) -> _TemplateResponse:
        context = context or {}
        context.setdefault("request", request)
        template = self.env.get_template(name)
        content = await template.render_async(context)
        return _TemplateResponse(template, content, context, status_code)
