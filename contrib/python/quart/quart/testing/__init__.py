from __future__ import annotations

from typing import Any, TYPE_CHECKING

from click.testing import CliRunner

from .app import TestApp
from .client import QuartClient
from .connections import WebsocketResponseError
from .utils import (
    make_test_body_with_headers,
    make_test_headers_path_and_query_string,
    make_test_scope,
    no_op_push,
    sentinel,
)
from ..cli import ScriptInfo

if TYPE_CHECKING:
    from ..app import Quart


class QuartCliRunner(CliRunner):
    def __init__(self, app: "Quart", **kwargs: Any) -> None:
        self.app = app
        super().__init__(**kwargs)

    def invoke(self, cli: Any = None, args: Any = None, **kwargs: Any) -> Any:  # type: ignore
        if cli is None:
            cli = self.app.cli

        if "obj" not in kwargs:
            kwargs["obj"] = ScriptInfo(create_app=lambda: self.app)

        return super().invoke(cli, args, **kwargs)


__all__ = (
    "make_test_body_with_headers",
    "make_test_headers_path_and_query_string",
    "make_test_scope",
    "no_op_push",
    "QuartClient",
    "QuartCliRunner",
    "sentinel",
    "TestApp",
    "WebsocketResponseError",
)
