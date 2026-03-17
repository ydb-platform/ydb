"""
app is a wsgi application which accepts environ and start_response as argument.
Here We implement a simple flask-like api to avoid explicitly add it as dependency.
wsgiref is used to spawn a server from sqllineage commandline. To serve production traffic, you can/should put
the app behind a real production server like gunicorn or uwsgi, as app is wsgi compatible.
A simple gunicorn example: gunicorn sqllineage.drawing:app
"""

import json
import logging
import mimetypes
import os
from argparse import Namespace
from collections.abc import Callable
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from wsgiref.simple_server import make_server

from sqllineage import DEFAULT_DIALECT, DEFAULT_HOST, DEFAULT_PORT, STATIC_FOLDER
from sqllineage.config import SQLLineageConfig
from sqllineage.core.metadata.dummy import DummyMetaDataProvider
from sqllineage.exceptions import SQLLineageException
from sqllineage.utils.constant import LineageLevel
from sqllineage.utils.helpers import extract_sql_from_args

logger = logging.getLogger(__name__)


class SQLLineageApp:
    def __init__(self) -> None:
        self.routes: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {}
        self.root_path = Path(SQLLineageConfig.DIRECTORY)
        self.metadata_provider = DummyMetaDataProvider()

    def route(self, path: str):
        def wrapper(handler):
            self.routes[path] = handler
            return handler

        return wrapper

    def __call__(self, environ, start_response) -> list[bytes]:
        static_folder = Path(os.path.dirname(__file__)).joinpath(Path(STATIC_FOLDER))
        request_method = environ["REQUEST_METHOD"]
        path_info = environ["PATH_INFO"]
        try:
            if request_method == "GET":
                mimetype = "text/html; charset=utf-8"
                if path_info == "/":
                    static_fname = str(static_folder.joinpath(Path("index.html")))
                else:
                    if ".." in path_info:
                        # Do not allow going back to parent path of static folder
                        return self.handle_404(start_response)
                    static_file = static_folder.joinpath(Path(path_info.strip("/")))
                    if static_file.exists():
                        static_fname = str(static_file)
                        optional_mimetype = mimetypes.guess_type(path_info)[0]
                        mimetype = (
                            optional_mimetype
                            if optional_mimetype is not None
                            else mimetype
                        )
                    else:
                        return self.handle_404(start_response)
                with open(static_fname, "rb") as f:
                    text = f.read()
                return self.handle_200_text(start_response, mimetype, text)
            elif request_method == "POST":
                if path_info in self.routes:
                    request_body_size = int(environ["CONTENT_LENGTH"])
                    request_body = environ["wsgi.input"].read(request_body_size)
                    payload = json.loads(request_body)
                    for param in ["d", "f"]:
                        if param in payload and not str(
                            Path(payload[param]).absolute()
                        ).startswith(str(Path(self.root_path).absolute())):
                            return self.handle_403(start_response)
                    data = self.routes[path_info](payload)
                    return self.handle_200_json(start_response, data)
                else:
                    return self.handle_404(start_response)
            elif request_method == "OPTIONS":
                if path_info in self.routes:
                    start_response(
                        "200 OK",
                        [
                            ("Access-Control-Allow-Origin", "*"),
                            (
                                "Access-Control-Allow-Headers",
                                "Content-Type",
                            ),
                            ("Access-Control-Allow-Methods", "POST"),
                        ],
                    )
                    return []
                else:
                    return self.handle_404(start_response)
            else:
                return self.handle_405(start_response)
        except (SystemExit, IsADirectoryError, FileNotFoundError, PermissionError):
            return self.handle_404(start_response)
        except (SQLLineageException, RuntimeError) as e:
            return self.handle_400(start_response, str(e))

    @staticmethod
    def handle_200_text(start_response, mimetype, text) -> list[bytes]:
        status_code = HTTPStatus.OK
        start_response(
            f"{status_code.value} {status_code.phrase}", [("Content-type", mimetype)]
        )
        return [text]

    def handle_200_json(self, start_response, data) -> list[bytes]:
        return self.handle_json_response(start_response, HTTPStatus.OK, data)

    def handle_400(self, start_response, message) -> list[bytes]:
        return self.handle_client_error_response(
            start_response, HTTPStatus.BAD_REQUEST, message
        )

    def handle_403(self, start_response) -> list[bytes]:
        message = "File Not Allowed For Accessing"
        return self.handle_client_error_response(
            start_response, HTTPStatus.FORBIDDEN, message
        )

    def handle_404(self, start_response) -> list[bytes]:
        message = "File Not Found"
        return self.handle_client_error_response(
            start_response, HTTPStatus.NOT_FOUND, message
        )

    def handle_405(self, start_response) -> list[bytes]:
        message = "Method Not Allowed"
        return self.handle_client_error_response(
            start_response, HTTPStatus.METHOD_NOT_ALLOWED, message
        )

    def handle_client_error_response(
        self, start_response, status_code, message
    ) -> list[bytes]:
        data = {"message": message}
        return self.handle_json_response(start_response, status_code, data)

    @staticmethod
    def handle_json_response(start_response, status_code, data) -> list[bytes]:
        start_response(
            f"{status_code.value} {status_code.phrase}",
            [
                ("Content-type", "application/json"),
                ("Access-Control-Allow-Origin", "*"),
            ],
        )
        return [json.dumps(data).encode("utf-8")]


app = SQLLineageApp()


@app.route("/lineage")
def lineage(payload):
    # this is to avoid circular import
    from sqllineage.runner import LineageRunner

    req_args = Namespace(**payload)
    sql = extract_sql_from_args(req_args)
    dialect = getattr(req_args, "dialect", DEFAULT_DIALECT)
    lr = LineageRunner(
        sql, dialect=dialect, verbose=True, metadata_provider=app.metadata_provider
    )
    data = {
        "verbose": str(lr),
        "dag": lr.to_cytoscape(),
        "column": lr.to_cytoscape(LineageLevel.COLUMN),
    }
    return data


@app.route("/script")
def script(payload):
    req_args = Namespace(**payload)
    sql = extract_sql_from_args(req_args)
    return {"content": sql}


@app.route("/directory")
def directory(payload):
    if payload.get("f"):
        root = Path(payload["f"]).parent
    elif payload.get("d"):
        root = Path(payload["d"])
    else:
        root = Path(SQLLineageConfig.DIRECTORY)
    data = {
        "id": str(root),
        "name": root.name,
        "is_dir": True,
        "children": [
            {"id": str(p), "name": p.name, "is_dir": p.is_dir()}
            for p in sorted(root.iterdir(), key=lambda _: (not _.is_dir(), _.name))
        ],
    }
    return data


def draw_lineage_graph(**kwargs) -> None:
    host = kwargs.pop("host", DEFAULT_HOST)
    port = kwargs.pop("port", DEFAULT_PORT)
    querystring = urlencode({k: v for k, v in kwargs.items() if v})
    path = f"/?{querystring}" if querystring else "/"
    if f := kwargs.get("f"):
        app.root_path = Path(f).parent
    if metadata_provider := kwargs.get("metadata_provider"):
        app.metadata_provider = metadata_provider
    with make_server(host, port, app) as httpd:
        print(f" * SQLLineage Running on http://{host}:{port}{path}")
        httpd.serve_forever()
