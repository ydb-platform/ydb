import asyncio
import importlib.resources
from os.path import abspath, dirname, join
from types import FunctionType

from aiohttp import web

from .helpers import (generate_doc_from_each_end_point,
                      load_doc_from_yaml_file, swagger_path)

try:
    import ujson as json
except ImportError:
    import json


async def _swagger_home(request):
    """
    Return the index.html main file
    """
    return web.Response(
        text=request.app["SWAGGER_TEMPLATE_CONTENT"],
        content_type="text/html"
    )


async def _swagger_def(request):
    """
    Returns the Swagger JSON Definition
    """
    return web.json_response(text=request.app["SWAGGER_DEF_CONTENT"])


def setup_swagger(app: web.Application,
                  *,
                  swagger_from_file: str = None,
                  swagger_url: str = "/api/doc",
                  api_base_url: str = "/",
                  swagger_validator_url: str = "",
                  description: str = "Swagger API definition",
                  api_version: str = "1.0.0",
                  ui_version: int = None,
                  title: str = "Swagger API",
                  contact: str = "",
                  swagger_home_decor: FunctionType = None,
                  swagger_def_decor: FunctionType = None,
                  swagger_info: dict = None,
                  swagger_template_path: str = None,
                  definitions: dict = None,
                  security_definitions: dict = None):
    _swagger_url = ("/{}".format(swagger_url)
                    if not swagger_url.startswith("/")
                    else swagger_url)
    _base_swagger_url = _swagger_url.rstrip('/')
    _swagger_def_url = '{}/swagger.json'.format(_base_swagger_url)

    if ui_version == 3:
        STATIC_PATH = importlib.resources.files(__package__) / "swagger_ui3"
    else:
        STATIC_PATH = importlib.resources.files(__package__) / "swagger_ui"

    # Build Swagget Info
    if swagger_info is None:
        if swagger_from_file:
            swagger_info = load_doc_from_yaml_file(swagger_from_file)
        else:
            swagger_info = generate_doc_from_each_end_point(
                app, ui_version=ui_version,
                api_base_url=api_base_url, description=description,
                api_version=api_version, title=title, contact=contact,
                template_path=swagger_template_path,
                definitions=definitions,
                security_definitions=security_definitions
            )
    else:
        swagger_info = json.dumps(swagger_info)

    _swagger_home_func = _swagger_home
    _swagger_def_func = _swagger_def

    if swagger_home_decor is not None:
        _swagger_home_func = swagger_home_decor(_swagger_home)

    if swagger_def_decor is not None:
        _swagger_def_func = swagger_def_decor(_swagger_def)

    # Add API routes
    app.router.add_route('GET', _swagger_url, _swagger_home_func)
    app.router.add_route('GET', "{}/".format(_base_swagger_url),
                         _swagger_home_func)
    app.router.add_route('GET', _swagger_def_url, _swagger_def_func)

    # Set statics
    statics_path = '{}/swagger_static'.format(_base_swagger_url)
    app.router.add_static(statics_path, STATIC_PATH)

    # --------------------------------------------------------------------------
    # Build templates
    # --------------------------------------------------------------------------
    app["SWAGGER_DEF_CONTENT"] = swagger_info
    with (STATIC_PATH / "index.html").open("r") as f:
        app["SWAGGER_TEMPLATE_CONTENT"] = (
            f.read()
            .replace("##SWAGGER_CONFIG##", '{}{}'.
                     format(api_base_url.rstrip('/'), _swagger_def_url))
            .replace("##STATIC_PATH##", '{}{}'.
                     format(api_base_url.rstrip('/'), statics_path))
            .replace("##SWAGGER_VALIDATOR_URL##", swagger_validator_url)
        )


__all__ = ("setup_swagger", "swagger_path")
