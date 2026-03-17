import json

import pytest

from aiohttp import web
from aiohttp.web_urldispatcher import StaticResource
from aiohttp_apispec import setup_aiohttp_apispec
from yarl import URL


def test_app_swagger_url(aiohttp_app):
    def safe_url_for(route):
        if isinstance(route._resource, StaticResource):
            # url_for on StaticResource requires filename arg
            return None
        try:
            return route.url_for()
        except KeyError:
            return None

    urls = [safe_url_for(route) for route in aiohttp_app.app.router.routes()]
    assert URL("/v1/api/docs/api-docs") in urls


async def test_app_swagger_json(aiohttp_app, example_for_request_schema):
    resp = await aiohttp_app.get("/v1/api/docs/api-docs")
    docs = await resp.json()
    assert docs["info"]["title"] == "API documentation"
    assert docs["info"]["version"] == "0.0.1"
    docs["paths"]["/v1/test"]["get"]["parameters"] = sorted(
        docs["paths"]["/v1/test"]["get"]["parameters"], key=lambda x: x["name"]
    )
    assert json.dumps(docs["paths"]["/v1/test"]["get"], sort_keys=True) == json.dumps(
        {
            "parameters": [
                {
                    "in": "query",
                    "name": "bool_field",
                    "required": False,
                    "type": "boolean",
                },
                {
                    "format": "int32",
                    "in": "query",
                    "name": "id",
                    "required": False,
                    "type": "integer",
                },
                {
                    "collectionFormat": "multi",
                    "in": "query",
                    "items": {"format": "int32", "type": "integer"},
                    "name": "list_field",
                    "required": False,
                    "type": "array",
                },
                {
                    "description": "name",
                    "in": "query",
                    "name": "name",
                    "required": False,
                    "type": "string",
                },
            ],
            "responses": {
                "200": {
                    "description": "Success response",
                    "schema": {"$ref": "#/definitions/Response"},
                },
                "404": {"description": "Not Found"},
            },
            "tags": ["mytag"],
            "summary": "Test method summary",
            "description": "Test method description",
            "produces": ["application/json"],
        },
        sort_keys=True,
    )
    docs["paths"]["/v1/class_echo"]["get"]["parameters"] = sorted(
        docs["paths"]["/v1/class_echo"]["get"]["parameters"], key=lambda x: x["name"]
    )
    assert json.dumps(
        docs["paths"]["/v1/class_echo"]["get"], sort_keys=True
    ) == json.dumps(
        {
            "parameters": [
                {
                    "in": "query",
                    "name": "bool_field",
                    "required": False,
                    "type": "boolean",
                },
                {
                    "format": "int32",
                    "in": "query",
                    "name": "id",
                    "required": False,
                    "type": "integer",
                },
                {
                    "collectionFormat": "multi",
                    "in": "query",
                    "items": {"format": "int32", "type": "integer"},
                    "name": "list_field",
                    "required": False,
                    "type": "array",
                },
                {
                    "description": "name",
                    "in": "query",
                    "name": "name",
                    "required": False,
                    "type": "string",
                },
            ],
            "responses": {},
            "tags": ["mytag"],
            "summary": "View method summary",
            "description": "View method description",
            "produces": ["application/json"],
        },
        sort_keys=True,
    )
    assert docs["paths"]["/v1/example_endpoint"]["post"]["parameters"] == [
        {
            'in': 'body',
            'required': False,
            'name': 'body',
            'schema': {
                'allOf': [
                    '#/definitions/Request'
                ],
                'example': example_for_request_schema
            }
        }
    ]

    _request_properties = {
        "properties": {
            "bool_field": {"type": "boolean"},
            "id": {"format": "int32", "type": "integer"},
            "list_field": {
                "items": {"format": "int32", "type": "integer"},
                "type": "array",
            },
            "name": {"description": "name", "type": "string"},
        },
        "type": "object",
    }

    assert json.dumps(docs["definitions"], sort_keys=True) == json.dumps(
        {
            "Request": {**_request_properties, 'example': example_for_request_schema},
            "Partial-Request": _request_properties,
            "Response": {
                "properties": {"data": {"type": "object"}, "msg": {"type": "string"}},
                "type": "object",
            },
        },
        sort_keys=True,
    )

async def test_not_register_route_for_none_url():
    app = web.Application()
    routes_count = len(app.router.routes())
    setup_aiohttp_apispec(app=app, url=None)
    routes_count_after_setup_apispec = len(app.router.routes())
    assert routes_count == routes_count_after_setup_apispec


@pytest.mark.skip
async def test_register_route_for_relative_url():
    app = web.Application()
    routes_count = len(app.router.routes())
    setup_aiohttp_apispec(app=app, url="api/swagger")
    routes_count_after_setup_apispec = len(app.router.routes())
    assert routes_count == routes_count_after_setup_apispec
