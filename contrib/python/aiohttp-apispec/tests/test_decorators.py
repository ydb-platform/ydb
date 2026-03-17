import pytest
from aiohttp import web
from marshmallow import fields, Schema

from aiohttp_apispec import docs, request_schema, response_schema


class RequestSchema(Schema):
    id = fields.Int()
    name = fields.Str(description="name")
    bool_field = fields.Bool()
    list_field = fields.List(fields.Int())


class ResponseSchema(Schema):
    msg = fields.Str()
    data = fields.Dict()


class TestViewDecorators:
    @pytest.fixture
    def aiohttp_view_all(self):
        @docs(
            tags=["mytag"],
            summary="Test method summary",
            description="Test method description",
        )
        @request_schema(RequestSchema, locations=["query"])
        @response_schema(ResponseSchema, 200)
        async def index(request, **data):
            return web.json_response({"msg": "done", "data": {}})

        return index

    @pytest.fixture
    def aiohttp_view_docs(self):
        @docs(
            tags=["mytag"],
            summary="Test method summary",
            description="Test method description",
        )
        async def index(request, **data):
            return web.json_response({"msg": "done", "data": {}})

        return index

    @pytest.fixture
    def aiohttp_view_kwargs(self):
        @request_schema(RequestSchema, locations=["query"])
        async def index(request, **data):
            return web.json_response({"msg": "done", "data": {}})

        return index

    @pytest.fixture
    def aiohttp_view_marshal(self):
        @response_schema(ResponseSchema, 200, description="Method description")
        async def index(request, **data):
            return web.json_response({"msg": "done", "data": {}})

        return index

    @pytest.fixture
    def aiohttp_view_request_schema_with_example_without_refs(self, example_for_request_schema):
        @request_schema(RequestSchema, example=example_for_request_schema)
        async def index(request, **data):
            return web.json_response({"msg": "done", "data": {}})

        return index

    @pytest.fixture
    def aiohttp_view_request_schema_with_example(self, example_for_request_schema):
        @request_schema(RequestSchema, example=example_for_request_schema, add_to_refs=True)
        async def index(request, **data):
            return web.json_response({"msg": "done", "data": {}})

        return index

    def test_docs_view(self, aiohttp_view_docs):
        assert hasattr(aiohttp_view_docs, "__apispec__")
        assert aiohttp_view_docs.__apispec__["tags"] == ["mytag"]
        assert aiohttp_view_docs.__apispec__["summary"] == "Test method summary"
        assert aiohttp_view_docs.__apispec__["description"] == "Test method description"
        for param in ("parameters", "responses"):
            assert param in aiohttp_view_docs.__apispec__

    def test_request_schema_view(self, aiohttp_view_kwargs):
        assert hasattr(aiohttp_view_kwargs, "__apispec__")
        assert hasattr(aiohttp_view_kwargs, "__schemas__")
        assert isinstance(
            aiohttp_view_kwargs.__schemas__[0].pop("schema"), RequestSchema
        )
        assert aiohttp_view_kwargs.__schemas__ == [
            {"locations": ["query"], 'put_into': None}
        ]
        for param in ("parameters", "responses"):
            assert param in aiohttp_view_kwargs.__apispec__

    @pytest.mark.skip
    def test_request_schema_parameters(self, aiohttp_view_kwargs):
        parameters = aiohttp_view_kwargs.__apispec__["parameters"]
        assert sorted(parameters, key=lambda x: x["name"]) == [
            {"in": "query", "name": "bool_field", "required": False, "type": "boolean"},
            {
                "in": "query",
                "name": "id",
                "required": False,
                "type": "integer",
                "format": "int32",
            },
            {
                "in": "query",
                "name": "list_field",
                "required": False,
                "collectionFormat": "multi",
                "type": "array",
                "items": {"type": "integer", "format": "int32"},
            },
            {
                "in": "query",
                "name": "name",
                "required": False,
                "type": "string",
                "description": "name",
            },
        ]

    def test_marshalling(self, aiohttp_view_marshal):
        assert hasattr(aiohttp_view_marshal, "__apispec__")
        for param in ("parameters", "responses"):
            assert param in aiohttp_view_marshal.__apispec__
        assert "200" in aiohttp_view_marshal.__apispec__["responses"]

    def test_request_schema_with_example_without_refs(
            self,
            aiohttp_view_request_schema_with_example_without_refs,
            example_for_request_schema):
        schema = aiohttp_view_request_schema_with_example_without_refs.__apispec__["schemas"][0]
        expacted_result = example_for_request_schema.copy()
        expacted_result['add_to_refs'] = False
        assert schema['example'] == expacted_result

    def test_request_schema_with_example(
            self,
            aiohttp_view_request_schema_with_example,
            example_for_request_schema):
        schema = aiohttp_view_request_schema_with_example.__apispec__["schemas"][0]
        expacted_result = example_for_request_schema.copy()
        expacted_result['add_to_refs'] = True
        assert schema['example'] == expacted_result

    def test_all(self, aiohttp_view_all):
        assert hasattr(aiohttp_view_all, "__apispec__")
        assert hasattr(aiohttp_view_all, "__schemas__")
        for param in ("parameters", "responses"):
            assert param in aiohttp_view_all.__apispec__
        assert aiohttp_view_all.__apispec__["tags"] == ["mytag"]
        assert aiohttp_view_all.__apispec__["summary"] == "Test method summary"
        assert aiohttp_view_all.__apispec__["description"] == "Test method description"

    def test_view_multiple_body_parameters(self):
        with pytest.raises(RuntimeError) as ex:

            @request_schema(RequestSchema, locations=["body"])
            @request_schema(RequestSchema, locations=["body"])
            async def index(request, **data):
                return web.json_response({"msg": "done", "data": {}})

        assert isinstance(ex.value, RuntimeError)
        assert str(ex.value) == "Multiple body parameters are not allowed"
