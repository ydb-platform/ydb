import json

import pytest

from marshmallow.fields import Field, DateTime, Dict, String, Nested, List
from marshmallow import Schema

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec.ext.marshmallow.openapi import MARSHMALLOW_VERSION_INFO
from apispec.ext.marshmallow import common
from apispec.exceptions import APISpecError
from tests.schemas import (
    PetSchema,
    AnalysisSchema,
    RunSchema,
    SelfReferencingSchema,
    OrderedSchema,
    PatternedObjectSchema,
    DefaultValuesSchema,
    AnalysisWithListSchema,
)

from tests.utils import get_schemas, get_parameters, get_responses, get_paths, build_ref


class TestDefinitionHelper:
    @pytest.mark.parametrize("schema", [PetSchema, PetSchema()])
    def test_can_use_schema_as_definition(self, spec, schema):
        spec.components.schema("Pet", schema=schema)
        definitions = get_schemas(spec)
        props = definitions["Pet"]["properties"]

        assert props["id"]["type"] == "integer"
        assert props["name"]["type"] == "string"

    def test_schema_helper_without_schema(self, spec):
        spec.components.schema("Pet", {"properties": {"key": {"type": "integer"}}})
        definitions = get_schemas(spec)
        assert definitions["Pet"]["properties"] == {"key": {"type": "integer"}}

    @pytest.mark.parametrize("schema", [AnalysisSchema, AnalysisSchema()])
    def test_resolve_schema_dict_auto_reference(self, schema):
        def resolver(schema):
            schema_cls = common.resolve_schema_cls(schema)
            return schema_cls.__name__

        spec = APISpec(
            title="Test auto-reference",
            version="0.1",
            openapi_version="2.0",
            plugins=(MarshmallowPlugin(schema_name_resolver=resolver),),
        )
        with pytest.raises(KeyError):
            get_schemas(spec)

        spec.components.schema("analysis", schema=schema)
        spec.path(
            "/test",
            operations={
                "get": {
                    "responses": {
                        "200": {"schema": build_ref(spec, "schema", "analysis")}
                    }
                }
            },
        )
        definitions = get_schemas(spec)
        assert 3 == len(definitions)

        assert "analysis" in definitions
        assert "SampleSchema" in definitions
        assert "RunSchema" in definitions

    @pytest.mark.parametrize(
        "schema", [AnalysisWithListSchema, AnalysisWithListSchema()]
    )
    def test_resolve_schema_dict_auto_reference_in_list(self, schema):
        def resolver(schema):
            schema_cls = common.resolve_schema_cls(schema)
            return schema_cls.__name__

        spec = APISpec(
            title="Test auto-reference",
            version="0.1",
            openapi_version="2.0",
            plugins=(MarshmallowPlugin(schema_name_resolver=resolver),),
        )
        with pytest.raises(KeyError):
            get_schemas(spec)

        spec.components.schema("analysis", schema=schema)
        spec.path(
            "/test",
            operations={
                "get": {
                    "responses": {
                        "200": {"schema": build_ref(spec, "schema", "analysis")}
                    }
                }
            },
        )
        definitions = get_schemas(spec)
        assert 3 == len(definitions)

        assert "analysis" in definitions
        assert "SampleSchema" in definitions
        assert "RunSchema" in definitions

    @pytest.mark.parametrize("schema", [AnalysisSchema, AnalysisSchema()])
    def test_resolve_schema_dict_auto_reference_return_none(self, schema):
        def resolver(schema):
            return None

        spec = APISpec(
            title="Test auto-reference",
            version="0.1",
            openapi_version="2.0",
            plugins=(MarshmallowPlugin(schema_name_resolver=resolver),),
        )
        with pytest.raises(KeyError):
            get_schemas(spec)

        with pytest.raises(
            APISpecError, match="Name resolver returned None for schema"
        ):
            spec.components.schema("analysis", schema=schema)

    @pytest.mark.parametrize("schema", [AnalysisSchema, AnalysisSchema()])
    def test_warning_when_schema_added_twice(self, spec, schema):
        spec.components.schema("Analysis", schema=schema)
        with pytest.warns(UserWarning, match="has already been added to the spec"):
            spec.components.schema("DuplicateAnalysis", schema=schema)

    def test_schema_instances_with_different_modifiers_added(self, spec):
        class MultiModifierSchema(Schema):
            pet_unmodified = Nested(PetSchema)
            pet_exclude = Nested(PetSchema, exclude=("name",))

        spec.components.schema("Pet", schema=PetSchema())
        spec.components.schema("Pet_Exclude", schema=PetSchema(exclude=("name",)))

        spec.components.schema("MultiModifierSchema", schema=MultiModifierSchema)

        definitions = get_schemas(spec)
        pet_unmodified_ref = definitions["MultiModifierSchema"]["properties"][
            "pet_unmodified"
        ]
        assert pet_unmodified_ref == build_ref(spec, "schema", "Pet")

        pet_exclude = definitions["MultiModifierSchema"]["properties"]["pet_exclude"]
        assert pet_exclude == build_ref(spec, "schema", "Pet_Exclude")

    def test_schema_instance_with_different_modifers_custom_resolver(self):
        class MultiModifierSchema(Schema):
            pet_unmodified = Nested(PetSchema)
            pet_exclude = Nested(PetSchema(partial=True))

        def resolver(schema):
            schema_instance = common.resolve_schema_instance(schema)
            prefix = "Partial-" if schema_instance.partial else ""
            schema_cls = common.resolve_schema_cls(schema)
            name = prefix + schema_cls.__name__
            if name.endswith("Schema"):
                return name[:-6] or name
            return name

        spec = APISpec(
            title="Test Custom Resolver for Partial",
            version="0.1",
            openapi_version="2.0",
            plugins=(MarshmallowPlugin(schema_name_resolver=resolver),),
        )

        with pytest.warns(None) as record:
            spec.components.schema("NameClashSchema", schema=MultiModifierSchema)

        #assert len(record) == 0

    def test_schema_with_clashing_names(self, spec):
        class Pet(PetSchema):
            another_field = String()

        class NameClashSchema(Schema):
            pet_1 = Nested(PetSchema)
            pet_2 = Nested(Pet)

        with pytest.warns(
            UserWarning, match="Multiple schemas resolved to the name Pet"
        ):
            spec.components.schema("NameClashSchema", schema=NameClashSchema)

        definitions = get_schemas(spec)

        assert "Pet" in definitions
        assert "Pet1" in definitions

    def test_resolve_nested_schema_many_true_resolver_return_none(self):
        def resolver(schema):
            return None

        class PetFamilySchema(Schema):
            pets_1 = Nested(PetSchema, many=True)
            pets_2 = List(Nested(PetSchema))

        spec = APISpec(
            title="Test auto-reference",
            version="0.1",
            openapi_version="2.0",
            plugins=(MarshmallowPlugin(schema_name_resolver=resolver),),
        )

        spec.components.schema("PetFamily", schema=PetFamilySchema)
        props = get_schemas(spec)["PetFamily"]["properties"]
        pets_1 = props["pets_1"]
        pets_2 = props["pets_2"]
        assert pets_1["type"] == pets_2["type"] == "array"


class TestComponentParameterHelper:
    @pytest.mark.parametrize("schema", [PetSchema, PetSchema()])
    def test_can_use_schema_in_parameter(self, spec, schema):
        if spec.openapi_version.major < 3:
            param = {"schema": schema}
        else:
            param = {"content": {"application/json": {"schema": schema}}}
        spec.components.parameter("Pet", "body", param)
        parameter = get_parameters(spec)["Pet"]
        assert parameter["in"] == "body"
        if spec.openapi_version.major < 3:
            reference = parameter["schema"]
        else:
            reference = parameter["content"]["application/json"]["schema"]
        assert reference == build_ref(spec, "schema", "Pet")

        resolved_schema = spec.components._schemas["Pet"]
        assert resolved_schema["properties"]["name"]["type"] == "string"
        assert resolved_schema["properties"]["password"]["type"] == "string"
        assert resolved_schema["properties"]["id"]["type"] == "integer"


class TestComponentResponseHelper:
    @pytest.mark.parametrize("schema", [PetSchema, PetSchema()])
    def test_can_use_schema_in_response(self, spec, schema):
        if spec.openapi_version.major < 3:
            resp = {"schema": schema}
        else:
            resp = {"content": {"application/json": {"schema": schema}}}
        spec.components.response("GetPetOk", resp)
        response = get_responses(spec)["GetPetOk"]
        if spec.openapi_version.major < 3:
            reference = response["schema"]
        else:
            reference = response["content"]["application/json"]["schema"]
        assert reference == build_ref(spec, "schema", "Pet")

        resolved_schema = spec.components._schemas["Pet"]
        assert resolved_schema["properties"]["id"]["type"] == "integer"
        assert resolved_schema["properties"]["name"]["type"] == "string"
        assert resolved_schema["properties"]["password"]["type"] == "string"

    @pytest.mark.parametrize("schema", [PetSchema, PetSchema()])
    def test_can_use_schema_in_response_header(self, spec, schema):
        resp = {"headers": {"PetHeader": {"schema": schema}}}
        spec.components.response("GetPetOk", resp)
        response = get_responses(spec)["GetPetOk"]
        reference = response["headers"]["PetHeader"]["schema"]
        assert reference == build_ref(spec, "schema", "Pet")

        resolved_schema = spec.components._schemas["Pet"]
        assert resolved_schema["properties"]["id"]["type"] == "integer"
        assert resolved_schema["properties"]["name"]["type"] == "string"
        assert resolved_schema["properties"]["password"]["type"] == "string"

    @pytest.mark.parametrize("spec", ("3.0.0",), indirect=True)
    def test_content_without_schema(self, spec):
        resp = {"content": {"application/json": {"example": {"name": "Example"}}}}
        spec.components.response("GetPetOk", resp)
        response = get_responses(spec)["GetPetOk"]
        assert response == resp


class TestCustomField:
    def test_can_use_custom_field_decorator(self, spec_fixture):
        @spec_fixture.marshmallow_plugin.map_to_openapi_type(DateTime)
        class CustomNameA(Field):
            pass

        @spec_fixture.marshmallow_plugin.map_to_openapi_type("integer", "int32")
        class CustomNameB(Field):
            pass

        with pytest.raises(TypeError):

            @spec_fixture.marshmallow_plugin.map_to_openapi_type("integer")
            class BadCustomField(Field):
                pass

        class CustomPetASchema(PetSchema):
            name = CustomNameA()

        class CustomPetBSchema(PetSchema):
            name = CustomNameB()

        spec_fixture.spec.components.schema("Pet", schema=PetSchema)
        spec_fixture.spec.components.schema("CustomPetA", schema=CustomPetASchema)
        spec_fixture.spec.components.schema("CustomPetB", schema=CustomPetBSchema)

        props_0 = get_schemas(spec_fixture.spec)["Pet"]["properties"]
        props_a = get_schemas(spec_fixture.spec)["CustomPetA"]["properties"]
        props_b = get_schemas(spec_fixture.spec)["CustomPetB"]["properties"]

        assert props_0["name"]["type"] == "string"
        assert "format" not in props_0["name"]

        assert props_a["name"]["type"] == "string"
        assert props_a["name"]["format"] == "date-time"

        assert props_b["name"]["type"] == "integer"
        assert props_b["name"]["format"] == "int32"


def get_nested_schema(schema, field_name):
    try:
        return schema._declared_fields[field_name]._schema
    except AttributeError:
        return schema._declared_fields[field_name]._Nested__schema


class TestOperationHelper:
    @pytest.mark.parametrize(
        "pet_schema",
        (PetSchema, PetSchema(), PetSchema(many=True), "tests.schemas.PetSchema"),
    )
    @pytest.mark.parametrize("spec_fixture", ("2.0",), indirect=True)
    def test_schema_v2(self, spec_fixture, pet_schema):
        spec_fixture.spec.path(
            path="/pet",
            operations={
                "get": {
                    "responses": {
                        200: {
                            "schema": pet_schema,
                            "description": "successful operation",
                            "headers": {"PetHeader": {"schema": pet_schema}},
                        }
                    }
                }
            },
        )
        get = get_paths(spec_fixture.spec)["/pet"]["get"]
        if isinstance(pet_schema, Schema) and pet_schema.many is True:
            assert get["responses"]["200"]["schema"]["type"] == "array"
            schema_reference = get["responses"]["200"]["schema"]["items"]
            assert (
                get["responses"]["200"]["headers"]["PetHeader"]["schema"]["type"]
                == "array"
            )
            header_reference = get["responses"]["200"]["headers"]["PetHeader"][
                "schema"
            ]["items"]
        else:
            schema_reference = get["responses"]["200"]["schema"]
            header_reference = get["responses"]["200"]["headers"]["PetHeader"]["schema"]
        assert schema_reference == build_ref(spec_fixture.spec, "schema", "Pet")
        assert header_reference == build_ref(spec_fixture.spec, "schema", "Pet")
        assert len(spec_fixture.spec.components._schemas) == 1
        resolved_schema = spec_fixture.spec.components._schemas["Pet"]
        assert resolved_schema == spec_fixture.openapi.schema2jsonschema(PetSchema)
        assert get["responses"]["200"]["description"] == "successful operation"

    @pytest.mark.parametrize(
        "pet_schema",
        (PetSchema, PetSchema(), PetSchema(many=True), "tests.schemas.PetSchema"),
    )
    @pytest.mark.parametrize("spec_fixture", ("3.0.0",), indirect=True)
    def test_schema_v3(self, spec_fixture, pet_schema):
        spec_fixture.spec.path(
            path="/pet",
            operations={
                "get": {
                    "responses": {
                        200: {
                            "content": {"application/json": {"schema": pet_schema}},
                            "description": "successful operation",
                            "headers": {"PetHeader": {"schema": pet_schema}},
                        }
                    }
                }
            },
        )
        get = get_paths(spec_fixture.spec)["/pet"]["get"]
        if isinstance(pet_schema, Schema) and pet_schema.many is True:
            assert (
                get["responses"]["200"]["content"]["application/json"]["schema"]["type"]
                == "array"
            )
            schema_reference = get["responses"]["200"]["content"]["application/json"][
                "schema"
            ]["items"]
            assert (
                get["responses"]["200"]["headers"]["PetHeader"]["schema"]["type"]
                == "array"
            )
            header_reference = get["responses"]["200"]["headers"]["PetHeader"][
                "schema"
            ]["items"]
        else:
            schema_reference = get["responses"]["200"]["content"]["application/json"][
                "schema"
            ]
            header_reference = get["responses"]["200"]["headers"]["PetHeader"]["schema"]

        assert schema_reference == build_ref(spec_fixture.spec, "schema", "Pet")
        assert header_reference == build_ref(spec_fixture.spec, "schema", "Pet")
        assert len(spec_fixture.spec.components._schemas) == 1
        resolved_schema = spec_fixture.spec.components._schemas["Pet"]
        assert resolved_schema == spec_fixture.openapi.schema2jsonschema(PetSchema)
        assert get["responses"]["200"]["description"] == "successful operation"

    @pytest.mark.parametrize("spec_fixture", ("2.0",), indirect=True)
    def test_schema_expand_parameters_v2(self, spec_fixture):
        spec_fixture.spec.path(
            path="/pet",
            operations={
                "get": {"parameters": [{"in": "query", "schema": PetSchema}]},
                "post": {
                    "parameters": [
                        {
                            "in": "body",
                            "description": "a pet schema",
                            "required": True,
                            "name": "pet",
                            "schema": PetSchema,
                        }
                    ]
                },
            },
        )
        p = get_paths(spec_fixture.spec)["/pet"]
        get = p["get"]
        assert get["parameters"] == spec_fixture.openapi.schema2parameters(
            PetSchema(), default_in="query"
        )
        post = p["post"]
        assert post["parameters"] == spec_fixture.openapi.schema2parameters(
            PetSchema,
            default_in="body",
            required=True,
            name="pet",
            description="a pet schema",
        )

    @pytest.mark.parametrize("spec_fixture", ("3.0.0",), indirect=True)
    def test_schema_expand_parameters_v3(self, spec_fixture):
        spec_fixture.spec.path(
            path="/pet",
            operations={
                "get": {"parameters": [{"in": "query", "schema": PetSchema}]},
                "post": {
                    "requestBody": {
                        "description": "a pet schema",
                        "required": True,
                        "content": {"application/json": {"schema": PetSchema}},
                    }
                },
            },
        )
        p = get_paths(spec_fixture.spec)["/pet"]
        get = p["get"]
        assert get["parameters"] == spec_fixture.openapi.schema2parameters(
            PetSchema(), default_in="query"
        )
        for parameter in get["parameters"]:
            description = parameter.get("description", False)
            assert description
            name = parameter["name"]
            assert description == PetSchema.description[name]
        post = p["post"]
        post_schema = spec_fixture.marshmallow_plugin.resolver.resolve_schema_dict(
            PetSchema
        )
        assert (
            post["requestBody"]["content"]["application/json"]["schema"] == post_schema
        )
        assert post["requestBody"]["description"] == "a pet schema"
        assert post["requestBody"]["required"]

    @pytest.mark.parametrize("spec_fixture", ("2.0",), indirect=True)
    def test_schema_uses_ref_if_available_v2(self, spec_fixture):
        spec_fixture.spec.components.schema("Pet", schema=PetSchema)
        spec_fixture.spec.path(
            path="/pet", operations={"get": {"responses": {200: {"schema": PetSchema}}}}
        )
        get = get_paths(spec_fixture.spec)["/pet"]["get"]
        assert get["responses"]["200"]["schema"] == build_ref(
            spec_fixture.spec, "schema", "Pet"
        )

    @pytest.mark.parametrize("spec_fixture", ("3.0.0",), indirect=True)
    def test_schema_uses_ref_if_available_v3(self, spec_fixture):
        spec_fixture.spec.components.schema("Pet", schema=PetSchema)
        spec_fixture.spec.path(
            path="/pet",
            operations={
                "get": {
                    "responses": {
                        200: {"content": {"application/json": {"schema": PetSchema}}}
                    }
                }
            },
        )
        get = get_paths(spec_fixture.spec)["/pet"]["get"]
        assert get["responses"]["200"]["content"]["application/json"][
            "schema"
        ] == build_ref(spec_fixture.spec, "schema", "Pet")

    def test_schema_uses_ref_if_available_name_resolver_returns_none_v2(self):
        def resolver(schema):
            return None

        spec = APISpec(
            title="Test auto-reference",
            version="0.1",
            openapi_version="2.0",
            plugins=(MarshmallowPlugin(schema_name_resolver=resolver),),
        )
        spec.components.schema("Pet", schema=PetSchema)
        spec.path(
            path="/pet", operations={"get": {"responses": {200: {"schema": PetSchema}}}}
        )
        get = get_paths(spec)["/pet"]["get"]
        assert get["responses"]["200"]["schema"] == build_ref(spec, "schema", "Pet")

    def test_schema_uses_ref_if_available_name_resolver_returns_none_v3(self):
        def resolver(schema):
            return None

        spec = APISpec(
            title="Test auto-reference",
            version="0.1",
            openapi_version="3.0.0",
            plugins=(MarshmallowPlugin(schema_name_resolver=resolver),),
        )
        spec.components.schema("Pet", schema=PetSchema)
        spec.path(
            path="/pet",
            operations={
                "get": {
                    "responses": {
                        200: {"content": {"application/json": {"schema": PetSchema}}}
                    }
                }
            },
        )
        get = get_paths(spec)["/pet"]["get"]
        assert get["responses"]["200"]["content"]["application/json"][
            "schema"
        ] == build_ref(spec, "schema", "Pet")

    @pytest.mark.parametrize(
        "pet_schema", (PetSchema, PetSchema(), "tests.schemas.PetSchema"),
    )
    def test_schema_name_resolver_returns_none_v2(self, pet_schema):
        def resolver(schema):
            return None

        spec = APISpec(
            title="Test resolver returns None",
            version="0.1",
            openapi_version="2.0",
            plugins=(MarshmallowPlugin(schema_name_resolver=resolver),),
        )
        spec.path(
            path="/pet",
            operations={"get": {"responses": {200: {"schema": pet_schema}}}},
        )
        get = get_paths(spec)["/pet"]["get"]
        assert "properties" in get["responses"]["200"]["schema"]

    @pytest.mark.parametrize(
        "pet_schema", (PetSchema, PetSchema(), "tests.schemas.PetSchema"),
    )
    def test_schema_name_resolver_returns_none_v3(self, pet_schema):
        def resolver(schema):
            return None

        spec = APISpec(
            title="Test resolver returns None",
            version="0.1",
            openapi_version="3.0.0",
            plugins=(MarshmallowPlugin(schema_name_resolver=resolver),),
        )
        spec.path(
            path="/pet",
            operations={
                "get": {
                    "responses": {
                        200: {"content": {"application/json": {"schema": pet_schema}}}
                    }
                }
            },
        )
        get = get_paths(spec)["/pet"]["get"]
        assert (
            "properties"
            in get["responses"]["200"]["content"]["application/json"]["schema"]
        )

    @pytest.mark.parametrize("spec_fixture", ("2.0",), indirect=True)
    def test_schema_uses_ref_in_parameters_and_request_body_if_available_v2(
        self, spec_fixture
    ):
        spec_fixture.spec.components.schema("Pet", schema=PetSchema)
        spec_fixture.spec.path(
            path="/pet",
            operations={
                "get": {"parameters": [{"in": "query", "schema": PetSchema}]},
                "post": {"parameters": [{"in": "body", "schema": PetSchema}]},
            },
        )
        p = get_paths(spec_fixture.spec)["/pet"]
        assert "schema" not in p["get"]["parameters"][0]
        post = p["post"]
        assert len(post["parameters"]) == 1
        assert post["parameters"][0]["schema"] == build_ref(
            spec_fixture.spec, "schema", "Pet"
        )

    @pytest.mark.parametrize("spec_fixture", ("3.0.0",), indirect=True)
    def test_schema_uses_ref_in_parameters_and_request_body_if_available_v3(
        self, spec_fixture
    ):
        spec_fixture.spec.components.schema("Pet", schema=PetSchema)
        spec_fixture.spec.path(
            path="/pet",
            operations={
                "get": {"parameters": [{"in": "query", "schema": PetSchema}]},
                "post": {
                    "requestBody": {
                        "content": {"application/json": {"schema": PetSchema}}
                    }
                },
            },
        )
        p = get_paths(spec_fixture.spec)["/pet"]
        assert "schema" in p["get"]["parameters"][0]
        post = p["post"]
        schema_ref = post["requestBody"]["content"]["application/json"]["schema"]
        assert schema_ref == build_ref(spec_fixture.spec, "schema", "Pet")

    @pytest.mark.parametrize("spec_fixture", ("2.0",), indirect=True)
    def test_schema_array_uses_ref_if_available_v2(self, spec_fixture):
        spec_fixture.spec.components.schema("Pet", schema=PetSchema)
        spec_fixture.spec.path(
            path="/pet",
            operations={
                "get": {
                    "parameters": [
                        {
                            "name": "petSchema",
                            "in": "body",
                            "schema": {"type": "array", "items": PetSchema},
                        }
                    ],
                    "responses": {
                        200: {"schema": {"type": "array", "items": PetSchema}}
                    },
                }
            },
        )
        get = get_paths(spec_fixture.spec)["/pet"]["get"]
        assert len(get["parameters"]) == 1
        resolved_schema = {
            "type": "array",
            "items": build_ref(spec_fixture.spec, "schema", "Pet"),
        }
        assert get["parameters"][0]["schema"] == resolved_schema
        assert get["responses"]["200"]["schema"] == resolved_schema

    @pytest.mark.parametrize("spec_fixture", ("3.0.0",), indirect=True)
    def test_schema_array_uses_ref_if_available_v3(self, spec_fixture):
        spec_fixture.spec.components.schema("Pet", schema=PetSchema)
        spec_fixture.spec.path(
            path="/pet",
            operations={
                "get": {
                    "parameters": [
                        {
                            "name": "Pet",
                            "in": "query",
                            "content": {
                                "application/json": {
                                    "schema": {"type": "array", "items": PetSchema}
                                }
                            },
                        }
                    ],
                    "responses": {
                        200: {
                            "content": {
                                "application/json": {
                                    "schema": {"type": "array", "items": PetSchema}
                                }
                            }
                        }
                    },
                }
            },
        )
        get = get_paths(spec_fixture.spec)["/pet"]["get"]
        assert len(get["parameters"]) == 1
        resolved_schema = {
            "type": "array",
            "items": build_ref(spec_fixture.spec, "schema", "Pet"),
        }
        request_schema = get["parameters"][0]["content"]["application/json"]["schema"]
        assert request_schema == resolved_schema
        response_schema = get["responses"]["200"]["content"]["application/json"][
            "schema"
        ]
        assert response_schema == resolved_schema

    @pytest.mark.parametrize("spec_fixture", ("2.0",), indirect=True)
    def test_schema_partially_v2(self, spec_fixture):
        spec_fixture.spec.components.schema("Pet", schema=PetSchema)
        spec_fixture.spec.path(
            path="/parents",
            operations={
                "get": {
                    "responses": {
                        200: {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "mother": PetSchema,
                                    "father": PetSchema,
                                },
                            }
                        }
                    }
                }
            },
        )
        get = get_paths(spec_fixture.spec)["/parents"]["get"]
        assert get["responses"]["200"]["schema"] == {
            "type": "object",
            "properties": {
                "mother": build_ref(spec_fixture.spec, "schema", "Pet"),
                "father": build_ref(spec_fixture.spec, "schema", "Pet"),
            },
        }

    @pytest.mark.parametrize("spec_fixture", ("3.0.0",), indirect=True)
    def test_schema_partially_v3(self, spec_fixture):
        spec_fixture.spec.components.schema("Pet", schema=PetSchema)
        spec_fixture.spec.path(
            path="/parents",
            operations={
                "get": {
                    "responses": {
                        200: {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "mother": PetSchema,
                                            "father": PetSchema,
                                        },
                                    }
                                }
                            }
                        }
                    }
                }
            },
        )
        get = get_paths(spec_fixture.spec)["/parents"]["get"]
        assert get["responses"]["200"]["content"]["application/json"]["schema"] == {
            "type": "object",
            "properties": {
                "mother": build_ref(spec_fixture.spec, "schema", "Pet"),
                "father": build_ref(spec_fixture.spec, "schema", "Pet"),
            },
        }

    def test_parameter_reference(self, spec_fixture):
        if spec_fixture.spec.openapi_version.major < 3:
            param = {"schema": PetSchema}
        else:
            param = {"content": {"application/json": {"schema": PetSchema}}}
        spec_fixture.spec.components.parameter("Pet", "body", param)
        spec_fixture.spec.path(
            path="/parents", operations={"get": {"parameters": ["Pet"]}}
        )
        get = get_paths(spec_fixture.spec)["/parents"]["get"]
        assert get["parameters"] == [build_ref(spec_fixture.spec, "parameter", "Pet")]

    def test_response_reference(self, spec_fixture):
        if spec_fixture.spec.openapi_version.major < 3:
            resp = {"schema": PetSchema}
        else:
            resp = {"content": {"application/json": {"schema": PetSchema}}}
        spec_fixture.spec.components.response("Pet", resp)
        spec_fixture.spec.path(
            path="/parents", operations={"get": {"responses": {"200": "Pet"}}}
        )
        get = get_paths(spec_fixture.spec)["/parents"]["get"]
        assert get["responses"] == {
            "200": build_ref(spec_fixture.spec, "response", "Pet")
        }

    def test_schema_global_state_untouched_2json(self, spec_fixture):
        assert get_nested_schema(RunSchema, "sample") is None
        data = spec_fixture.openapi.schema2jsonschema(RunSchema)
        json.dumps(data)
        assert get_nested_schema(RunSchema, "sample") is None

    def test_schema_global_state_untouched_2parameters(self, spec_fixture):
        assert get_nested_schema(RunSchema, "sample") is None
        data = spec_fixture.openapi.schema2parameters(RunSchema)
        json.dumps(data)
        assert get_nested_schema(RunSchema, "sample") is None


class TestCircularReference:
    def test_circular_referencing_schemas(self, spec):
        spec.components.schema("Analysis", schema=AnalysisSchema)
        definitions = get_schemas(spec)
        ref = definitions["Analysis"]["properties"]["sample"]
        assert ref == build_ref(spec, "schema", "Sample")


# Regression tests for issue #55
class TestSelfReference:
    def test_self_referencing_field_single(self, spec):
        spec.components.schema("SelfReference", schema=SelfReferencingSchema)
        definitions = get_schemas(spec)
        ref = definitions["SelfReference"]["properties"]["single"]
        assert ref == build_ref(spec, "schema", "SelfReference")

    def test_self_referencing_field_many(self, spec):
        spec.components.schema("SelfReference", schema=SelfReferencingSchema)
        definitions = get_schemas(spec)
        result = definitions["SelfReference"]["properties"]["many"]
        assert result == {
            "type": "array",
            "items": build_ref(spec, "schema", "SelfReference"),
        }


class TestOrderedSchema:
    def test_ordered_schema(self, spec):
        spec.components.schema("Ordered", schema=OrderedSchema)
        result = get_schemas(spec)["Ordered"]["properties"]
        assert list(result.keys()) == ["field1", "field2", "field3", "field4", "field5"]


class TestFieldWithCustomProps:
    def test_field_with_custom_props(self, spec):
        spec.components.schema("PatternedObject", schema=PatternedObjectSchema)
        result = get_schemas(spec)["PatternedObject"]["properties"]["count"]
        assert "x-count" in result
        assert result["x-count"] == 1

    def test_field_with_custom_props_passed_as_snake_case(self, spec):
        spec.components.schema("PatternedObject", schema=PatternedObjectSchema)
        result = get_schemas(spec)["PatternedObject"]["properties"]["count2"]
        assert "x-count2" in result
        assert result["x-count2"] == 2


class TestSchemaWithDefaultValues:
    def test_schema_with_default_values(self, spec):
        spec.components.schema("DefaultValuesSchema", schema=DefaultValuesSchema)
        definitions = get_schemas(spec)
        props = definitions["DefaultValuesSchema"]["properties"]
        assert props["number_auto_default"]["default"] == 12
        assert props["number_manual_default"]["default"] == 42
        assert "default" not in props["string_callable_default"]
        assert props["string_manual_default"]["default"] == "Manual"
        assert "default" not in props["numbers"]


@pytest.mark.skipif(
    MARSHMALLOW_VERSION_INFO[0] < 3, reason="Values ignored in marshmallow 2"
)
class TestDictValues:
    def test_dict_values_resolve_to_additional_properties(self, spec):
        class SchemaWithDict(Schema):
            dict_field = Dict(values=String())

        spec.components.schema("SchemaWithDict", schema=SchemaWithDict)
        result = get_schemas(spec)["SchemaWithDict"]["properties"]["dict_field"]
        assert result == {"type": "object", "additionalProperties": {"type": "string"}}

    def test_dict_with_empty_values_field(self, spec):
        class SchemaWithDict(Schema):
            dict_field = Dict()

        spec.components.schema("SchemaWithDict", schema=SchemaWithDict)
        result = get_schemas(spec)["SchemaWithDict"]["properties"]["dict_field"]
        assert result == {"type": "object"}

    def test_dict_with_nested(self, spec):
        class SchemaWithDict(Schema):
            dict_field = Dict(values=Nested(PetSchema))

        spec.components.schema("SchemaWithDict", schema=SchemaWithDict)

        assert len(get_schemas(spec)) == 2

        result = get_schemas(spec)["SchemaWithDict"]["properties"]["dict_field"]
        assert result == {
            "additionalProperties": build_ref(spec, "schema", "Pet"),
            "type": "object",
        }


class TestList:
    def test_list_with_nested(self, spec):
        class SchemaWithList(Schema):
            list_field = List(Nested(PetSchema))

        spec.components.schema("SchemaWithList", schema=SchemaWithList)

        assert len(get_schemas(spec)) == 2

        result = get_schemas(spec)["SchemaWithList"]["properties"]["list_field"]
        assert result == {"items": build_ref(spec, "schema", "Pet"), "type": "array"}
