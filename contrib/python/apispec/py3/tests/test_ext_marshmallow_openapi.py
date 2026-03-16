import pytest

from marshmallow import fields, Schema, validate

from apispec.ext.marshmallow import MarshmallowPlugin
from apispec.ext.marshmallow.openapi import MARSHMALLOW_VERSION_INFO
from apispec import exceptions, utils, APISpec

from tests.schemas import CustomList, CustomStringField
from tests.utils import get_schemas, build_ref


class TestMarshmallowFieldToOpenAPI:
    def test_fields_with_missing_load(self, openapi):
        field_dict = {"field": fields.Str(default="foo", missing="bar")}
        res = openapi.fields2parameters(field_dict, default_in="query")
        if openapi.openapi_version.major < 3:
            assert res[0]["default"] == "bar"
        else:
            assert res[0]["schema"]["default"] == "bar"

    def test_fields_with_location(self, openapi):
        field_dict = {"field": fields.Str(location="querystring")}
        res = openapi.fields2parameters(field_dict, default_in="headers")
        assert res[0]["in"] == "query"

    # json/body is invalid for OpenAPI 3
    @pytest.mark.parametrize("openapi", ("2.0",), indirect=True)
    def test_fields_with_multiple_json_locations(self, openapi):
        field_dict = {
            "field1": fields.Str(location="json", required=True),
            "field2": fields.Str(location="json", required=True),
            "field3": fields.Str(location="json"),
        }
        res = openapi.fields2parameters(field_dict, default_in=None)
        assert len(res) == 1
        assert res[0]["in"] == "body"
        assert res[0]["required"] is False
        assert "field1" in res[0]["schema"]["properties"]
        assert "field2" in res[0]["schema"]["properties"]
        assert "field3" in res[0]["schema"]["properties"]
        assert "required" in res[0]["schema"]
        assert len(res[0]["schema"]["required"]) == 2
        assert "field1" in res[0]["schema"]["required"]
        assert "field2" in res[0]["schema"]["required"]

    def test_fields2parameters_does_not_modify_metadata(self, openapi):
        field_dict = {"field": fields.Str(location="querystring")}
        res = openapi.fields2parameters(field_dict, default_in="headers")
        assert res[0]["in"] == "query"

        res = openapi.fields2parameters(field_dict, default_in="headers")
        assert res[0]["in"] == "query"

    def test_fields_location_mapping(self, openapi):
        field_dict = {"field": fields.Str(location="cookies")}
        res = openapi.fields2parameters(field_dict, default_in="headers")
        assert res[0]["in"] == "cookie"

    def test_fields_default_location_mapping(self, openapi):
        field_dict = {"field": fields.Str()}
        res = openapi.fields2parameters(field_dict, default_in="headers")
        assert res[0]["in"] == "header"

    # json/body is invalid for OpenAPI 3
    @pytest.mark.parametrize("openapi", ("2.0",), indirect=True)
    def test_fields_default_location_mapping_if_schema_many(self, openapi):
        class ExampleSchema(Schema):
            id = fields.Int()

        schema = ExampleSchema(many=True)
        res = openapi.schema2parameters(schema=schema, default_in="json")
        assert res[0]["in"] == "body"

    def test_fields_with_dump_only(self, openapi):
        class UserSchema(Schema):
            name = fields.Str(dump_only=True)

        res = openapi.fields2parameters(UserSchema._declared_fields, default_in="query")
        assert len(res) == 0
        res = openapi.fields2parameters(UserSchema().fields, default_in="query")
        assert len(res) == 0

        class UserSchema(Schema):
            name = fields.Str()

            class Meta:
                dump_only = ("name",)

        res = openapi.schema2parameters(schema=UserSchema, default_in="query")
        assert len(res) == 0


class TestMarshmallowSchemaToModelDefinition:
    def test_invalid_schema(self, openapi):
        with pytest.raises(ValueError):
            openapi.schema2jsonschema(None)

    def test_schema2jsonschema_with_explicit_fields(self, openapi):
        class UserSchema(Schema):
            _id = fields.Int()
            email = fields.Email(description="email address of the user")
            name = fields.Str()

            class Meta:
                title = "User"

        res = openapi.schema2jsonschema(UserSchema)
        assert res["title"] == "User"
        assert res["type"] == "object"
        props = res["properties"]
        assert props["_id"]["type"] == "integer"
        assert props["email"]["type"] == "string"
        assert props["email"]["format"] == "email"
        assert props["email"]["description"] == "email address of the user"

    @pytest.mark.skipif(
        MARSHMALLOW_VERSION_INFO[0] >= 3, reason="Behaviour changed in marshmallow 3"
    )
    def test_schema2jsonschema_override_name_ma2(self, openapi):
        class ExampleSchema(Schema):
            _id = fields.Int(load_from="id", dump_to="id")
            _dt = fields.Int(load_from="lf_no_match", dump_to="dt")
            _lf = fields.Int(load_from="lf")
            _global = fields.Int(load_from="global", dump_to="global")

            class Meta:
                exclude = ("_global",)

        res = openapi.schema2jsonschema(ExampleSchema)
        assert res["type"] == "object"
        props = res["properties"]
        # `_id` renamed to `id`
        assert "_id" not in props and props["id"]["type"] == "integer"
        # `load_from` and `dump_to` do not match, `dump_to` is used
        assert "lf_no_match" not in props
        assert props["dt"]["type"] == "integer"
        # `load_from` and no `dump_to`, `load_from` is used
        assert props["lf"]["type"] == "integer"
        # `_global` excluded correctly
        assert "_global" not in props and "global" not in props

    @pytest.mark.skipif(
        MARSHMALLOW_VERSION_INFO[0] < 3, reason="Behaviour changed in marshmallow 3"
    )
    def test_schema2jsonschema_override_name_ma3(self, openapi):
        class ExampleSchema(Schema):
            _id = fields.Int(data_key="id")
            _global = fields.Int(data_key="global")

            class Meta:
                exclude = ("_global",)

        res = openapi.schema2jsonschema(ExampleSchema)
        assert res["type"] == "object"
        props = res["properties"]
        # `_id` renamed to `id`
        assert "_id" not in props and props["id"]["type"] == "integer"
        # `_global` excluded correctly
        assert "_global" not in props and "global" not in props

    def test_required_fields(self, openapi):
        class BandSchema(Schema):
            drummer = fields.Str(required=True)
            bassist = fields.Str()

        res = openapi.schema2jsonschema(BandSchema)
        assert res["required"] == ["drummer"]

    def test_partial(self, openapi):
        class BandSchema(Schema):
            drummer = fields.Str(required=True)
            bassist = fields.Str(required=True)

        res = openapi.schema2jsonschema(BandSchema(partial=True))
        assert "required" not in res

        res = openapi.schema2jsonschema(BandSchema(partial=("drummer",)))
        assert res["required"] == ["bassist"]

    def test_no_required_fields(self, openapi):
        class BandSchema(Schema):
            drummer = fields.Str()
            bassist = fields.Str()

        res = openapi.schema2jsonschema(BandSchema)
        assert "required" not in res

    def test_title_and_description_may_be_added(self, openapi):
        class UserSchema(Schema):
            class Meta:
                title = "User"
                description = "A registered user"

        res = openapi.schema2jsonschema(UserSchema)
        assert res["description"] == "A registered user"
        assert res["title"] == "User"

    def test_excluded_fields(self, openapi):
        class WhiteStripesSchema(Schema):
            class Meta:
                exclude = ("bassist",)

            guitarist = fields.Str()
            drummer = fields.Str()
            bassist = fields.Str()

        res = openapi.schema2jsonschema(WhiteStripesSchema)
        assert set(res["properties"].keys()) == {"guitarist", "drummer"}

    def test_only_explicitly_declared_fields_are_translated(self, openapi):
        class UserSchema(Schema):
            _id = fields.Int()

            class Meta:
                title = "User"
                fields = ("_id", "email")

        with pytest.warns(
            UserWarning,
            match="Only explicitly-declared fields will be included in the Schema Object.",
        ):
            res = openapi.schema2jsonschema(UserSchema)
            assert res["type"] == "object"
            props = res["properties"]
            assert "_id" in props
            assert "email" not in props

    def test_observed_field_name_for_required_field(self, openapi):
        if MARSHMALLOW_VERSION_INFO[0] < 3:
            fields_dict = {
                "user_id": fields.Int(load_from="id", dump_to="id", required=True)
            }
        else:
            fields_dict = {"user_id": fields.Int(data_key="id", required=True)}

        res = openapi.fields2jsonschema(fields_dict)
        assert res["required"] == ["id"]

    @pytest.mark.parametrize("many", (True, False))
    def test_schema_instance_inspection(self, openapi, many):
        class UserSchema(Schema):
            _id = fields.Int()

        res = openapi.schema2jsonschema(UserSchema(many=many))
        assert res["type"] == "object"
        props = res["properties"]
        assert "_id" in props

    def test_raises_error_if_no_declared_fields(self, openapi):
        class NotASchema:
            pass

        expected_error = "{!r} doesn't have either `fields` or `_declared_fields`.".format(
            NotASchema
        )
        with pytest.raises(ValueError, match=expected_error):
            openapi.schema2jsonschema(NotASchema)


class TestMarshmallowSchemaToParameters:
    @pytest.mark.parametrize("ListClass", [fields.List, CustomList])
    def test_field_multiple(self, ListClass, openapi):
        field = ListClass(fields.Str, location="querystring")
        res = openapi.field2parameter(field, name="field", default_in=None)
        assert res["in"] == "query"
        if openapi.openapi_version.major < 3:
            assert res["type"] == "array"
            assert res["items"]["type"] == "string"
            assert res["collectionFormat"] == "multi"
        else:
            assert res["schema"]["type"] == "array"
            assert res["schema"]["items"]["type"] == "string"
            assert res["style"] == "form"
            assert res["explode"] is True

    def test_field_required(self, openapi):
        field = fields.Str(required=True, location="query")
        res = openapi.field2parameter(field, name="field", default_in=None)
        assert res["required"] is True

    def test_invalid_schema(self, openapi):
        with pytest.raises(ValueError):
            openapi.schema2parameters(None)

    # json/body is invalid for OpenAPI 3
    @pytest.mark.parametrize("openapi", ("2.0",), indirect=True)
    def test_schema_body(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email()

        res = openapi.schema2parameters(UserSchema, default_in="body")
        assert len(res) == 1
        param = res[0]
        assert param["in"] == "body"
        assert param["schema"] == {"$ref": "#/definitions/User"}

    # json/body is invalid for OpenAPI 3
    @pytest.mark.parametrize("openapi", ("2.0",), indirect=True)
    def test_schema_body_with_dump_only(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email(dump_only=True)

        res_nodump = openapi.schema2parameters(UserSchema, default_in="body")
        assert len(res_nodump) == 1
        param = res_nodump[0]
        assert param["in"] == "body"
        assert param["schema"] == build_ref(openapi.spec, "schema", "User")

    # json/body is invalid for OpenAPI 3
    @pytest.mark.parametrize("openapi", ("2.0",), indirect=True)
    def test_schema_body_many(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email()

        res = openapi.schema2parameters(UserSchema(many=True), default_in="body")
        assert len(res) == 1
        param = res[0]
        assert param["in"] == "body"
        assert param["schema"]["type"] == "array"
        assert param["schema"]["items"] == {"$ref": "#/definitions/User"}

    def test_schema_query(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email()

        res = openapi.schema2parameters(UserSchema, default_in="query")
        assert len(res) == 2
        res.sort(key=lambda param: param["name"])
        assert res[0]["name"] == "email"
        assert res[0]["in"] == "query"
        assert res[1]["name"] == "name"
        assert res[1]["in"] == "query"

    def test_schema_query_instance(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email()

        res = openapi.schema2parameters(UserSchema(), default_in="query")
        assert len(res) == 2
        res.sort(key=lambda param: param["name"])
        assert res[0]["name"] == "email"
        assert res[0]["in"] == "query"
        assert res[1]["name"] == "name"
        assert res[1]["in"] == "query"

    def test_schema_query_instance_many_should_raise_exception(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email()

        with pytest.raises(AssertionError):
            openapi.schema2parameters(UserSchema(many=True), default_in="query")

    def test_fields_query(self, openapi):
        field_dict = {"name": fields.Str(), "email": fields.Email()}
        res = openapi.fields2parameters(field_dict, default_in="query")
        assert len(res) == 2
        res.sort(key=lambda param: param["name"])
        assert res[0]["name"] == "email"
        assert res[0]["in"] == "query"
        assert res[1]["name"] == "name"
        assert res[1]["in"] == "query"

    def test_raises_error_if_not_a_schema(self, openapi):
        class NotASchema:
            pass

        expected_error = "{!r} doesn't have either `fields` or `_declared_fields`".format(
            NotASchema
        )
        with pytest.raises(ValueError, match=expected_error):
            openapi.schema2jsonschema(NotASchema)


class CategorySchema(Schema):
    id = fields.Int()
    name = fields.Str(required=True)
    breed = fields.Str(dump_only=True)


class PageSchema(Schema):
    offset = fields.Int()
    limit = fields.Int()


class PetSchema(Schema):
    category = fields.Nested(CategorySchema, many=True)
    name = fields.Str()


class TestNesting:
    def test_schema2jsonschema_with_nested_fields(self, spec_fixture):
        res = spec_fixture.openapi.schema2jsonschema(PetSchema)
        props = res["properties"]

        assert props["category"]["items"] == build_ref(
            spec_fixture.spec, "schema", "Category"
        )

    @pytest.mark.parametrize("modifier", ("only", "exclude"))
    def test_schema2jsonschema_with_nested_fields_only_exclude(
        self, spec_fixture, modifier
    ):
        class Child(Schema):
            i = fields.Int()
            j = fields.Int()

        class Parent(Schema):
            child = fields.Nested(Child, **{modifier: ("i",)})

        spec_fixture.openapi.schema2jsonschema(Parent)
        props = get_schemas(spec_fixture.spec)["Child"]["properties"]
        assert ("i" in props) == (modifier == "only")
        assert ("j" not in props) == (modifier == "only")

    def test_schema2jsonschema_with_nested_fields_with_adhoc_changes(
        self, spec_fixture
    ):
        category_schema = CategorySchema()
        category_schema.fields["id"].required = True

        class PetSchema(Schema):
            category = fields.Nested(category_schema, many=True)
            name = fields.Str()

        spec_fixture.spec.components.schema("Pet", schema=PetSchema)
        props = get_schemas(spec_fixture.spec)

        assert props["Category"] == spec_fixture.openapi.schema2jsonschema(
            category_schema
        )
        assert set(props["Category"]["required"]) == {"id", "name"}

        props["Category"]["required"] = ["name"]
        assert props["Category"] == spec_fixture.openapi.schema2jsonschema(
            CategorySchema
        )

    def test_schema2jsonschema_with_nested_excluded_fields(self, spec):
        category_schema = CategorySchema(exclude=("breed",))

        class PetSchema(Schema):
            category = fields.Nested(category_schema)

        spec.components.schema("Pet", schema=PetSchema)

        category_props = get_schemas(spec)["Category"]["properties"]
        assert "breed" not in category_props


def test_openapi_tools_validate_v2():
    ma_plugin = MarshmallowPlugin()
    spec = APISpec(
        title="Pets", version="0.1", plugins=(ma_plugin,), openapi_version="2.0"
    )
    openapi = ma_plugin.converter

    spec.components.schema("Category", schema=CategorySchema)
    spec.components.schema("Pet", {"discriminator": "name"}, schema=PetSchema)

    spec.path(
        view=None,
        path="/category/{category_id}",
        operations={
            "get": {
                "parameters": [
                    {"name": "q", "in": "query", "type": "string"},
                    {
                        "name": "category_id",
                        "in": "path",
                        "required": True,
                        "type": "string",
                    },
                    openapi.field2parameter(
                        field=fields.List(
                            fields.Str(),
                            validate=validate.OneOf(["freddie", "roger"]),
                            location="querystring",
                        ),
                        default_in=None,
                        name="body",
                    ),
                ]
                + openapi.schema2parameters(PageSchema, default_in="query"),
                "responses": {200: {"schema": PetSchema, "description": "A pet"}},
            },
            "post": {
                "parameters": (
                    [
                        {
                            "name": "category_id",
                            "in": "path",
                            "required": True,
                            "type": "string",
                        }
                    ]
                    + openapi.schema2parameters(CategorySchema, default_in="body")
                ),
                "responses": {201: {"schema": PetSchema, "description": "A pet"}},
            },
        },
    )
    try:
        utils.validate_spec(spec)
    except exceptions.OpenAPIError as error:
        pytest.fail(str(error))


def test_openapi_tools_validate_v3():
    ma_plugin = MarshmallowPlugin()
    spec = APISpec(
        title="Pets", version="0.1", plugins=(ma_plugin,), openapi_version="3.0.0"
    )
    openapi = ma_plugin.converter

    spec.components.schema("Category", schema=CategorySchema)
    spec.components.schema("Pet", schema=PetSchema)

    spec.path(
        view=None,
        path="/category/{category_id}",
        operations={
            "get": {
                "parameters": [
                    {"name": "q", "in": "query", "schema": {"type": "string"}},
                    {
                        "name": "category_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"},
                    },
                    openapi.field2parameter(
                        field=fields.List(
                            fields.Str(),
                            validate=validate.OneOf(["freddie", "roger"]),
                            location="querystring",
                        ),
                        default_in=None,
                        name="body",
                    ),
                ]
                + openapi.schema2parameters(PageSchema, default_in="query"),
                "responses": {
                    200: {
                        "description": "success",
                        "content": {"application/json": {"schema": PetSchema}},
                    }
                },
            },
            "post": {
                "parameters": (
                    [
                        {
                            "name": "category_id",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "string"},
                        }
                    ]
                ),
                "requestBody": {
                    "content": {"application/json": {"schema": CategorySchema}}
                },
                "responses": {
                    201: {
                        "description": "created",
                        "content": {"application/json": {"schema": PetSchema}},
                    }
                },
            },
        },
    )
    try:
        utils.validate_spec(spec)
    except exceptions.OpenAPIError as error:
        pytest.fail(str(error))


class TestFieldValidation:
    class ValidationSchema(Schema):
        id = fields.Int(dump_only=True)
        range = fields.Int(validate=validate.Range(min=1, max=10))
        multiple_ranges = fields.Int(
            validate=[
                validate.Range(min=1),
                validate.Range(min=3),
                validate.Range(max=10),
                validate.Range(max=7),
            ]
        )
        list_length = fields.List(fields.Str, validate=validate.Length(min=1, max=10))
        custom_list_length = CustomList(
            fields.Str, validate=validate.Length(min=1, max=10)
        )
        string_length = fields.Str(validate=validate.Length(min=1, max=10))
        custom_field_length = CustomStringField(validate=validate.Length(min=1, max=10))
        multiple_lengths = fields.Str(
            validate=[
                validate.Length(min=1),
                validate.Length(min=3),
                validate.Length(max=10),
                validate.Length(max=7),
            ]
        )
        equal_length = fields.Str(
            validate=[validate.Length(equal=5), validate.Length(min=1, max=10)]
        )

    @pytest.mark.parametrize(
        ("field", "properties"),
        [
            ("range", {"minimum": 1, "maximum": 10}),
            ("multiple_ranges", {"minimum": 3, "maximum": 7}),
            ("list_length", {"minItems": 1, "maxItems": 10}),
            ("custom_list_length", {"minItems": 1, "maxItems": 10}),
            ("string_length", {"minLength": 1, "maxLength": 10}),
            ("custom_field_length", {"minLength": 1, "maxLength": 10}),
            ("multiple_lengths", {"minLength": 3, "maxLength": 7}),
            ("equal_length", {"minLength": 5, "maxLength": 5}),
        ],
    )
    def test_properties(self, field, properties, spec):
        spec.components.schema("Validation", schema=self.ValidationSchema)
        result = get_schemas(spec)["Validation"]["properties"][field]

        for attr, expected_value in properties.items():
            assert attr in result
            assert result[attr] == expected_value
