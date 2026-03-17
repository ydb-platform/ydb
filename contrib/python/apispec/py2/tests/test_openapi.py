# -*- coding: utf-8 -*-

import warnings

import pytest
from pytest import mark

import marshmallow
from marshmallow import fields, Schema, validate

from apispec.ext.marshmallow import MarshmallowPlugin
from apispec.ext.marshmallow.openapi import OpenAPIConverter, MARSHMALLOW_VERSION_INFO
from apispec import exceptions, utils, APISpec

@pytest.fixture(params=('2.0', '3.0.0'))
def openapi(request):
    return OpenAPIConverter(openapi_version=request.param)


class TestMarshmallowFieldToOpenAPI:

    def test_field2choices_preserving_order(self, openapi):
        choices = ['a', 'b', 'c', 'aa', '0', 'cc']
        field = fields.String(validate=validate.OneOf(choices))
        assert openapi.field2choices(field) == {'enum': choices}

    @mark.parametrize(
        ('FieldClass', 'jsontype'), [
            (fields.Integer, 'integer'),
            (fields.Number, 'number'),
            (fields.Float, 'number'),
            (fields.String, 'string'),
            (fields.Str, 'string'),
            (fields.Boolean, 'boolean'),
            (fields.Bool, 'boolean'),
            (fields.UUID, 'string'),
            (fields.DateTime, 'string'),
            (fields.Date, 'string'),
            (fields.Time, 'string'),
            (fields.Email, 'string'),
            (fields.URL, 'string'),
            # Assume base Field and Raw are strings
            (fields.Field, 'string'),
            (fields.Raw, 'string'),
        ],
    )
    def test_field2property_type(self, FieldClass, jsontype, openapi):
        field = FieldClass()
        res = openapi.field2property(field)
        assert res['type'] == jsontype

    def test_formatted_field_translates_to_array(self, openapi):
        field = fields.List(fields.String)
        res = openapi.field2property(field)
        assert res['type'] == 'array'
        assert res['items'] == openapi.field2property(fields.String())

    @mark.parametrize(
        ('FieldClass', 'expected_format'), [
            (fields.Integer, 'int32'),
            (fields.Float, 'float'),
            (fields.UUID, 'uuid'),
            (fields.DateTime, 'date-time'),
            (fields.Date, 'date'),
            (fields.Email, 'email'),
            (fields.URL, 'url'),
        ],
    )
    def test_field2property_formats(self, FieldClass, expected_format, openapi):
        field = FieldClass()
        res = openapi.field2property(field)
        assert res['format'] == expected_format

    def test_field_with_description(self, openapi):
        field = fields.Str(description='a username')
        res = openapi.field2property(field)
        assert res['description'] == 'a username'

    def test_field_with_missing(self, openapi):
        field = fields.Str(default='foo', missing='bar')
        res = openapi.field2property(field)
        assert res['default'] == 'bar'

    def test_field_with_boolean_false_missing(self, openapi):
        field = fields.Boolean(default=None, missing=False)
        res = openapi.field2property(field)
        assert res['default'] is False

    def test_field_with_missing_load(self, openapi):
        field = fields.Str(default='foo', missing='bar')
        res = openapi.field2property(field, dump=False)
        assert res['default'] == 'bar'

    def test_field_with_boolean_false_missing_load(self, openapi):
        field = fields.Boolean(default=None, missing=False)
        res = openapi.field2property(field, dump=False)
        assert res['default'] is False

    def test_fields_with_missing_load(self, openapi):
        field_dict = {'field': fields.Str(default='foo', missing='bar')}
        res = openapi.fields2parameters(field_dict, default_in='query')
        if openapi.openapi_version.major < 3:
            assert res[0]['default'] == 'bar'
        else:
            assert res[0]['schema']['default'] == 'bar'

    def test_fields_with_location(self, openapi):
        field_dict = {'field': fields.Str(location='querystring')}
        res = openapi.fields2parameters(field_dict, default_in='headers')
        assert res[0]['in'] == 'query'

    # json/body is invalid for OpenAPI 3
    @pytest.mark.parametrize('openapi', ('2.0', ), indirect=True)
    def test_fields_with_multiple_json_locations(self, openapi):
        field_dict = {
            'field1': fields.Str(location='json', required=True),
            'field2': fields.Str(location='json', required=True),
            'field3': fields.Str(location='json'),
        }
        res = openapi.fields2parameters(field_dict, default_in=None)
        assert len(res) == 1
        assert res[0]['in'] == 'body'
        assert res[0]['required'] is False
        assert 'field1' in res[0]['schema']['properties']
        assert 'field2' in res[0]['schema']['properties']
        assert 'field3' in res[0]['schema']['properties']
        assert 'required' in res[0]['schema']
        assert len(res[0]['schema']['required']) == 2
        assert 'field1' in res[0]['schema']['required']
        assert 'field2' in res[0]['schema']['required']

    def test_fields2parameters_does_not_modify_metadata(self, openapi):
        field_dict = {'field': fields.Str(location='querystring')}
        res = openapi.fields2parameters(field_dict, default_in='headers')
        assert res[0]['in'] == 'query'

        res = openapi.fields2parameters(field_dict, default_in='headers')
        assert res[0]['in'] == 'query'

    def test_fields_location_mapping(self, openapi):
        field_dict = {'field': fields.Str(location='cookies')}
        res = openapi.fields2parameters(field_dict, default_in='headers')
        assert res[0]['in'] == 'cookie'

    def test_fields_default_location_mapping(self, openapi):
        field_dict = {'field': fields.Str()}
        res = openapi.fields2parameters(field_dict, default_in='headers')
        assert res[0]['in'] == 'header'

    # json/body is invalid for OpenAPI 3
    @pytest.mark.parametrize('openapi', ('2.0', ), indirect=True)
    def test_fields_default_location_mapping_if_schema_many(self, openapi):

        class ExampleSchema(Schema):
            id = fields.Int()

        schema = ExampleSchema(many=True)
        res = openapi.fields2parameters(schema.fields, schema=schema, default_in='json')
        assert res[0]['in'] == 'body'

    def test_fields_with_dump_only(self, openapi):
        class UserSchema(Schema):
            name = fields.Str(dump_only=True)
        res = openapi.fields2parameters(UserSchema._declared_fields, default_in='query')
        assert len(res) == 0
        res = openapi.fields2parameters(UserSchema().fields, default_in='query')
        assert len(res) == 0

        class UserSchema(Schema):
            name = fields.Str()

            class Meta:
                dump_only = ('name',)
        res = openapi.fields2parameters(
            UserSchema._declared_fields, schema=UserSchema, default_in='query',
        )
        assert len(res) == 0
        res = openapi.fields2parameters(
            UserSchema().fields, schema=UserSchema, default_in='query',
        )
        assert len(res) == 0

    def test_field_with_choices(self, openapi):
        field = fields.Str(validate=validate.OneOf(['freddie', 'brian', 'john']))
        res = openapi.field2property(field)
        assert set(res['enum']) == {'freddie', 'brian', 'john'}

    def test_field_with_equal(self, openapi):
        field = fields.Str(validate=validate.Equal('only choice'))
        res = openapi.field2property(field)
        assert res['enum'] == ['only choice']

    def test_only_allows_valid_properties_in_metadata(self, openapi):
        field = fields.Str(
            missing='foo',
            description='foo',
            enum=['red', 'blue'],
            allOf=['bar'],
            not_valid='lol',
        )
        res = openapi.field2property(field)
        assert res['default'] == field.missing
        assert 'description' in res
        assert 'enum' in res
        assert 'allOf' in res
        assert 'not_valid' not in res

    def test_field_with_choices_multiple(self, openapi):
        field = fields.Str(validate=[
            validate.OneOf(['freddie', 'brian', 'john']),
            validate.OneOf(['brian', 'john', 'roger']),
        ])
        res = openapi.field2property(field)
        assert set(res['enum']) == {'brian', 'john'}

    def test_field_with_additional_metadata(self, openapi):
        field = fields.Str(minLength=6, maxLength=100)
        res = openapi.field2property(field)
        assert res['maxLength'] == 100
        assert res['minLength'] == 6

    def test_field_with_allow_none(self, openapi):
        field = fields.Str(allow_none=True)
        res = openapi.field2property(field)
        if openapi.openapi_version.major < 3:
            assert res['x-nullable'] is True
        else:
            assert res['nullable'] is True

class TestMarshmallowSchemaToModelDefinition:

    def test_invalid_schema(self, openapi):
        with pytest.raises(ValueError):
            openapi.schema2jsonschema(None)

    def test_schema2jsonschema_with_explicit_fields(self, openapi):
        class UserSchema(Schema):
            _id = fields.Int()
            email = fields.Email(description='email address of the user')
            name = fields.Str()

            class Meta:
                title = 'User'

        res = openapi.schema2jsonschema(UserSchema)
        assert res['title'] == 'User'
        assert res['type'] == 'object'
        props = res['properties']
        assert props['_id']['type'] == 'integer'
        assert props['email']['type'] == 'string'
        assert props['email']['format'] == 'email'
        assert props['email']['description'] == 'email address of the user'

    @pytest.mark.skipif(
        MARSHMALLOW_VERSION_INFO[0] >= 3 and marshmallow.__version__ != '3.0.0b6',  # this version is more 2 than 3
        reason='Behaviour changed in marshmallow 3',
    )
    def test_schema2jsonschema_override_name_ma2(self, openapi):
        class ExampleSchema(Schema):
            _id = fields.Int(load_from='id', dump_to='id')
            _dt = fields.Int(load_from='lf_no_match', dump_to='dt')
            _lf = fields.Int(load_from='lf')
            _global = fields.Int(load_from='global', dump_to='global')

            class Meta:
                exclude = ('_global', )

        res = openapi.schema2jsonschema(ExampleSchema)
        assert res['type'] == 'object'
        props = res['properties']
        # `_id` renamed to `id`
        assert '_id' not in props and props['id']['type'] == 'integer'
        # `load_from` and `dump_to` do not match, `dump_to` is used
        assert 'lf_no_match' not in props
        assert props['dt']['type'] == 'integer'
        # `load_from` and no `dump_to`, `load_from` is used
        assert props['lf']['type'] == 'integer'
        # `_global` excluded correctly
        assert '_global' not in props and 'global' not in props

    @pytest.mark.skipif(
        MARSHMALLOW_VERSION_INFO[0] < 3 or marshmallow.__version__ == '3.0.0b6',
        reason='Behaviour changed in marshmallow 3',
    )
    def test_schema2jsonschema_override_name_ma3(self, openapi):
        class ExampleSchema(Schema):
            _id = fields.Int(data_key='id')
            _global = fields.Int(data_key='global')

            class Meta:
                exclude = ('_global', )

        res = openapi.schema2jsonschema(ExampleSchema)
        assert res['type'] == 'object'
        props = res['properties']
        # `_id` renamed to `id`
        assert '_id' not in props and props['id']['type'] == 'integer'
        # `_global` excluded correctly
        assert '_global' not in props and 'global' not in props

    def test_required_fields(self, openapi):
        class BandSchema(Schema):
            drummer = fields.Str(required=True)
            bassist = fields.Str()
        res = openapi.schema2jsonschema(BandSchema)
        assert res['required'] == ['drummer']

    def test_partial(self, openapi):
        class BandSchema(Schema):
            drummer = fields.Str(required=True)
            bassist = fields.Str(required=True)

        res = openapi.schema2jsonschema(BandSchema(partial=True))
        assert 'required' not in res

        res = openapi.schema2jsonschema(BandSchema(partial=('drummer', )))
        assert res['required'] == ['bassist']

    def test_no_required_fields(self, openapi):
        class BandSchema(Schema):
            drummer = fields.Str()
            bassist = fields.Str()
        res = openapi.schema2jsonschema(BandSchema)
        assert 'required' not in res

    def test_title_and_description_may_be_added(self, openapi):
        class UserSchema(Schema):
            class Meta:
                title = 'User'
                description = 'A registered user'

        res = openapi.schema2jsonschema(UserSchema)
        assert res['description'] == 'A registered user'
        assert res['title'] == 'User'

    def test_excluded_fields(self, openapi):
        class WhiteStripesSchema(Schema):
            class Meta:
                exclude = ('bassist', )
            guitarist = fields.Str()
            drummer = fields.Str()
            bassist = fields.Str()

        res = openapi.schema2jsonschema(WhiteStripesSchema)
        assert set(res['properties'].keys()) == set(['guitarist', 'drummer'])

    def test_only_explicitly_declared_fields_are_translated(self, recwarn, openapi):
        class UserSchema(Schema):
            _id = fields.Int()

            class Meta:
                title = 'User'
                fields = ('_id', 'email', )

        with warnings.catch_warnings():
            warnings.simplefilter('always')
            res = openapi.schema2jsonschema(UserSchema)
            assert res['type'] == 'object'
            props = res['properties']
            assert '_id' in props
            assert 'email' not in props
            warning = recwarn.pop()
            expected_msg = 'Only explicitly-declared fields will be included in the Schema Object.'
            assert expected_msg in str(warning.message)
            assert issubclass(warning.category, UserWarning)

    def test_observed_field_name_for_required_field(self, openapi):
        if MARSHMALLOW_VERSION_INFO[0] < 3 or marshmallow.__version__ == '3.0.0b6':
            fields_dict = {
                'user_id': fields.Int(load_from='id', dump_to='id', required=True),
            }
        else:
            fields_dict = {
                'user_id': fields.Int(data_key='id', required=True),
            }

        res = openapi.fields2jsonschema(fields_dict)
        assert res['required'] == ['id']

    def test_schema_instance_inspection(self, openapi):
        class UserSchema(Schema):
            _id = fields.Int()

        res = openapi.schema2jsonschema(UserSchema())
        assert res['type'] == 'object'
        props = res['properties']
        assert '_id' in props

    def test_schema_instance_inspection_with_many(self, openapi):
        class UserSchema(Schema):
            _id = fields.Int()

        res = openapi.schema2jsonschema(UserSchema(many=True))
        assert res['type'] == 'array'
        assert 'items' in res
        props = res['items']['properties']
        assert '_id' in props

    def test_raises_error_if_no_declared_fields(self, openapi):
        class NotASchema(object):
            pass

        with pytest.raises(ValueError) as excinfo:
            openapi.schema2jsonschema(NotASchema)

        assert excinfo.value.args[0] == ("{0!r} doesn't have either `fields` "
                                         'or `_declared_fields`'.format(NotASchema))

    def test_dump_only_load_only_fields(self, openapi):
        class UserSchema(Schema):
            _id = fields.Str(dump_only=True)
            name = fields.Str()
            password = fields.Str(load_only=True)

        res = openapi.schema2jsonschema(UserSchema())
        props = res['properties']
        assert 'name' in props
        # dump_only field appears with readOnly attribute
        assert '_id' in props
        assert 'readOnly' in props['_id']
        # load_only field appears (writeOnly attribute does not exist)
        assert 'password' in props
        if openapi.openapi_version.major < 3:
            assert 'writeOnly' not in props['password']
        else:
            assert 'writeOnly' in props['password']


class TestMarshmallowSchemaToParameters:

    def test_field_multiple(self, openapi):
        field = fields.List(fields.Str, location='querystring')
        res = openapi.field2parameter(field, name='field')
        assert res['in'] == 'query'
        if openapi.openapi_version.major < 3:
            assert res['type'] == 'array'
            assert res['items']['type'] == 'string'
            assert res['collectionFormat'] == 'multi'
        else:
            assert res['schema']['type'] == 'array'
            assert res['schema']['items']['type'] == 'string'
            assert res['style'] == 'form'
            assert res['explode'] is True

    def test_field_required(self, openapi):
        field = fields.Str(required=True, location='query')
        res = openapi.field2parameter(field, name='field')
        assert res['required'] is True

    def test_invalid_schema(self, openapi):
        with pytest.raises(ValueError):
            openapi.schema2parameters(None)

    # json/body is invalid for OpenAPI 3
    @pytest.mark.parametrize('openapi', ('2.0', ), indirect=True)
    def test_schema_body(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email()

        res = openapi.schema2parameters(UserSchema, default_in='body')
        assert len(res) == 1
        param = res[0]
        assert param['in'] == 'body'
        assert param['schema'] == openapi.schema2jsonschema(UserSchema)

    def test_schema_body_with_dump_only(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email(dump_only=True)

        res_nodump = openapi.schema2parameters(UserSchema, default_in='body')
        assert len(res_nodump) == 1
        param = res_nodump[0]
        assert param['in'] == 'body'
        assert param['schema'] == openapi.schema2jsonschema(UserSchema, dump=False)
        assert set(param['schema']['properties'].keys()) == {'name'}

    # json/body is invalid for OpenAPI 3
    @pytest.mark.parametrize('openapi', ('2.0', ), indirect=True)
    def test_schema_body_many(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email()

        res = openapi.schema2parameters(UserSchema(many=True), default_in='body')
        assert len(res) == 1
        param = res[0]
        assert param['in'] == 'body'
        assert param['schema']['type'] == 'array'
        assert param['schema']['items']['type'] == 'object'
        assert param['schema']['items'] == openapi.schema2jsonschema(UserSchema)

    def test_schema_query(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email()

        res = openapi.schema2parameters(UserSchema, default_in='query')
        assert len(res) == 2
        res.sort(key=lambda param: param['name'])
        assert res[0]['name'] == 'email'
        assert res[0]['in'] == 'query'
        assert res[1]['name'] == 'name'
        assert res[1]['in'] == 'query'

    def test_schema_query_instance(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email()

        res = openapi.schema2parameters(UserSchema(), default_in='query')
        assert len(res) == 2
        res.sort(key=lambda param: param['name'])
        assert res[0]['name'] == 'email'
        assert res[0]['in'] == 'query'
        assert res[1]['name'] == 'name'
        assert res[1]['in'] == 'query'

    def test_schema_query_instance_many_should_raise_exception(self, openapi):
        class UserSchema(Schema):
            name = fields.Str()
            email = fields.Email()

        with pytest.raises(AssertionError):
            openapi.schema2parameters(UserSchema(many=True), default_in='query')

    # json/body is invalid for OpenAPI 3
    @pytest.mark.parametrize('openapi', ('2.0', ), indirect=True)
    def test_fields_default_in_body(self, openapi):
        field_dict = {
            'name': fields.Str(),
            'email': fields.Email(),
        }
        res = openapi.fields2parameters(field_dict)
        assert len(res) == 1
        assert set(res[0]['schema']['properties'].keys()) == {'name', 'email'}

    def test_fields_query(self, openapi):
        field_dict = {
            'name': fields.Str(),
            'email': fields.Email(),
        }
        res = openapi.fields2parameters(field_dict, default_in='query')
        assert len(res) == 2
        res.sort(key=lambda param: param['name'])
        assert res[0]['name'] == 'email'
        assert res[0]['in'] == 'query'
        assert res[1]['name'] == 'name'
        assert res[1]['in'] == 'query'

    def test_raises_error_if_not_a_schema(self, openapi):
        class NotASchema(object):
            pass

        with pytest.raises(ValueError) as excinfo:
            openapi.schema2jsonschema(NotASchema)

        assert excinfo.value.args[0] == ("{0!r} doesn't have either `fields` "
                                         'or `_declared_fields`'.format(NotASchema))


class CategorySchema(Schema):
    id = fields.Int()
    name = fields.Str(required=True)
    breed = fields.Str(dump_only=True)

class PageSchema(Schema):
    offset = fields.Int()
    limit = fields.Int()

class PetSchema(Schema):
    category = fields.Nested(CategorySchema, many=True, ref='#/definitions/Category')
    name = fields.Str()

class PetSchemaV3(Schema):
    category = fields.Nested(CategorySchema, many=True, ref='#/components/schemas/Category')
    name = fields.Str()

class TestNesting:

    @staticmethod
    def ref_path(spec):
        if spec.openapi_version.version[0] < 3:
            return '#/definitions/'
        return '#/components/schemas/'

    def test_field2property_nested_spec_metadatas(self, spec_fixture):
        spec_fixture.spec.definition('Category', schema=CategorySchema)
        category = fields.Nested(CategorySchema, description='A category')
        result = spec_fixture.openapi.field2property(category)
        assert result == {
            '$ref': self.ref_path(spec_fixture.spec) + 'Category',
            'description': 'A category',
        }

    def test_field2property_nested_spec(self, spec_fixture):
        spec_fixture.spec.definition('Category', schema=CategorySchema)
        category = fields.Nested(CategorySchema)
        assert spec_fixture.openapi.field2property(category) == {
            '$ref': self.ref_path(spec_fixture.spec) + 'Category',
        }

    def test_field2property_nested_many_spec(self, spec_fixture):
        spec_fixture.spec.definition('Category', schema=CategorySchema)
        category = fields.Nested(CategorySchema, many=True)
        ret = spec_fixture.openapi.field2property(category)
        assert ret['type'] == 'array'
        assert ret['items'] == {'$ref': self.ref_path(spec_fixture.spec) + 'Category'}

    def test_field2property_nested_ref(self, openapi):
        category = fields.Nested(CategorySchema)
        assert openapi.field2property(category) == openapi.schema2jsonschema(CategorySchema)

        cat_with_ref = fields.Nested(CategorySchema, ref='Category')
        assert openapi.field2property(cat_with_ref) == {'$ref': 'Category'}

    def test_field2property_nested_ref_with_meta(self, openapi):
        category = fields.Nested(CategorySchema)
        assert openapi.field2property(category) == openapi.schema2jsonschema(CategorySchema)

        cat_with_ref = fields.Nested(CategorySchema, ref='Category', description='A category')
        result = openapi.field2property(cat_with_ref)
        assert result == {'$ref': 'Category', 'description': 'A category'}

    def test_field2property_nested_many(self, openapi):
        categories = fields.Nested(CategorySchema, many=True)
        res = openapi.field2property(categories)
        assert res['type'] == 'array'
        assert res['items'] == openapi.schema2jsonschema(CategorySchema)

        cats_with_ref = fields.Nested(CategorySchema, many=True, ref='Category')
        res = openapi.field2property(cats_with_ref)
        assert res['type'] == 'array'
        assert res['items'] == {'$ref': 'Category'}

    def test_field2property_nested_self_without_name_raises_error(self, openapi):
        self_nesting = fields.Nested('self')
        with pytest.raises(ValueError):
            openapi.field2property(self_nesting)

    def test_field2property_nested_self(self, openapi):
        self_nesting = fields.Nested('self')
        res = openapi.field2property(self_nesting, name='Foo')
        if openapi.openapi_version.major < 3:
            assert res == {'$ref': '#/definitions/Foo'}
        else:
            assert res == {'$ref': '#/components/schemas/Foo'}

    def test_field2property_nested_self_many(self, openapi):
        self_nesting = fields.Nested('self', many=True)
        res = openapi.field2property(self_nesting, name='Foo')
        if openapi.openapi_version.major < 3:
            assert res == {'type': 'array', 'items': {'$ref': '#/definitions/Foo'}}
        else:
            assert res == {'type': 'array', 'items': {'$ref': '#/components/schemas/Foo'}}

    def test_field2property_nested_self_ref_with_meta(self, openapi):
        self_nesting = fields.Nested('self', ref='#/definitions/Bar')
        res = openapi.field2property(self_nesting)
        assert res == {'$ref': '#/definitions/Bar'}

        self_nesting2 = fields.Nested('self', ref='#/definitions/Bar')
        # name is passed
        res = openapi.field2property(self_nesting2, name='Foo')
        assert res == {'$ref': '#/definitions/Bar'}

    def test_field2property_nested_dump_only(self, openapi):
        category = fields.Nested(CategorySchema)
        res = openapi.field2property(category, name='Foo', dump=False)
        props = res['properties']
        assert 'breed' not in props

    def test_field2property_nested_dump_only_with_spec(self, openapi):
        category = fields.Nested(CategorySchema)
        res = openapi.field2property(category, name='Foo', dump=False)
        props = res['properties']
        assert 'breed' not in props

    def test_schema2jsonschema_with_nested_fields(self, openapi):
        res = openapi.schema2jsonschema(PetSchema, use_refs=False)
        props = res['properties']
        assert props['category']['items'] == openapi.schema2jsonschema(CategorySchema)

    def test_schema2jsonschema_with_nested_fields_with_adhoc_changes(self, openapi):
        category_schema = CategorySchema(many=True)
        category_schema.fields['id'].required = True

        class PetSchema(Schema):
            category = fields.Nested(category_schema, many=True, ref='#/definitions/Category')
            name = fields.Str()

        res = openapi.schema2jsonschema(PetSchema(), use_refs=False)
        props = res['properties']
        assert props['category'] == openapi.schema2jsonschema(category_schema)
        assert set(props['category']['items']['required']) == {'id', 'name'}

        props['category']['items']['required'] = ['name']
        assert props['category']['items'] == openapi.schema2jsonschema(CategorySchema)

    def test_schema2jsonschema_with_nested_excluded_fields(self, openapi):
        category_schema = CategorySchema(exclude=('breed', ))

        class PetSchema(Schema):
            category = fields.Nested(category_schema)

        res = openapi.schema2jsonschema(PetSchema(), use_refs=False)
        category_props = res['properties']['category']['properties']
        assert 'breed' not in category_props

    def test_nested_field_with_property(self, spec_fixture):
        ref_path = self.ref_path(spec_fixture.spec)

        category_1 = fields.Nested(CategorySchema)
        category_2 = fields.Nested(CategorySchema, ref=ref_path + 'Category')
        category_3 = fields.Nested(CategorySchema, dump_only=True)
        category_4 = fields.Nested(CategorySchema, dump_only=True, ref=ref_path + 'Category')
        category_5 = fields.Nested(CategorySchema, many=True)
        category_6 = fields.Nested(CategorySchema, many=True, ref=ref_path + 'Category')
        category_7 = fields.Nested(CategorySchema, many=True, dump_only=True)
        category_8 = fields.Nested(CategorySchema, many=True, dump_only=True, ref=ref_path + 'Category')
        spec_fixture.spec.definition('Category', schema=CategorySchema)

        assert spec_fixture.openapi.field2property(category_1) == {
            '$ref': ref_path + 'Category',
        }
        assert spec_fixture.openapi.field2property(category_2) == {
            '$ref': ref_path + 'Category',
        }
        assert spec_fixture.openapi.field2property(category_3) == {
            'allOf': [{'$ref': ref_path + 'Category'}], 'readOnly': True,
        }
        assert spec_fixture.openapi.field2property(category_4) == {
            'allOf': [{'$ref': ref_path + 'Category'}], 'readOnly': True,
        }
        assert spec_fixture.openapi.field2property(category_5) == {
            'items': {'$ref': ref_path + 'Category'}, 'type': 'array',
        }
        assert spec_fixture.openapi.field2property(category_6) == {
            'items': {'$ref': ref_path + 'Category'}, 'type': 'array',
        }
        assert spec_fixture.openapi.field2property(category_7) == {
            'items': {'$ref': ref_path + 'Category'}, 'readOnly': True, 'type': 'array',
        }
        assert spec_fixture.openapi.field2property(category_8) == {
            'items': {'$ref': ref_path + 'Category'}, 'readOnly': True, 'type': 'array',
        }

def _test_openapi_tools_validate_v2():
    ma_plugin = MarshmallowPlugin()
    spec = APISpec(
        title='Pets',
        version='0.1',
        plugins=(ma_plugin, ),
        openapi_version='2.0',
    )
    openapi = ma_plugin.openapi

    spec.definition('Category', schema=CategorySchema)
    spec.definition('Pet', schema=PetSchema, extra_fields={'discriminator': 'name'})

    spec.add_path(
        view=None,
        path='/category/{category_id}',
        operations={
            'get': {
                'parameters': [
                    {'name': 'q', 'in': 'query', 'type': 'string'},
                    {'name': 'category_id', 'in': 'path', 'required': True, 'type': 'string'},
                    openapi.field2parameter(
                        field=fields.List(
                            fields.Str(),
                            validate=validate.OneOf(['freddie', 'roger']),
                            location='querystring',
                        ),
                        name='body',
                        use_refs=False,
                    ),
                ] + openapi.schema2parameters(PageSchema, default_in='query'),
                'responses': {
                    200: {
                        'schema': PetSchema,
                        'description': 'A pet',
                    },
                },
            },
            'post': {
                'parameters': (
                    [{'name': 'category_id', 'in': 'path', 'required': True, 'type': 'string'}] +
                    openapi.schema2parameters(CategorySchema, default_in='body')
                ),
                'responses': {
                    201: {
                        'schema': PetSchema,
                        'description': 'A pet',
                    },
                },
            },
        },
    )
    try:
        utils.validate_spec(spec)
    except exceptions.OpenAPIError as error:
        pytest.fail(str(error))

def _test_openapi_tools_validate_v3():
    ma_plugin = MarshmallowPlugin()
    spec = APISpec(
        title='Pets',
        version='0.1',
        plugins=(ma_plugin, ),
        openapi_version='3.0.0',
    )
    #openapi = ma_plugin.openapi

    spec.definition('Category', schema=CategorySchema)
    spec.definition('Pet', schema=PetSchemaV3)

    spec.add_path(
        view=None,
        path='/category/{category_id}',
        operations={
            'get': {
                'parameters': [
                    {
                        'name': 'q',
                        'in': 'query',
                        'schema': {'type': 'string'},
                    },
                    {
                        'name': 'category_id',
                        'in': 'path',
                        'required': True,
                        'schema': {'type': 'string'},
                    },
                ],  # + openapi.schema2parameters(PageSchema, default_in='query'),
                'responses': {
                    200: {
                        'description': 'success',
                        'content': {
                            'application/json': {
                                'schema': PetSchemaV3,
                            },
                        },
                    },
                },
            },
            'post': {
                'parameters': (
                    [
                        {
                            'name': 'category_id',
                            'in': 'path',
                            'required': True,
                            'schema': {'type': 'string'},
                        },
                    ]
                ),
                'requestBody': {
                    'content': {
                        'application/json': {
                            'schema': CategorySchema,
                        },
                    },
                },
                'responses': {
                    201: {
                        'description': 'created',
                        'content': {
                            'application/json': {
                                'schema': PetSchemaV3,
                            },
                        },
                    },
                },
            },
        },
    )
    try:
        utils.validate_spec(spec)
    except exceptions.OpenAPIError as error:
        pytest.fail(str(error))

class ValidationSchema(Schema):
    id = fields.Int(dump_only=True)
    range = fields.Int(validate=validate.Range(min=1, max=10))
    multiple_ranges = fields.Int(validate=[
        validate.Range(min=1),
        validate.Range(min=3),
        validate.Range(max=10),
        validate.Range(max=7),
    ])
    list_length = fields.List(fields.Str, validate=validate.Length(min=1, max=10))
    string_length = fields.Str(validate=validate.Length(min=1, max=10))
    multiple_lengths = fields.Str(validate=[
        validate.Length(min=1),
        validate.Length(min=3),
        validate.Length(max=10),
        validate.Length(max=7),
    ])
    equal_length = fields.Str(validate=[
        validate.Length(equal=5),
        validate.Length(min=1, max=10),
    ])

class TestFieldValidation:

    def test_range(self, spec):
        spec.definition('Validation', schema=ValidationSchema)
        result = spec._definitions['Validation']['properties']['range']

        assert 'minimum' in result
        assert result['minimum'] == 1
        assert 'maximum' in result
        assert result['maximum'] == 10

    def test_multiple_ranges(self, spec):
        spec.definition('Validation', schema=ValidationSchema)
        result = spec._definitions['Validation']['properties']['multiple_ranges']

        assert 'minimum' in result
        assert result['minimum'] == 3
        assert 'maximum' in result
        assert result['maximum'] == 7

    def test_list_length(self, spec):
        spec.definition('Validation', schema=ValidationSchema)
        result = spec._definitions['Validation']['properties']['list_length']

        assert 'minItems' in result
        assert result['minItems'] == 1
        assert 'maxItems' in result
        assert result['maxItems'] == 10

    def test_string_length(self, spec):
        spec.definition('Validation', schema=ValidationSchema)
        result = spec._definitions['Validation']['properties']['string_length']

        assert 'minLength' in result
        assert result['minLength'] == 1
        assert 'maxLength' in result
        assert result['maxLength'] == 10

    def test_multiple_lengths(self, spec):
        spec.definition('Validation', schema=ValidationSchema)
        result = spec._definitions['Validation']['properties']['multiple_lengths']

        assert 'minLength' in result
        assert result['minLength'] == 3
        assert 'maxLength' in result
        assert result['maxLength'] == 7

    def test_equal_length(self, spec):
        spec.definition('Validation', schema=ValidationSchema)
        result = spec._definitions['Validation']['properties']['equal_length']

        assert 'minLength' in result
        assert result['minLength'] == 5
        assert 'maxLength' in result
        assert result['maxLength'] == 5
