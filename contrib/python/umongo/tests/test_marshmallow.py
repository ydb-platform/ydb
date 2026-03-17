"""Test marshmallow-related features"""
import datetime as dt

import pytest

from bson import ObjectId
import marshmallow as ma

from umongo import (
    Document, EmbeddedDocument, fields, set_gettext, validate, missing, RemoveMissingSchema
)
from umongo import marshmallow_bonus as ma_bonus_fields
from umongo.abstract import BaseField, BaseSchema, BaseMarshmallowSchema

from .common import BaseTest


class TestMarshmallow(BaseTest):

    def teardown_method(self, method):
        # Reset i18n config before each test
        set_gettext(None)

    def setup(self):
        super().setup()

        class User(Document):
            name = fields.StrField()
            birthday = fields.DateTimeField()

        self.User = self.instance.register(User)

    def test_by_field(self):
        ma_name_field = self.User.schema.fields['name'].as_marshmallow_field()
        assert isinstance(ma_name_field, ma.fields.Field)
        assert not isinstance(ma_name_field, BaseField)

    def test_by_schema(self):
        ma_schema_cls = self.User.schema.as_marshmallow_schema()
        assert issubclass(ma_schema_cls, ma.Schema)
        assert not issubclass(ma_schema_cls, BaseSchema)

    def test_base_marshmallow_schema(self):
        ma_schema_cls = self.User.schema.as_marshmallow_schema()
        assert ma_schema_cls.Meta.ordered

    def test_custom_ma_base_schema_cls(self):

        # Define custom marshmallow schema base class
        class ExcludeBaseSchema(ma.Schema):
            class Meta:
                unknown = ma.EXCLUDE

        # Typically, we'll use it in all our schemas, so let's define base
        # Document and EmbeddedDocument classes using this base schema class
        @self.instance.register
        class MyDocument(Document):
            MA_BASE_SCHEMA_CLS = ExcludeBaseSchema

            class Meta:
                abstract = True

        @self.instance.register
        class MyEmbeddedDocument(EmbeddedDocument):
            MA_BASE_SCHEMA_CLS = ExcludeBaseSchema

            class Meta:
                abstract = True

        # Now, all our objects will generate "exclude" marshmallow schemas
        @self.instance.register
        class Accessory(MyEmbeddedDocument):
            brief = fields.StrField()
            value = fields.IntField()

        @self.instance.register
        class Bag(MyDocument):
            item = fields.EmbeddedField(Accessory)
            content = fields.ListField(fields.EmbeddedField(Accessory))

        data = {
            'item': {'brief': 'sportbag', 'value': 100, 'name': 'Unknown'},
            'content': [
                {'brief': 'cellphone', 'value': 500, 'name': 'Unknown'},
                {'brief': 'lighter', 'value': 2, 'name': 'Unknown'}
            ],
            'name': 'Unknown',
        }
        excl_data = {
            'item': {'brief': 'sportbag', 'value': 100},
            'content': [
                {'brief': 'cellphone', 'value': 500},
                {'brief': 'lighter', 'value': 2}]
        }

        ma_schema = Bag.schema.as_marshmallow_schema()
        assert ma_schema().load(data) == excl_data

    def test_as_marshmallow_field_pass_params(self):
        @self.instance.register
        class MyDoc(Document):
            dk = fields.IntField(marshmallow_data_key='dkdk')
            at = fields.IntField(marshmallow_attribute='atat')
            re = fields.IntField(marshmallow_required=True)
            an = fields.IntField(marshmallow_allow_none=True)
            lo = fields.IntField(marshmallow_load_only=True)
            do = fields.IntField(marshmallow_dump_only=True)
            va = fields.IntField(marshmallow_validate=validate.Range(min=0))
            em = fields.IntField(marshmallow_error_messages={'invalid': 'Wrong'})

        MyMaDoc = MyDoc.schema.as_marshmallow_schema()

        assert MyMaDoc().fields['dk'].data_key == 'dkdk'
        assert MyMaDoc().fields['at'].attribute == 'atat'
        assert MyMaDoc().fields['re'].required is True
        assert MyMaDoc().fields['an'].allow_none is True
        assert MyMaDoc().fields['lo'].load_only is True
        assert MyMaDoc().fields['do'].dump_only is True
        with pytest.raises(ma.ValidationError) as excinfo:
            MyMaDoc().load({'va': -1})
        assert 'va' in excinfo.value.messages
        assert MyMaDoc().fields['em'].error_messages['invalid'] == 'Wrong'

    def test_as_marshmallow_field_infer_missing_default(self):
        @self.instance.register
        class MyDoc(Document):
            de = fields.IntField(default=42)
            mm = fields.IntField(marshmallow_missing=12)
            md = fields.IntField(marshmallow_default=12)
            mmd = fields.IntField(default=42, marshmallow_missing=12)
            mdd = fields.IntField(default=42, marshmallow_default=12)

        MyMaDoc = MyDoc.schema.as_marshmallow_schema()

        data = MyMaDoc().load({})
        assert data == {
            'de': 42,
            'mm': 12,
            'mmd': 12,
            'mdd': 42,
        }

        data = MyMaDoc().dump({})
        assert data == {
            'de': 42,
            'md': 12,
            'mmd': 42,
            'mdd': 12,
        }

    def test_as_marshmallow_schema_cache(self):
        ma_schema_cls = self.User.schema.as_marshmallow_schema()
        new_ma_schema_cls = self.User.schema.as_marshmallow_schema()
        assert new_ma_schema_cls == ma_schema_cls

    def test_keep_attributes(self):
        @self.instance.register
        class Vehicle(Document):
            brand = fields.StrField(metadata={"description": "Manufacturer name"})
            category = fields.StrField(required=True)
            nb_wheels = fields.IntField(default=4)

        ma_schema_cls = Vehicle.schema.as_marshmallow_schema()
        schema = ma_schema_cls()

        with pytest.raises(ma.ValidationError) as excinfo:
            schema.load({})
        assert excinfo.value.messages == {'category': ['Missing data for required field.']}
        assert schema.load({'category': 'Car'}) == {'category': 'Car', 'nb_wheels': 4}

        assert schema.fields['brand'].metadata['description'] == 'Manufacturer name'

    def test_keep_validators(self):
        @self.instance.register
        class WithMailUser(self.User):
            email = fields.StrField(validate=[validate.Email(), validate.Length(max=100)])
            number_of_legs = fields.IntField(validate=[validate.OneOf([0, 1, 2])])

        ma_schema_cls = WithMailUser.schema.as_marshmallow_schema()
        schema = ma_schema_cls()

        with pytest.raises(ma.ValidationError) as excinfo:
            schema.load({'email': 'a' * 100 + '@user.com', 'number_of_legs': 4})
        assert excinfo.value.messages == {
            'email': ['Longer than maximum length 100.'],
            'number_of_legs': ['Must be one of: 0, 1, 2.']}

        data = {'email': 'user@user.com', 'number_of_legs': 2}
        assert schema.load(data) == data

    def test_inheritance(self):
        @self.instance.register
        class AdvancedUser(self.User):
            name = fields.StrField(default='1337')
            is_left_handed = fields.BooleanField()

        ma_schema_cls = AdvancedUser.schema.as_marshmallow_schema()
        schema = ma_schema_cls()

        assert schema.dump({'is_left_handed': True}) == {
            'name': '1337', 'is_left_handed': True, 'cls': 'AdvancedUser'}

    def test_to_mongo(self):
        @self.instance.register
        class Dog(Document):
            name = fields.StrField(attribute='_id', required=True)
            age = fields.IntField()

        payload = {'name': 'Scruffy', 'age': 2}
        ma_schema_cls = Dog.schema.as_marshmallow_schema()

        ret = ma_schema_cls().load(payload)
        assert ret == {'name': 'Scruffy', 'age': 2}
        assert ma_schema_cls().dump(ret) == payload

    def test_i18n(self):
        # i18n support should be kept, because it's pretty cool to have this !
        def my_gettext(message):
            return 'OMG !!! ' + message

        set_gettext(my_gettext)

        ma_schema_cls = self.User.schema.as_marshmallow_schema()
        with pytest.raises(ma.ValidationError) as excinfo:
            ma_schema_cls().load({'name': 'John', 'birthday': 'not_a_date', 'dummy_field': 'dummy'})
        assert excinfo.value.messages == {
            'birthday': ['OMG !!! Not a valid datetime.'],
            'dummy_field': ['OMG !!! Unknown field.']}

    def test_unknown_fields(self):

        class ExcludeBaseSchema(ma.Schema):
            class Meta:
                unknown = ma.EXCLUDE

        @self.instance.register
        class ExcludeUser(self.User):
            MA_BASE_SCHEMA_CLS = ExcludeBaseSchema

        user_ma_schema_cls = self.User.schema.as_marshmallow_schema()
        assert issubclass(user_ma_schema_cls, ma.Schema)
        exclude_user_ma_schema_cls = ExcludeUser.schema.as_marshmallow_schema()
        assert issubclass(exclude_user_ma_schema_cls, ExcludeBaseSchema)

        data = {'name': 'John', 'dummy': 'dummy'}
        excl_data = {'name': 'John'}

        # By default, marshmallow schemas raise on unknown fields
        with pytest.raises(ma.ValidationError) as excinfo:
            user_ma_schema_cls().load(data)
        assert excinfo.value.messages == {'dummy': ['Unknown field.']}

        # With custom schema, exclude unknown fields
        assert exclude_user_ma_schema_cls().load(data) == excl_data

    def test_missing_accessor(self):

        @self.instance.register
        class WithDefault(Document):
            with_umongo_default = fields.DateTimeField(default=dt.datetime(1999, 1, 1))
            with_marshmallow_missing = fields.DateTimeField(
                marshmallow_missing=dt.datetime(2000, 1, 1))
            with_marshmallow_default = fields.DateTimeField(
                marshmallow_default=dt.datetime(2001, 1, 1))
            with_marshmallow_and_umongo = fields.DateTimeField(
                default=dt.datetime(1999, 1, 1),
                marshmallow_missing=dt.datetime(2000, 1, 1),
                marshmallow_default=dt.datetime(2001, 1, 1)
            )
            with_force_missing = fields.DateTimeField(
                default=dt.datetime(2001, 1, 1),
                marshmallow_missing=missing,
                marshmallow_default=missing
            )
            with_nothing = fields.StrField()

        ma_schema = WithDefault.schema.as_marshmallow_schema()()
        assert ma_schema.dump({}) == {
            'with_umongo_default': '1999-01-01T00:00:00',
            'with_marshmallow_default': '2001-01-01T00:00:00',
            'with_marshmallow_and_umongo': '2001-01-01T00:00:00',
        }
        assert ma_schema.load({}) == {
            'with_umongo_default': dt.datetime(1999, 1, 1),
            'with_marshmallow_missing': dt.datetime(2000, 1, 1),
            'with_marshmallow_and_umongo': dt.datetime(2000, 1, 1),
        }

    def test_nested_field(self):
        @self.instance.register
        class Accessory(EmbeddedDocument):
            brief = fields.StrField(attribute='id', required=True)
            value = fields.IntField()

        @self.instance.register
        class Bag(Document):
            id = fields.EmbeddedField(Accessory, attribute='_id', required=True)
            content = fields.ListField(fields.EmbeddedField(Accessory))

        data = {
            'id': {'brief': 'sportbag', 'value': 100},
            'content': [{'brief': 'cellphone', 'value': 500}, {'brief': 'lighter', 'value': 2}]
        }

        # Here data is the same in both OO world and user world
        # (no ObjectId to str conversion needed for example)

        ma_schema = Bag.schema.as_marshmallow_schema()()

        bag = Bag(**data)
        assert ma_schema.dump(bag) == data
        assert ma_schema.load(data) == data

    def test_marshmallow_bonus_fields(self):
        # Fields related to mongodb provided for marshmallow
        @self.instance.register
        class Doc(Document):
            id = fields.ObjectIdField(attribute='_id')
            ref = fields.ReferenceField('Doc')
            gen_ref = fields.GenericReferenceField()

        for name, field_cls in (
                ('id', ma_bonus_fields.ObjectId),
                ('ref', ma_bonus_fields.ObjectId),
                ('gen_ref', ma_bonus_fields.GenericReference)
        ):
            ma_field = Doc.schema.fields[name].as_marshmallow_field()
            assert isinstance(ma_field, field_cls)
            assert not isinstance(ma_field, BaseField)

        oo_data = {
            'id': ObjectId("57c1a71113adf27ab96b2c4f"),
            'ref': ObjectId("57c1a71113adf27ab96b2c4f"),
            "gen_ref": {'cls': 'Doc', 'id': ObjectId("57c1a71113adf27ab96b2c4f")}
        }
        serialized = {
            'id': "57c1a71113adf27ab96b2c4f",
            'ref': "57c1a71113adf27ab96b2c4f",
            "gen_ref": {'cls': 'Doc', 'id': "57c1a71113adf27ab96b2c4f"}
        }
        doc = Doc(**oo_data)
        ma_schema_cls = Doc.schema.as_marshmallow_schema()
        ma_schema = ma_schema_cls()
        # Dump uMongo object
        assert ma_schema.dump(doc) == serialized
        # Dump OO data (not uMongo object) to ensure bonus fields round-trip
        assert ma_schema.dump(oo_data) == serialized
        # Load serialized data
        assert ma_schema.load(serialized) == oo_data

    def test_marshmallow_bonus_objectid_field(self):

        class DocSchema(ma.Schema):
            id = ma_bonus_fields.ObjectId()

            class Meta:
                strict = True

        schema = DocSchema()

        assert schema.load({"id": "57c1a71113adf27ab96b2c4f"}) == {
            "id": ObjectId("57c1a71113adf27ab96b2c4f")}

        for invalid_id in ("lol", [1, 2], {"1", 2}):
            with pytest.raises(ma.ValidationError) as exc:
                schema.load({"id": invalid_id})
            assert exc.value.messages == {"id": ["Invalid ObjectId."]}

    def test_marshmallow_remove_missing_schema(self):

        @self.instance.register
        class Doc(Document):
            a = fields.IntField()

            @property
            def prop(self):
                return "I'm a property !"

        class VanillaDocSchema(ma.Schema):
            a = ma.fields.Int()

        class RemoveMissingDocSchema(RemoveMissingSchema):
            a = ma.fields.Int()

        data = VanillaDocSchema().dump(Doc())
        assert data == {'a': None}

        data = RemoveMissingDocSchema().dump(Doc())
        assert data == {}

        data = RemoveMissingDocSchema().dump(Doc(a=1))
        assert data == {'a': 1}

    @pytest.mark.parametrize("base_schema", (BaseMarshmallowSchema, ma.Schema))
    def test_marshmallow_base_schema_remove_missing(self, base_schema):
        """Test remove missing feature in base marshmallow Schema

        Also test opting out by setting a pure marshmallow Schema for base
        """
        # Typically, we'll use it in all our schemas, so let's define base
        # Document and EmbeddedDocument classes using this base schema class
        @self.instance.register
        class MyDocument(Document):
            MA_BASE_SCHEMA_CLS = base_schema

            class Meta:
                abstract = True

        @self.instance.register
        class MyEmbeddedDocument(EmbeddedDocument):
            MA_BASE_SCHEMA_CLS = base_schema

            class Meta:
                abstract = True

        @self.instance.register
        class Accessory(MyEmbeddedDocument):
            brief = fields.StrField()
            value = fields.IntField()

        @self.instance.register
        class Bag(MyDocument):
            item = fields.EmbeddedField(Accessory)
            content = fields.ListField(fields.EmbeddedField(Accessory))

        data = {
            'item': {'brief': 'sportbag'},
            'content': [
                {'brief': 'cellphone'},
                {'brief': 'lighter'}]
        }
        dump = {
            'id': None,
            'content': [
                {'brief': 'cellphone', 'value': None},
                {'brief': 'lighter', 'value': None}
            ],
            'item': {'brief': 'sportbag', 'value': None}
        }
        remove_missing_dump = {
            'item': {'brief': 'sportbag'},
            'content': [
                {'brief': 'cellphone'},
                {'brief': 'lighter'}
            ]
        }
        expected_dump = {
            BaseMarshmallowSchema: remove_missing_dump,
            ma.Schema: dump,
        }[base_schema]

        bag = Bag(**data)
        ma_schema = Bag.schema.as_marshmallow_schema()
        assert ma_schema().dump(bag) == expected_dump

    def test_marshmallow_access_custom_attributes(self):

        @self.instance.register
        class Doc(EmbeddedDocument):
            a = fields.IntField()

            attribute_foo = 'foo'

            @property
            def str_prop(self):
                return "I'm a property !"

            @property
            def none_prop(self):
                return None

            @property
            def missing_prop(self):
                return ma.missing

        class Schema(Doc.schema.as_marshmallow_schema()):
            str_prop = ma.fields.Str(dump_only=True)
            none_prop = ma.fields.Str(allow_none=True, dump_only=True)
            missing_prop = ma.fields.Str(dump_only=True)
            attribute_foo = ma.fields.Str(dump_only=True)

        assert Schema().dump(Doc(a=1)) == {
            'a': 1,
            'str_prop': "I'm a property !",
            'none_prop': None,
            'attribute_foo': 'foo',
        }

    def test_dump_only(self):

        @self.instance.register
        class Doc(Document):
            dl = fields.IntField()
            do = fields.IntField(dump_only=True)
            lo = fields.IntField(load_only=True)
            nope = fields.IntField(dump_only=True, load_only=True)

        with pytest.raises(ma.ValidationError):
            Doc(do=1)

        with pytest.raises(ma.ValidationError):
            Doc(nope=1)

        assert Doc(dl=1, lo=2).dump() == {'dl': 1}

        with pytest.raises(ma.ValidationError) as excinfo:
            Doc(nope=ma.missing, do=ma.missing)
        assert excinfo.value.messages == {'nope': ['Unknown field.'], 'do': ['Unknown field.']}
