import pytest

from bson import ObjectId
import marshmallow as ma

from umongo import fields, EmbeddedDocument, validate, exceptions
from umongo.abstract import BaseSchema
from umongo.data_proxy import data_proxy_factory, BaseDataProxy, BaseNonStrictDataProxy

from .common import BaseTest, assert_equal_order


class TestDataProxy(BaseTest):

    def test_repr(self):

        class MySchema(BaseSchema):
            field_a = fields.IntField(attribute='mongo_field_a')
            field_b = fields.StrField()

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy({'field_a': 1, 'field_b': 'value'})
        assert MyDataProxy.__name__ == 'MyDataProxy'
        repr_d = repr(d)
        assert repr_d.startswith("<MyDataProxy(")
        assert "'field_a': 1" in repr_d
        assert "'field_b': 'value'" in repr_d

    def test_simple(self):

        class MySchema(BaseSchema):
            a = fields.IntField()
            b = fields.IntField()

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        d.load({'a': 1, 'b': 2})
        assert d.get('a') == 1
        d.set('b', 3)
        assert d.get('b') == 3
        assert d._data == {'a': 1, 'b': 3}
        assert d.dump() == {'a': 1, 'b': 3}
        d.delete('b')
        assert d._data == {'a': 1, 'b': ma.missing}
        assert d.dump() == {'a': 1}

    def test_load(self):

        class MySchema(BaseSchema):
            a = fields.IntField()
            b = fields.IntField(attribute='in_mongo_b')

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        d.from_mongo({'a': 1, 'in_mongo_b': 2})
        assert d.to_mongo() == {'a': 1, 'in_mongo_b': 2}

        d.set('a', 3)
        assert d.to_mongo(update=True) == {'$set': {'a': 3}}

        d.from_mongo({'a': 4, 'in_mongo_b': 5})
        assert d.to_mongo(update=True) is None
        assert d.to_mongo() == {'a': 4, 'in_mongo_b': 5}

        d2 = MyDataProxy(data={'a': 4, 'b': 5})
        assert d == d2

    def test_modify(self):

        class MySchema(BaseSchema):
            a = fields.IntField()
            b = fields.IntField(attribute='in_mongo_b')

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        assert d.get_modified_fields() == set()
        d.load({'a': 1, 'b': 2})
        assert d.get_modified_fields() == {'a', 'b'}
        d.from_mongo({'a': 1, 'in_mongo_b': 2})
        assert d.get_modified_fields() == set()
        assert d.to_mongo() == {'a': 1, 'in_mongo_b': 2}
        assert d.to_mongo(update=True) is None
        d.set('a', 3)
        d.delete('b')
        assert d.to_mongo(update=True) == {'$set': {'a': 3}, '$unset': {'in_mongo_b': ''}}
        d.clear_modified()
        assert d.get_modified_fields() == set()
        assert d.to_mongo(update=True) is None
        assert d.to_mongo() == {'a': 3}

    def test_list_field_modify(self):

        class MySchema(BaseSchema):
            a = fields.ListField(fields.IntField())
            b = fields.ListField(fields.IntField(), attribute='in_mongo_b')

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        assert d.get_modified_fields() == set()
        d.load({'a': [1], 'b': [2, 2]})
        assert d.get_modified_fields() == {'a', 'b'}
        d.from_mongo({'a': [1], 'in_mongo_b': [2, 2]})
        assert d.get_modified_fields() == set()
        assert d.to_mongo() == {'a': [1], 'in_mongo_b': [2, 2]}
        assert d.to_mongo(update=True) is None
        d.set('a', [3, 3, 3])
        d.delete('b')
        assert d.to_mongo(update=True) == {'$set': {'a': [3, 3, 3]}, '$unset': {'in_mongo_b': ''}}
        d.clear_modified()
        assert d.get_modified_fields() == set()
        assert d.to_mongo() == {'a': [3, 3, 3]}
        assert d.to_mongo(update=True) is None
        d.clear_modified()
        d.load({'a': [1], 'b': [2, 2]})
        d._data['a'].append(1)
        d._data['in_mongo_b'].append(2)
        assert d.get_modified_fields() == {'a', 'b'}
        assert d.to_mongo() == {'a': [1, 1], 'in_mongo_b': [2, 2, 2]}
        assert d.to_mongo(update=True) == {'$set': {'a': [1, 1], 'in_mongo_b': [2, 2, 2]}}
        d.clear_modified()
        del d._data['a'][0]
        del d._data['in_mongo_b'][0]
        assert d.get_modified_fields() == {'a', 'b'}
        assert d.to_mongo() == {'a': [1], 'in_mongo_b': [2, 2]}
        assert d.to_mongo(update=True) == {'$set': {'a': [1], 'in_mongo_b': [2, 2]}}
        d.clear_modified()
        d._data['a'].clear()
        d._data['in_mongo_b'].clear()
        assert d.get_modified_fields() == {'a', 'b'}
        assert d.to_mongo() == {'a': [], 'in_mongo_b': []}
        assert d.to_mongo(update=True) == {'$set': {'a': [], 'in_mongo_b': []}}

    def test_complex_field_clear_modified(self):

        @self.instance.register
        class MyEmbedded(EmbeddedDocument):
            aa = fields.IntField()

        class MySchema(BaseSchema):
            # EmbeddedField need instance to retrieve implementation
            a = fields.EmbeddedField(MyEmbedded, instance=self.instance)
            b = fields.ListField(fields.IntField)

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        d.load({'a': {'aa': 1}, 'b': [2, 3]})
        assert d.to_mongo() == {'a': {'aa': 1}, 'b': [2, 3]}
        d.get('a').aa = 4
        d.get('b').append(5)
        assert d.to_mongo(update=True) == {'$set': {'a': {'aa': 4}, 'b': [2, 3, 5]}}
        d.clear_modified()
        assert d.to_mongo(update=True) is None
        assert not d.get('a').is_modified()
        assert not d.get('b').is_modified()

    def test_set(self):

        class MySchema(BaseSchema):
            a = fields.IntField()
            b = fields.IntField(attribute='in_mongo_b')
            c = fields.StrField(
                allow_none=True,
                validate=validate.Length(min=1, max=5)
            )
            d = fields.StrField()

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        d.from_mongo({'a': 1, 'in_mongo_b': 2})
        d.set('a', 3)
        assert d.to_mongo() == {'a': 3, 'in_mongo_b': 2}
        assert d.to_mongo(update=True) == {'$set': {'a': 3}}

        d.from_mongo({'a': 1, 'in_mongo_b': 2})
        d.set('b', 3)
        assert d.to_mongo() == {'a': 1, 'in_mongo_b': 3}
        assert d.to_mongo(update=True) == {'$set': {'in_mongo_b': 3}}

        with pytest.raises(KeyError):
            d.set('in_mongo_b', 2)

        d.from_mongo({})
        d.set('c', None)
        assert d.to_mongo() == {'c': None}
        with pytest.raises(ma.ValidationError):
            d.set('c', '123456')
        with pytest.raises(ma.ValidationError):
            d.set('d', None)

    def test_del(self):

        class MySchema(BaseSchema):
            a = fields.IntField()
            b = fields.IntField(attribute='in_mongo_b')

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        d.from_mongo({'a': 1, 'in_mongo_b': 2})
        d.delete('b')
        assert d.to_mongo() == {'a': 1}
        assert d.to_mongo(update=True) == {'$unset': {'in_mongo_b': ''}}
        d.delete('a')
        assert d.to_mongo(update=True) == {'$unset': {'a': '', 'in_mongo_b': ''}}

        with pytest.raises(KeyError):
            d.delete('in_mongo_b')

    def test_route_naming(self):

        class MySchema(BaseSchema):
            in_front = fields.IntField(attribute='in_mongo')

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        with pytest.raises(ma.ValidationError):
            d.load({'in_mongo': 42})
        d.load({'in_front': 42})
        with pytest.raises(KeyError):
            d.get('in_mongo')
        assert d.get('in_front') == 42
        d.set('in_front', 24)
        assert d._data == {'in_mongo': 24}
        assert d.get('in_front') == 24
        assert d.dump() == {'in_front': 24}
        assert d.to_mongo() == {'in_mongo': 24}

    def test_from_mongo(self):

        class MySchema(BaseSchema):
            in_front = fields.IntField(attribute='in_mongo')

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        with pytest.raises(exceptions.UnknownFieldInDBError):
            d.from_mongo({'in_front': 42})
        d.from_mongo({'in_mongo': 42})
        assert d.get('in_front') == 42

    def test_equality(self):

        class MySchema(BaseSchema):
            a = fields.IntField()
            b = fields.IntField(attribute='in_mongo_b')

        MyDataProxy = data_proxy_factory('My', MySchema())
        d1 = MyDataProxy()
        d1.load({'a': 1, 'b': 2})
        assert d1 == {'a': 1, 'in_mongo_b': 2}

        d2 = MyDataProxy()
        d2.load({'a': 1, 'b': 2})
        assert d1 == d2

        assert d1 != None  # noqa: E711 (None comparison)
        assert d1 != ma.missing
        assert None != d1  # noqa: E711 (None comparison)
        assert ma.missing != d1

    def test_share_ressources(self):

        class MySchema(BaseSchema):
            a = fields.IntField()
            b = fields.IntField(attribute='in_mongo_b')

        MyDataProxy = data_proxy_factory('My', MySchema())
        d1 = MyDataProxy()
        d2 = MyDataProxy()
        for field in ('schema', '_fields', '_fields_from_mongo_key'):
            assert getattr(d1, field) is getattr(d2, field)
        d1.load({'a': 1})
        d2.load({'b': 2})
        assert d1 != d2

    def test_set_to_missing_fields(self):

        class MySchema(BaseSchema):
            a = fields.IntField()
            b = fields.IntField(attribute='in_mongo_b')

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy(data={'a': 1})
        assert d.get('b') is ma.missing
        assert d._data['in_mongo_b'] is ma.missing
        d.set('b', 2)
        assert d.get('b') == 2
        d.delete('b')
        # Can do it two time in a row without error
        d.delete('b')
        assert d._data['in_mongo_b'] is ma.missing

    def test_default(self):
        default_value = ObjectId('507f1f77bcf86cd799439011')
        default_callable = lambda: ObjectId('507f1f77bcf86cd799439012')  # noqa: E731

        class MySchema(BaseSchema):
            no_default = fields.ObjectIdField()
            with_default = fields.ObjectIdField(default=default_value)
            with_callable_default = fields.ObjectIdField(default=default_callable)

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy(data={})
        assert d._data['no_default'] is ma.missing
        assert d._data['with_default'] == default_value
        assert d._data['with_callable_default'] == default_callable()
        assert d.get('no_default') is ma.missing
        assert d.get('with_default') == default_value
        assert d.get('with_callable_default') == default_callable()
        assert d.to_mongo() == {
            'with_default': default_value,
            'with_callable_default': default_callable(),
        }
        assert d.dump() == {
            'with_default': str(default_value),
            'with_callable_default': str(default_callable()),
        }

        d.delete('with_default')
        assert d._data['with_default'] == default_value
        assert d.get('with_default') == default_value
        d.delete('with_callable_default')
        assert d._data['with_callable_default'] == default_callable()
        assert d.get('with_callable_default') == default_callable()

    def test_validate(self):

        class MySchema(BaseSchema):
            with_max = fields.IntField(validate=validate.Range(max=99))

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy(data={})
        with pytest.raises(ma.ValidationError) as exc:
            MyDataProxy(data={'with_max': 100})
        assert exc.value.args[0] == {'with_max': ['Must be less than or equal to 99.']}
        with pytest.raises(ma.ValidationError) as exc:
            d.set('with_max', 100)
        assert exc.value.args[0] == ['Must be less than or equal to 99.']

    def test_required_validate(self):

        @self.instance.register
        class MyEmbedded(EmbeddedDocument):
            required = fields.IntField(required=True)

        class MySchema(BaseSchema):
            # EmbeddedField need instance to retrieve implementation
            required = fields.IntField(required=True)
            embedded = fields.EmbeddedField(MyEmbedded, instance=self.instance)
            listed = fields.ListField(fields.EmbeddedField(MyEmbedded, instance=self.instance))
            dicted = fields.DictField(
                values=fields.EmbeddedField(MyEmbedded, instance=self.instance))

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()

        d.load({
            'required': 42,
            'embedded': {'required': 42},
            'listed': [{'required': 42}],
            'dicted': {'a': {'required': 42}}
        })
        d.required_validate()
        # Empty list/dict should not trigger required if embedded field has required fields
        d.load({'embedded': {'required': 42}, 'required': 42})
        d.required_validate()

        d.load({'embedded': {'required': 42}})
        with pytest.raises(ma.ValidationError) as exc:
            d.required_validate()
        assert exc.value.messages == {'required': ['Missing data for required field.']}

        # Missing embedded is valid even though some fields are required in the embedded document
        d.load({'required': 42})
        d.required_validate()
        # Required fields in the embedded document are only checked if the document is not missing
        d.load({'embedded': {}, 'required': 42})
        with pytest.raises(ma.ValidationError) as exc:
            d.required_validate()
        assert exc.value.messages == {
            'embedded': {'required': ['Missing data for required field.']}}

        d.load({'embedded': {'required': 42}, 'required': 42, 'listed': [{}], 'dicted': {'a': {}}})
        with pytest.raises(ma.ValidationError) as exc:
            d.required_validate()
        assert exc.value.messages == {
            'listed': {0: {'required': ['Missing data for required field.']}},
            'dicted': {'a': {'value': {'required': ['Missing data for required field.']}}},
        }

    def test_unkown_field_in_db(self):
        class MySchema(BaseSchema):
            field = fields.IntField(attribute='mongo_field')

        DataProxy = data_proxy_factory('My', MySchema())
        d = DataProxy()
        d.from_mongo({'mongo_field': 42})
        assert d._data == {'mongo_field': 42}
        with pytest.raises(exceptions.UnknownFieldInDBError):
            d.from_mongo({'mongo_field': 42, 'xxx': 'foo'})

    def test_iterators(self):
        class MySchema(BaseSchema):
            field_a = fields.IntField(attribute='mongo_field_a')
            field_b = fields.IntField(attribute='mongo_field_b')

        DataProxy = data_proxy_factory('My', MySchema())
        d = DataProxy()
        d.from_mongo({'mongo_field_a': 42, 'mongo_field_b': 24})

        assert set(d.keys()) == {'mongo_field_a', 'mongo_field_b'}
        assert set(d.values()) == {42, 24}
        assert set(d.items()) == {('field_a', 42), ('field_b', 24)}

        d.load({'field_a': 100, 'field_b': 200})
        assert set(d.keys()) == {'mongo_field_a', 'mongo_field_b'}
        assert set(d.values()) == {100, 200}
        assert set(d.items()) == {('field_a', 100), ('field_b', 200)}

    def test_order(self):
        """Test schema order of embedded doc preserved when serializing to mongo"""

        @self.instance.register
        class MyEmbedded(EmbeddedDocument):
            a = fields.IntField()
            b = fields.IntField()
            c = fields.IntField()

        class MySchema(BaseSchema):
            # EmbeddedField need instance to retrieve implementation
            e = fields.EmbeddedField(MyEmbedded, instance=self.instance)

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        d.load({'e': {'c': 3, 'b': 2, 'a': 1}})
        assert_equal_order(d.to_mongo()['e'], {'a': 1, 'b': 2, 'c': 3})
        d.get('e')['b'] = 4
        assert_equal_order(d.to_mongo()['e'], {'a': 1, 'b': 4, 'c': 3})


class TestNonStrictDataProxy(BaseTest):

    def test_build(self):

        class MySchema(BaseSchema):
            pass

        strict_proxy = data_proxy_factory('My', MySchema(), strict=True)
        assert issubclass(strict_proxy, BaseDataProxy)
        non_strict_proxy = data_proxy_factory('My', MySchema(), strict=False)
        assert issubclass(non_strict_proxy, BaseNonStrictDataProxy)

    def test_basic(self):
        class MySchema(BaseSchema):
            field_a = fields.IntField(attribute='mongo_field_a')

        NonStrictDataProxy = data_proxy_factory('My', MySchema(), strict=False)
        with pytest.raises(ma.ValidationError) as exc:
            NonStrictDataProxy({'field_a': 42, 'xxx': 'foo'})
        assert exc.value.messages == {'xxx': ['Unknown field.']}
        d = NonStrictDataProxy()
        d.from_mongo({'mongo_field_a': 42, 'xxx': 'foo'})
        assert d._data == {'mongo_field_a': 42}
        assert d._additional_data == {'xxx': 'foo'}
