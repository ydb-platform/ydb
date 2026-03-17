from __future__ import absolute_import
from __future__ import unicode_literals
import six
from copy import deepcopy
import unittest as unittest2
from jsonobject import *
from jsonobject.exceptions import (
    BadValueError,
    DeleteNotAllowed,
    WrappingAttributeError,
)
from six.moves import map


class Features(JsonObject):
    """
    Make sure doc string isn't treated as a property called __doc__!

    """

    hair = StringProperty(choices=['brown', ('blond', 'Blond'), 'grey'])
    eyes = StringProperty()


class FeatureMap(JsonObject):
    feature_map = DictProperty(Features)


class Document(JsonObject):

    @StringProperty()
    def doc_type(self):
        return self.__class__.__name__


class Person(Document):

    first_name = StringProperty(required=True)
    last_name = StringProperty()
    features = ObjectProperty(Features)
    favorite_numbers = ListProperty(int)
    tags = ListProperty(six.text_type)

    @property
    def full_name(self):
        return '{self.first_name} {self.last_name}'.format(self=self)


class FamilyMember(Person):
    base_doc = 'Person'
    brothers = ListProperty(lambda: FamilyMember)


class JunkCD(JsonObject):
    c_property = IntegerProperty(name='c')

    @StringProperty(name='d')
    def d_property(self):
        return None


class JunkAB(JsonObject):
    a_property = ListProperty(int, name='a')
    b_property = ObjectProperty(JunkCD, name='b')


class ObjectWithDictProperty(JsonObject):
    mapping = DictProperty()


class JsonObjectTestCase(unittest2.TestCase):
    def _danny_data(self):
        return {
            'first_name': 'Danny',
            'last_name': 'Roberts',
            'brothers': [{
                'first_name': 'Alex',
                'last_name': 'Roberts',
            }, {
                'first_name': 'Nicky',
                'last_name': 'Roberts',
            }],
            'features': {'hair': 'brown', 'eyes': 'brown'},
            'favorite_numbers': [1, 1, 2, 3, 5, 8],
            'tags': ['happy', 'know it'],
        }

    def test_wrap(self):
        data = self._danny_data()
        danny = FamilyMember.wrap(data)
        self.assertEqual(danny.doc_type, 'FamilyMember')
        self.assertIsInstance(danny.doc_type, six.text_type)
        self.assertEqual(danny.first_name, 'Danny')
        self.assertEqual(danny.last_name, 'Roberts')
        self.assertEqual(danny.brothers[0].full_name, 'Alex Roberts')
        self.assertEqual(danny.brothers[1].full_name, 'Nicky Roberts')
        self.assertEqual(danny.features.hair, 'brown')
        self.assertEqual(danny.features.eyes, 'brown')
        self.assertEqual(danny.favorite_numbers, [1, 1, 2, 3, 5, 8])
        self.assertEqual(danny.tags, ['happy', 'know it'])

        danny.brothers[1].first_name = 'Nick'
        self.assertEqual(danny.brothers[1].full_name, 'Nick Roberts')

        brothers_json = [{
            'first_name': 'Alex',
            'last_name': 'Roberts',
        }, {
            'first_name': 'Nicky',
            'last_name': 'Roberts',
        }]
        with self.assertRaises(AssertionError):
            danny.brothers = brothers_json

        brothers = list(map(FamilyMember.wrap, brothers_json))
        danny.brothers = brothers

        self.assertEqual(danny.brothers, brothers)
        self.assertTrue(isinstance(danny.brothers, JsonArray))
        self.assertEqual(danny.to_json(), data)

        new_brothers = list(map(FamilyMember.wrap, brothers_json))
        danny.brothers[2:3] = new_brothers
        self.assertEqual(len(danny.brothers), 4)

        danny.features.hair = 'blond'
        self.assertEqual(danny.features.hair, 'blond')
        with self.assertRaises(BadValueError):
            danny.features.hair = 'green'

        features = {'hair': 'grey', 'eyes': 'blue'}
        with self.assertRaises(AssertionError):
            danny.features = features

        features = Features.wrap(features)
        danny.features = features
        self.assertEqual(dict(danny.features), {'hair': 'grey', 'eyes': 'blue'})

        numbers = [1, 2, 3, 4, 5]
        danny.favorite_numbers = numbers
        self.assertEqual(danny.favorite_numbers, numbers)
        self.assertEqual(danny.to_json()['favorite_numbers'], numbers)

    def test_bad_wrap(self):
        for error_type in (AttributeError, WrappingAttributeError):
            with self.assertRaises(error_type) as cm:
                Person.wrap({'full_name': 'Danny Roberts'})
            if six.PY2:
                self.assertEqual(
                    six.text_type(cm.exception),
                    "can't set attribute corresponding to "
                    "u'full_name' on a <class '__tests__.tests.Person'> "
                    "while wrapping {u'full_name': u'Danny Roberts'}"
                )
            else:
                self.assertEqual(
                    six.text_type(cm.exception),
                    "can't set attribute corresponding to "
                    "'full_name' on a <class '__tests__.tests.Person'> "
                    "while wrapping {'full_name': 'Danny Roberts'}"
                )

    def test_pickle(self):
        import pickle
        f1 = FamilyMember.wrap(self._danny_data())
        f2 = FamilyMember.wrap(self._danny_data())
        self.assertEqual(f2.to_json(), pickle.loads(pickle.dumps(f1)).to_json())

    def test_default(self):
        p = FamilyMember(first_name='PJ')
        self.assertEqual(p.to_json(), {
            'doc_type': 'FamilyMember',
            'base_doc': 'Person',
            'first_name': 'PJ',
            'last_name': None,
            'brothers': [],
            'features': {'hair': None, 'eyes': None},
            'favorite_numbers': [],
            'tags': [],
        })

    def test_float(self):
        class Foo(JsonObject):
            f = FloatProperty()

        foo = Foo.wrap({'f': 1.0})
        self.assertEqual(foo.f, 1.0)
        foo.f = 3
        self.assertEqual(foo.f, 3.0)
        self.assertIsInstance(foo.f, float)

    def test_name(self):
        class Wack(JsonObject):
            underscore_obj = StringProperty(name='_obj')
        w = Wack()
        self.assertEqual(w.to_json(), {'_obj': None})
        w.underscore_obj = 'new_value'
        self.assertEqual(w.underscore_obj, 'new_value')
        self.assertEqual(w.to_json(), {'_obj': 'new_value'})

    def test_mapping(self):

        json_end = {
            'a': [1, 2, 3],
            'b': {
                'c': 1,
                'd': 'string',
            }
        }

        p = JunkAB(deepcopy(json_end))
        self.assertEqual(p.to_json(), json_end)
        p.a_property.append(4)
        self.assertEqual(p.to_json(), {'a': [1, 2, 3, 4], 'b': json_end['b']})
        p.a_property = []
        self.assertEqual(p.to_json(), {'a': [], 'b': json_end['b']})
        p.a_property = None
        self.assertEqual(p.to_json(), {'a': None, 'b': json_end['b']})
        p['a'] = [1, 2, 3]
        self.assertEqual(p.to_json(), json_end)
        self.assertEqual(list(p.keys()), list(p.to_json().keys()))

    def test_competing_names(self):
        with self.assertRaises(AssertionError):
            class Bad(JsonObject):
                a = IntegerProperty(name='ay')
                eh = StringProperty(name='ay')

    def test_init(self):
        from jsonobject.base import get_dynamic_properties
        self.assertEqual(JunkCD(c_property=1, d_property='yyy').to_json(),
                         JunkCD({'c': 1, 'd': 'yyy'}).to_json())
        x = JunkCD(non_existent_property=2)
        self.assertEqual(get_dynamic_properties(x),
                         {'non_existent_property': 2})


        ab = JunkAB(a_property=[1, 2, 3],
                    b_property=JunkCD({'c': 1, 'd': 'string'}))
        self.assertEqual(ab.to_json(), {
            'a': [1, 2, 3],
            'b': {
                'c': 1,
                'd': 'string',
            }
        })

    def test_choices(self):

        with self.assertRaises(BadValueError):
            Features(hair='blue')
        with self.assertRaises(BadValueError):
            Features.wrap({'hair': 'blue'})
        with self.assertRaises(BadValueError):
            f = Features()
            f.hair = 'blue'

    def test_required(self):
        with self.assertRaises(BadValueError):
            Person()
        Person(first_name='')
        Person(first_name='James')

    def test_dynamic_properties(self):
        p = Features.wrap({'platypus': 'James'})
        p.marmot = 'Sally'
        p._nope = 10
        self.assertEqual(p.to_json(), {
            'platypus': 'James',
            'marmot': 'Sally',
            'eyes': None,
            'hair': None,
        })
        self.assertEqual(p.platypus, 'James')
        self.assertEqual(p.marmot, 'Sally')
        self.assertEqual(p._nope, 10)

    def test_delete_dynamic(self):
        def assertReallyThere():
            self.assertEqual(p.a, 1)
            self.assertEqual(p['a'], 1)
            self.assertEqual(p.to_json()['a'], 1)

        def assertReallyDeleted():
            with self.assertRaises(AttributeError):
                p.a
            with self.assertRaises(KeyError):
                p['a']
            with self.assertRaises(KeyError):
                p.to_json()['a']

        # delete attribute
        p = Features.wrap({'a': 1})
        assertReallyThere()
        del p.a
        assertReallyDeleted()

        # delete dict item
        p = Features.wrap({'a': 1})
        assertReallyThere()
        del p['a']
        assertReallyDeleted()

        with self.assertRaises(DeleteNotAllowed):
            del p.hair

        with self.assertRaises(DeleteNotAllowed):
            del p['hair']

    def test_dict_clear(self):
        class Foo(JsonObject):
            dct = DictProperty()
        dct = {'mydict': 'yay'}
        foo = Foo(dct=dct)
        json_dict = foo.dct
        self.assertEqual(json_dict, dct)
        json_dict.clear()
        self.assertEqual(json_dict, {})
        self.assertEqual(json_dict._obj, {})

    def test_dynamic_container(self):
        class Foo(JsonObject):
            pass
        foo = Foo(my_list=[])
        self.assertIs(foo.my_list._obj, foo._obj['my_list'])
        foo = Foo(my_dict={})
        self.assertIs(foo.my_dict._obj, foo._obj['my_dict'])
        foo = Foo(my_set=set())
        self.assertIs(foo.my_set._obj, foo._obj['my_set'])

    def test_dynamic_dict_property(self):
        "dates copied from couchdbkit"
        import datetime

        class Foo(JsonObject):
            my_datetime = DateTimeProperty()
            my_dict = DictProperty()
        foo = Foo()
        full_datetime = datetime.datetime(2009, 5, 10, 21, 19, 21, 127380)
        normalized_datetime = datetime.datetime(2009, 5, 10, 21, 19, 21)

        foo.my_datetime = full_datetime
        self.assertEqual(foo.my_datetime, normalized_datetime)

        foo.my_dict['test'] = {
            'a': full_datetime
        }
        self.assertEqual(foo.my_dict, {
            'test': {
                'a': normalized_datetime
            }
        })

    def test_access_to_descriptor(self):
        p = StringProperty()
        class Foo(JsonObject):
            string = p

        self.assertIs(Foo.string, p)

    def test_recursive_validation(self):
        class Baz(JsonObject):
            string = StringProperty()

        class Bar(JsonObject):
            baz = ObjectProperty(Baz)

        class Foo(JsonObject):
            bar = ObjectProperty(Bar)

        with self.assertRaises(BadValueError):
            Foo.wrap({'bar': {'baz': {'string': 1}}})
        with self.assertRaises(BadValueError):
            Foo.wrap({'bar': {'baz': []}})
        with self.assertRaises(BadValueError):
            Foo.wrap({'bar': {'baz': 1}})
        with self.assertRaises(BadValueError):
            Foo.wrap({'bar': []})
        Foo.wrap({'bar': {'baz': {'string': ''}}})

    def test_long(self):
        class Dummy(JsonObject):
            i = IntegerProperty()
            l = ListProperty(int)
            l2 = ListProperty(IntegerProperty)
        d = Dummy()
        longint = 2 ** 63
        self.assertIsInstance(longint, six.integer_types)
        d.i = longint
        self.assertEqual(d.i, longint)
        d.l = [longint]
        self.assertEqual(d.l, [longint])
        d.l2 = [longint]
        self.assertEqual(d.l2, [longint])

    def test_string_list_property(self):

        class Foo(JsonObject):
            string_list = ListProperty(StringProperty)

        foo = Foo({'string_list': ['a', 'b', 'c']})
        self.assertEqual(foo.string_list, ['a', 'b' , 'c'])

    def test_typed_dict_of_dict(self):

        class City(JsonObject):
            _allow_dynamic_properties = False
            name = StringProperty()

        class Foo(JsonObject):
            _allow_dynamic_properties = False
            cities_by_state_by_country = DictProperty(DictProperty(City))

        # testing an internal assumption; can remove if internals change
        self.assertEqual(Foo.cities_by_state_by_country.item_wrapper.item_wrapper.item_type, City)

        city = City.wrap({'name': 'Boston'})
        with self.assertRaises(AttributeError):
            city.off_spec = 'bar'

        foo = Foo.wrap({'cities_by_state_by_country': {'USA': {'MA': {'name': 'Boston'}}}})
        self.assertIsInstance(foo.cities_by_state_by_country['USA']['MA'], City)
        with self.assertRaises(AttributeError):
            foo.cities_by_state_by_country['USA']['MA'].off_spec = 'bar'

    def test_object_property_with_lambda(self):
        class Bar(JsonObject):
            string = StringProperty()

        class Foo(JsonObject):
            bar = ObjectProperty(lambda: Bar)

        foo = Foo()
        self.assertIsInstance(foo.bar, Bar)


class PropertyInsideContainerTest(unittest2.TestCase):

    def test_default_is_required(self):
        class Foo(JsonObject):
            container = ListProperty(int)

        with self.assertRaises(BadValueError):
            Foo(container=[None])

    def test_property_class_required(self):
        class Foo(JsonObject):
            container = ListProperty(IntegerProperty)

        with self.assertRaises(BadValueError):
            Foo(container=[None])

    def test_property(self):
        class Foo(JsonObject):
            container = ListProperty(IntegerProperty())

        # assert does not error
        Foo(container=[None])

    def test_required_property(self):

        class Foo(JsonObject):
            container = ListProperty(IntegerProperty(required=True))

        with self.assertRaises(BadValueError):
            Foo(container=[None])


class LazyValidationTest(unittest2.TestCase):

    def _validate_raises(self, foo):
        with self.assertRaises(BadValueError):
            foo.validate()

        with self.assertRaises(BadValueError):
            foo.to_json()

    def _validate_not_raises(self, foo):
        foo.validate()
        foo.to_json()

    def test_string(self):
        class Foo(JsonObject):
            _validate_required_lazily = True
            string = StringProperty(required=True)

        foo = Foo()
        self._validate_raises(foo)
        foo.string = 'hi'
        self._validate_not_raises(foo)

    def test_object(self):
        class Bar(JsonObject):
            _validate_required_lazily = True
            string = StringProperty(required=True)

        class Foo(JsonObject):
            _validate_required_lazily = True
            bar = ObjectProperty(Bar)

        foo = Foo()
        self._validate_raises(foo)
        foo.bar.string = 'hi'
        self._validate_not_raises(foo)

    def test_list(self):
        class Bar(JsonObject):
            _validate_required_lazily = True
            string = StringProperty(required=True)

        class Foo(JsonObject):
            _validate_required_lazily = True
            bars = ListProperty(Bar, required=True)

        foo = Foo()
        self._validate_raises(foo)
        foo.bars.append(Bar())
        self._validate_raises(foo)
        foo.bars[0].string = 'hi'
        self._validate_not_raises(foo)

    def test_list_update_by_index(self):
        class Foo(JsonObject):
            bar = ListProperty()

        foo = Foo()
        foo.bar.append(1)
        self.assertEqual(foo.bar, foo.to_json()['bar'])
        foo.bar[0] = 2
        self.assertEqual(foo.bar, foo.to_json()['bar'])
        self.assertEqual(2, foo.to_json()['bar'][0])

    def test_schema_list_update_by_index(self):
        class Bar(JsonObject):
            string = StringProperty()

        class Foo(JsonObject):
            bar = ListProperty(Bar)

        foo = Foo()
        foo.bar.append(Bar(string='hi'))
        self.assertEqual(foo.bar[0].string, foo.to_json()['bar'][0]['string'])
        self.assertIsInstance(foo.bar[0], Bar)
        self.assertIsInstance(foo.to_json()['bar'][0], dict)
        foo.bar[0] = Bar(string='lo')
        self.assertEqual(foo.bar[0].string, foo.to_json()['bar'][0]['string'])
        self.assertEqual('lo', foo.to_json()['bar'][0]['string'])
        self.assertIsInstance(foo.bar[0], Bar)
        self.assertIsInstance(foo.to_json()['bar'][0], dict)

    def test_list_plus_equals(self):
        class Foo(JsonObject):
            bar = ListProperty()

        foo = Foo()
        foo.bar = [1, 2, 3]
        self.assertEqual(foo.bar, [1, 2, 3])
        self.assertEqual(foo.to_json()['bar'], [1, 2, 3])
        foo.bar += [4]
        self.assertEqual(foo.bar, [1, 2, 3, 4])
        self.assertEqual(foo.to_json()['bar'], [1, 2, 3, 4])

    def test_dict(self):
        class Bar(JsonObject):
            _validate_required_lazily = True
            string = StringProperty(required=True)

        class Foo(JsonObject):
            _validate_required_lazily = True
            bar_map = DictProperty(Bar, required=True)

        foo = Foo()
        self._validate_raises(foo)
        foo.bar_map['hi'] = Bar()
        self._validate_raises(foo)
        foo.bar_map['hi'].string = 'hi'
        self._validate_not_raises(foo)


class PropertyTestCase(unittest2.TestCase):
    def test_date(self):
        import datetime
        p = DateProperty()
        for string, date in [('1988-07-07', datetime.date(1988, 7, 7))]:
            self.assertEqual(p.wrap(string), date)
            self.assertEqual(p.unwrap(date), (date, string))
        with self.assertRaises(BadValueError):
            p.wrap('1234-05-90')
        with self.assertRaises(BadValueError):
            p.wrap('2000-01-01T00:00:00Z')

    def test_datetime(self):
        import datetime
        p = DateTimeProperty()
        for string, dt in [('2011-01-18T12:38:09Z', datetime.datetime(2011, 1, 18, 12, 38, 9))]:
            self.assertEqual(p.wrap(string), dt)
            self.assertEqual(p.unwrap(dt), (dt, string))
        with self.assertRaises(BadValueError):
            p.wrap('1234-05-90T00:00:00Z')
        with self.assertRaises(BadValueError):
            p.wrap('1988-07-07')

    def test_time(self):
        import datetime
        p = TimeProperty()
        for string, time in [('12:38:09', datetime.time(12, 38, 9))]:
            self.assertEqual(p.wrap(string), time)
            self.assertEqual(p.unwrap(time), (time, string))
        with self.assertRaises(BadValueError):
            p.wrap('25:00:00')
        with self.assertRaises(BadValueError):
            p.wrap('2011-01-18T12:38:09Z')
        with self.assertRaises(BadValueError):
            p.wrap('1988-07-07')

    def test_decimal(self):
        import decimal

        class Foo(JsonObject):
            decimal = DecimalProperty()

        foo = Foo(decimal=decimal.Decimal('2.0'))
        self.assertEqual(foo.decimal, decimal.Decimal('2.0'))
        self.assertEqual(foo.to_json()['decimal'], '2.0')

        foo.decimal = 3
        self.assertEqual(foo.decimal, decimal.Decimal(3))
        self.assertEqual(foo.to_json()['decimal'], '3')

        foo.decimal = 4
        self.assertEqual(foo.decimal, decimal.Decimal(4))
        self.assertEqual(foo.to_json()['decimal'], '4')

        foo.decimal = 5.25
        self.assertEqual(foo.decimal, decimal.Decimal('5.25'))
        self.assertEqual(foo.to_json()['decimal'], '5.25')

    def test_dict(self):
        mapping = {'one': 1, 'two': 2}
        o = ObjectWithDictProperty(mapping=mapping)
        self.assertEqual(o.mapping, mapping)
        self.assertEqual(o.to_json()['mapping'], mapping)

    def test_dict_update(self):
        mapping = {'one': 1, 'two': 2}
        o = ObjectWithDictProperty(mapping=mapping)
        o.mapping.update({'three': 3}, four=4)
        self.assertEqual(o.mapping, {'one': 1, 'two': 2, 'three': 3, 'four': 4})
        self.assertEqual(o.to_json()['mapping'], {'one': 1, 'two': 2, 'three': 3, 'four': 4})

    def test_dict_pop(self):
        mapping = {'one': 1, 'two': 2}
        o = ObjectWithDictProperty(mapping=mapping)
        val = o.mapping.pop('two')
        self.assertEqual(val, 2)
        self.assertEqual(o.mapping, {'one': 1})
        self.assertEqual(o.to_json()['mapping'], {'one': 1})

    def test_dict_setdefault(self):
        mapping = {'one': 1, 'two': 2}
        o = ObjectWithDictProperty(mapping=mapping)
        val = o.mapping.setdefault('three', 3)
        self.assertEqual(val, 3)
        self.assertEqual(o.mapping, {'one': 1, 'two': 2, 'three': 3})
        self.assertEqual(o.to_json()['mapping'], {'one': 1, 'two': 2, 'three': 3})

    def test_dict_popitem(self):
        mapping = {'one': 1, 'two': 2}
        o = ObjectWithDictProperty(mapping=mapping)
        old_dict = o.mapping.copy()
        val = o.mapping.popitem()
        self.assertTrue(val[0] in old_dict)
        self.assertTrue(val[1] == old_dict[val[0]])
        self.assertTrue(val[0] not in o.mapping)
        self.assertTrue(val[0] not in o.to_json()['mapping'])

    def test_typed_dict(self):
        features = FeatureMap({'feature_map': {'lala': {}, 'foo': None}})
        self.assertEqual(features.to_json(), {
            'feature_map': {
                'lala': {'hair': None, 'eyes': None},
                'foo': {'hair': None, 'eyes': None},
            },
        })
        with self.assertRaises(BadValueError):
            FeatureMap({'feature_map': {'lala': 10}})

        features.feature_map.update({'hoho': Features(eyes='brown')})
        self.assertEqual(features.to_json(), {
            'feature_map': {
                'lala': {'hair': None, 'eyes': None},
                'foo': {'hair': None, 'eyes': None},
                'hoho': {'hair': None, 'eyes': 'brown'},
            },
        })

    def test_allow_dynamic(self):
        class Foo(JsonObject):
            _allow_dynamic_properties = False

        foo = Foo()
        with self.assertRaises(AttributeError):
            foo.blah = 3
        foo._blah = 5
        self.assertEqual(dict(foo), {})
        self.assertEqual(foo.to_json(), {})
        self.assertEqual(foo._blah, 5)

    def test_exclude_if_none(self):
        class Foo(JsonObject):
            _id = StringProperty(exclude_if_none=True)
            name = StringProperty()

        foo = Foo()
        self.assertEqual(foo.to_json(), {'name': None})
        self.assertEqual(foo._id, None)
        foo = Foo(_id='xxx')
        self.assertEqual(dict(foo), {'name': None, '_id': 'xxx'})
        foo._id = None
        self.assertEqual(foo.to_json(), {'name': None})
        self.assertEqual(foo._id, None)

    def test_descriptor(self):
        class Desc(object):
            def __get__(self, instance, owner):
                if not instance:
                    return self
                return instance._string

            def __set__(self, instance, value):
                instance._string = value

        class Foo(JsonObject):
            _string = StringProperty()
            string = Desc()

        foo = Foo(_string='hello')
        self.assertEqual(foo._string, 'hello')
        self.assertEqual(foo.string, 'hello')
        foo.string = 'goodbye'
        self.assertEqual(foo.string, 'goodbye')
        self.assertEqual(foo._string, 'goodbye')
        self.assertEqual(foo.to_json(), {
            '_string': 'goodbye',
        })
    def test_key_error(self):
        class Foo(JsonObject):
            pass
        foo = Foo()

        with self.assertRaises(KeyError):
            foo['hello']

    def test_attribute_error(self):
        class Foo(JsonObject):
            pass
        foo = Foo()
        
        with self.assertRaises(AttributeError):
            foo.hello


class DynamicConversionTestCase(unittest2.TestCase):
    import datetime

    class Foo(JsonObject):
        pass
    string_date = '2012-01-01'
    date_date = datetime.date(2012, 1, 1)

    def _test_dynamic_conversion(self, foo):
        string_date = self.string_date
        date_date = self.date_date
        self.assertEqual(foo.to_json()['my_date'], string_date)
        self.assertEqual(foo.my_date, date_date)

        self.assertEqual(foo.to_json()['my_list'], [1, 2, [string_date]])
        self.assertEqual(foo.my_list, [1, 2, [date_date]])

        self.assertEqual(foo.to_json()['my_dict'], {'a': {'b': string_date}})
        self.assertEqual(foo.my_dict, {'a': {'b': date_date}})

    def test_wrapping(self):
        foo = self.Foo({
            'my_date': self.string_date,
            'my_list': [1, 2, [self.string_date]],
            'my_dict': {'a': {'b': self.string_date}},
        })
        self._test_dynamic_conversion(foo)

    def test_kwargs(self):

        foo = self.Foo(
            my_date=self.date_date,
            my_list=[1, 2, [self.date_date]],
            my_dict={'a': {'b': self.date_date}},
        )
        self._test_dynamic_conversion(foo)

    def test_assignment(self):
        foo = self.Foo()
        foo.my_date = self.date_date
        foo.my_list = [1, 2, [self.date_date]]
        foo.my_dict = {'a': {'b': self.date_date}}
        self._test_dynamic_conversion(foo)

    def test_manipulation(self):
        foo = self.Foo()
        foo.my_date = self.date_date
        foo.my_list = [1, 2, []]
        foo.my_list[2].append(self.date_date)
        foo.my_dict = {'a': {}}
        foo.my_dict['a']['b'] = self.date_date
        self._test_dynamic_conversion(foo)

    def test_properties(self):
        class Foo(JsonObject):
            string = StringProperty()
            date = DateProperty()
            dict = DictProperty()

        self.assertEqual(Foo.properties(), Foo().properties())
        self.assertEqual(Foo.properties(), {
            'string': Foo.string,
            'date': Foo.date,
            'dict': Foo.dict,
        })

    def test_change_type(self):
        class Foo(JsonObject):
            my_list = ('bar',)

        foo = Foo()
        foo.my_list = list(foo.my_list)
        self.assertEqual(foo.to_json(), {'my_list': ['bar']})


class User(JsonObject):
    username = StringProperty()
    name = StringProperty()
    active = BooleanProperty(default=False, required=True)
    date_joined = DateTimeProperty()
    tags = ListProperty(six.text_type)


class TestExactDateTime(unittest2.TestCase):
    def test_exact(self):
        class DateObj(JsonObject):
            date = DateTimeProperty(exact=True)
        import datetime
        date = datetime.datetime.utcnow()
        date_obj = DateObj(date=date)
        self.assertEqual(date_obj.date, date)
        self.assertEqual(date_obj.to_json()['date'], date.isoformat() + 'Z')
        self.assertEqual(len(date_obj.to_json()['date']), 27)

        date = date.replace(microsecond=0)
        date_obj = DateObj(date=date)
        self.assertEqual(date_obj.date, date)
        self.assertEqual(date_obj.to_json()['date'],
                         date.isoformat() + '.000000Z')
        self.assertEqual(len(date_obj.to_json()['date']), 27)


class IntegerTest(unittest2.TestCase):
    @classmethod
    def setUpClass(cls):
        class Foo(JsonObject):
            my_int = IntegerProperty(default=30)
        cls.Foo = Foo

    def test_default(self):
        self.assertEqual(self.Foo().my_int, 30)

    def test_init_zero(self):
        self.assertEqual(self.Foo(my_int=0).my_int, 0)
        self.assertEqual(self.Foo.wrap({'my_int': 0}).my_int, 0)

    def test_set_zero(self):
        foo = self.Foo()
        foo.my_int = 0
        self.assertEqual(foo.my_int, 0)
        self.assertEqual(foo.to_json()['my_int'], 0)


class TestReadmeExamples(unittest2.TestCase):
    def test(self):
        import datetime
        user1 = User(
            name='John Doe',
            username='jdoe',
            date_joined=datetime.datetime(2013, 8, 5, 2, 46, 58),
            tags=['generic', 'anonymous']
        )
        self.assertEqual(
            user1.to_json(), {
                'name': 'John Doe',
                'username': 'jdoe',
                'active': False,
                'date_joined': '2013-08-05T02:46:58Z',
                'tags': ['generic', 'anonymous']
            }
        )
