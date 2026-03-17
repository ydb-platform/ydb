from copy import copy, deepcopy

import pytest

import marshmallow as ma

from umongo.data_proxy import data_proxy_factory
from umongo import Document, EmbeddedDocument, MixinDocument, fields, exceptions, ExposeMissing

from .common import BaseTest


class TestEmbeddedDocument(BaseTest):

    def test_embedded_inheritance(self):
        @self.instance.register
        class MyChildEmbeddedDocument(EmbeddedDocument):
            num = fields.IntField()

        @self.instance.register
        class MyParentEmbeddedDocument(EmbeddedDocument):
            embedded = fields.EmbeddedField(MyChildEmbeddedDocument)

        @self.instance.register
        class MyDoc(Document):
            embedded = fields.EmbeddedField(MyParentEmbeddedDocument)

        document = MyDoc(**{
            "embedded": {"embedded": {"num": 1}}
        })

        assert document is not None
        assert isinstance(document, MyDoc)
        assert isinstance(document.embedded, MyParentEmbeddedDocument)
        assert isinstance(document.embedded.embedded, MyChildEmbeddedDocument)

    def test_embedded_document(self):
        @self.instance.register
        class MyEmbeddedDocument(EmbeddedDocument):
            a = fields.IntField(attribute='in_mongo_a')
            b = fields.IntField()

        embedded = MyEmbeddedDocument()
        assert embedded.to_mongo(update=True) is None
        assert not embedded.is_modified()

        @self.instance.register
        class MyDoc(Document):
            embedded = fields.EmbeddedField(MyEmbeddedDocument,
                attribute='in_mongo_embedded', allow_none=True)

        MySchema = MyDoc.Schema

        # Make sure embedded document doesn't have implicit _id field
        assert '_id' not in MyEmbeddedDocument.Schema().fields
        assert 'id' not in MyEmbeddedDocument.Schema().fields

        MyDataProxy = data_proxy_factory('My', MySchema())
        d = MyDataProxy()
        d.from_mongo(data={'in_mongo_embedded': {'in_mongo_a': 1, 'b': 2}})
        assert d.dump() == {'embedded': {'a': 1, 'b': 2}}
        embedded = d.get('embedded')
        assert type(embedded) == MyEmbeddedDocument
        assert embedded.a == 1
        assert embedded.b == 2
        assert embedded.dump() == {'a': 1, 'b': 2}
        assert embedded.to_mongo() == {'in_mongo_a': 1, 'b': 2}
        assert d.to_mongo() == {'in_mongo_embedded': {'in_mongo_a': 1, 'b': 2}}

        d2 = MyDataProxy()
        d2.from_mongo(data={'in_mongo_embedded': {'in_mongo_a': 1, 'b': 2}})
        assert d == d2

        embedded.a = 3
        assert embedded.is_modified()
        assert embedded.to_mongo(update=True) == {'$set': {'in_mongo_a': 3}}
        assert d.to_mongo(update=True) == {'$set': {'in_mongo_embedded': {'in_mongo_a': 3, 'b': 2}}}
        embedded.clear_modified()
        assert embedded.to_mongo(update=True) is None
        assert d.to_mongo(update=True) is None

        del embedded.a
        assert embedded.to_mongo(update=True) == {'$unset': {'in_mongo_a': ''}}
        assert d.to_mongo(update=True) == {'$set': {'in_mongo_embedded': {'b': 2}}}

        d.set('embedded', MyEmbeddedDocument(a=4))
        assert d.get('embedded').to_mongo(update=True) == {'$set': {'in_mongo_a': 4}}
        d.get('embedded').clear_modified()
        assert d.get('embedded').to_mongo(update=True) is None
        assert d.to_mongo(update=True) == {'$set': {'in_mongo_embedded': {'in_mongo_a': 4}}}

        embedded_doc = MyEmbeddedDocument(a=1, b=2)
        assert embedded_doc.a == 1
        assert embedded_doc.b == 2
        assert embedded_doc == {'in_mongo_a': 1, 'b': 2}
        assert embedded_doc == MyEmbeddedDocument(a=1, b=2)
        assert embedded_doc['a'] == 1
        assert embedded_doc['b'] == 2

        embedded_doc.clear_modified()
        embedded_doc.update({'b': 42})
        assert embedded_doc.is_modified()
        assert embedded_doc.a == 1
        assert embedded_doc.b == 42

        with pytest.raises(ma.ValidationError):
            MyEmbeddedDocument(in_mongo_a=1, b=2)

        embedded_doc['a'] = 1
        assert embedded_doc.a == embedded_doc['a'] == 1
        del embedded_doc['a']
        assert embedded_doc.a is embedded_doc['a'] is None

        # Test repr readability
        repr_d = repr(MyEmbeddedDocument(a=1, b=2))
        assert 'tests.test_embedded_document.MyEmbeddedDocument' in repr_d
        assert "'in_mongo_a'" not in repr_d
        assert "'a': 1" in repr_d
        assert "'b': 2" in repr_d

        # Test allow_none
        d3 = MyDataProxy({'embedded': None})
        assert d3.to_mongo() == {'in_mongo_embedded': None}
        d3.from_mongo({'in_mongo_embedded': None})
        assert d3.get('embedded') is None

    def test_fields_by_attr(self):
        @self.instance.register
        class EmbeddedStudent(EmbeddedDocument):
            name = fields.StrField(required=True)
            birthday = fields.DateTimeField()
            gpa = fields.FloatField()

        john = EmbeddedStudent.build_from_mongo(data={'name': 'John Doe'})
        assert john.name == 'John Doe'
        john.name = 'William Doe'
        assert john.name == 'William Doe'
        del john.name
        assert john.name is None
        with pytest.raises(AttributeError):
            john.missing
        with pytest.raises(AttributeError):
            del john.missing
        with pytest.raises(AttributeError):
            del john.dump
        john.dummy = 42
        assert john.dummy == 42
        del john.dummy
        with pytest.raises(AttributeError):
            john.dummy

    def test_fields_by_items(self):
        @self.instance.register
        class EmbeddedStudent(EmbeddedDocument):
            name = fields.StrField(required=True)
            birthday = fields.DateTimeField()
            gpa = fields.FloatField()

        john = EmbeddedStudent.build_from_mongo(data={'name': 'John Doe'})
        assert john['name'] == 'John Doe'
        john['name'] = 'William Doe'
        assert john['name'] == 'William Doe'
        del john['name']
        assert john['name'] is None
        with pytest.raises(KeyError):
            john['missing']
        with pytest.raises(KeyError):
            john['missing'] = None
        with pytest.raises(KeyError):
            del john['missing']

    def test_bad_embedded_document(self):

        @self.instance.register
        class MyEmbeddedDocument(EmbeddedDocument):
            a = fields.IntField()

        @self.instance.register
        class MyDoc(Document):
            e = fields.EmbeddedField(MyEmbeddedDocument)
            li = fields.ListField(fields.EmbeddedField(MyEmbeddedDocument))
            b = fields.IntField(required=True)

        with pytest.raises(ma.ValidationError) as exc:
            MyDoc(li={})
        assert exc.value.args[0] == {'li': ['Not a valid list.']}

        with pytest.raises(ma.ValidationError) as exc:
            MyDoc(li=True)
        assert exc.value.args[0] == {'li': ['Not a valid list.']}

        with pytest.raises(ma.ValidationError) as exc:
            MyDoc(li="string is not a list")
        assert exc.value.args[0] == {'li': ['Not a valid list.']}

        with pytest.raises(ma.ValidationError) as exc:
            MyDoc(li=[42])
        assert exc.value.args[0] == {'li': {0: {'_schema': ['Invalid input type.']}}}

        with pytest.raises(ma.ValidationError) as exc:
            MyDoc(li=[{}, 42])
        assert exc.value.args[0] == {'li': {1: {'_schema': ['Invalid input type.']}}}

        with pytest.raises(ma.ValidationError) as exc:
            MyDoc(b=[{}])
        assert exc.value.args[0] == {'b': ['Not a valid integer.']}

        with pytest.raises(ma.ValidationError) as exc:
            MyDoc(e=[{}])
        assert exc.value.args[0] == {'e': {'_schema': ['Invalid input type.']}}

    def test_inheritance(self):
        @self.instance.register
        class EmbeddedParent(EmbeddedDocument):
            a = fields.IntField(attribute='in_mongo_a_parent')
            b = fields.IntField()

        @self.instance.register
        class EmbeddedChild(EmbeddedParent):
            a = fields.IntField(attribute='in_mongo_a_child')
            c = fields.IntField()

        @self.instance.register
        class GrandChild(EmbeddedChild):
            d = fields.IntField()

        @self.instance.register
        class OtherEmbedded(EmbeddedDocument):
            pass

        @self.instance.register
        class MyDoc(Document):
            parent = fields.EmbeddedField(EmbeddedParent)
            child = fields.EmbeddedField(EmbeddedChild)

        assert EmbeddedParent.opts.offspring == {EmbeddedChild, GrandChild}
        assert EmbeddedChild.opts.offspring == {GrandChild}
        assert GrandChild.opts.offspring == set()
        assert OtherEmbedded.opts.offspring == set()

        parent = EmbeddedParent(a=1)
        child = EmbeddedChild(a=1, b=2, c=3)
        grandchild = GrandChild(d=4)

        assert parent.to_mongo() == {'in_mongo_a_parent': 1}
        assert child.to_mongo() == {'in_mongo_a_child': 1, 'b': 2, 'c': 3, '_cls': 'EmbeddedChild'}
        assert grandchild.to_mongo() == {'d': 4, '_cls': 'GrandChild'}

        with pytest.raises(ma.ValidationError) as exc:
            MyDoc(parent=OtherEmbedded())
        assert exc.value.args[0] == {'parent': {'_schema': ['Invalid input type.']}}
        with pytest.raises(ma.ValidationError):
            MyDoc(child=parent)
        doc = MyDoc(parent=child, child=child)
        assert doc.child == doc.parent

        doc = MyDoc(child={'a': 1, 'cls': 'GrandChild'},
                    parent={'cls': 'EmbeddedChild', 'a': 1})
        assert doc.child.to_mongo() == {'in_mongo_a_child': 1, '_cls': 'GrandChild'}
        assert doc.parent.to_mongo() == {'in_mongo_a_child': 1, '_cls': 'EmbeddedChild'}

        with pytest.raises(ma.ValidationError) as exc:
            MyDoc(child={'a': 1, '_cls': 'GrandChild'})
        assert exc.value.messages == {'child': {'_cls': ['Unknown field.']}}

        # Try to build a non-child document
        with pytest.raises(ma.ValidationError) as exc:
            MyDoc(child={'cls': 'OtherEmbedded'})
        assert exc.value.messages == {'child': ['Unknown document `OtherEmbedded`.']}

        # Test embedded child deserialization from mongo
        child = EmbeddedChild(c=69)
        doc = MyDoc(parent=child)
        mongo_data = doc.to_mongo()
        doc2 = MyDoc.build_from_mongo(mongo_data)
        assert isinstance(doc2.parent, EmbeddedChild)
        assert doc._data == doc2._data

        # Test grandchild can be passed as parent
        doc = MyDoc(parent={'cls': 'GrandChild', 'd': 2})
        assert doc.parent.to_mongo() == {'d': 2, '_cls': 'GrandChild'}

    def test_abstract_inheritance(self):
        class AlienClass:
            pass

        @self.instance.register
        class AbstractParent(EmbeddedDocument):
            a = fields.IntField(attribute='in_mongo_a_parent')
            b = fields.IntField()

            class Meta:
                abstract = True

        @self.instance.register
        class AbstractChild(AbstractParent):
            a = fields.IntField(attribute='in_mongo_a_child')
            c = fields.IntField()

            class Meta:
                abstract = True

        @self.instance.register
        class ConcreteChild(AbstractParent, AlienClass):
            c = fields.IntField()

        @self.instance.register
        class ConcreteGrandChild(AbstractChild):
            d = fields.IntField()

        @self.instance.register
        class ConcreteConcreteGrandChild(ConcreteChild):
            d = fields.IntField()

        @self.instance.register
        class OtherEmbedded(EmbeddedDocument):
            pass

        with pytest.raises(exceptions.AbstractDocumentError) as exc:
            AbstractParent()
        assert exc.value.args[0] == "Cannot instantiate an abstract EmbeddedDocument"

        # test
        cc = ConcreteChild(a=1, b=2, c=3)
        cgc = ConcreteGrandChild(a=1, b=2, c=3, d=4)
        ccgc = ConcreteConcreteGrandChild(a=1, b=2, c=3, d=4)

        # Child of abstract doesn't need `cls` hint field in serialization
        assert cc.to_mongo() == {'in_mongo_a_parent': 1, 'b': 2, 'c': 3}
        assert cgc.to_mongo() == {'in_mongo_a_child': 1, 'b': 2, 'c': 3, 'd': 4}
        # But child of non abstract does
        assert ccgc.to_mongo() == {
            'in_mongo_a_parent': 1, 'b': 2, 'c': 3, 'd': 4, '_cls': 'ConcreteConcreteGrandChild'}

        # Cannot use abstract embedded document in EmbeddedField
        @self.instance.register
        class MyDoc(Document):
            impossible = fields.EmbeddedField(AbstractParent)

        with pytest.raises(exceptions.DocumentDefinitionError) as exc:
            MyDoc(impossible={"a": 12})
        assert exc.value.args[0] == "EmbeddedField doesn't accept abstract embedded document"

        @self.instance.register
        class MyOtherDoc(Document):
            impossible = fields.EmbeddedField(AbstractChild)

        with pytest.raises(exceptions.DocumentDefinitionError) as exc:
            MyOtherDoc(impossible={"a": 12})
        assert exc.value.args[0] == "EmbeddedField doesn't accept abstract embedded document"

    def test_bad_inheritance(self):
        @self.instance.register
        class NotAbstractParent(EmbeddedDocument):
            pass

        with pytest.raises(exceptions.DocumentDefinitionError) as exc:
            @self.instance.register
            class ImpossibleChild2(NotAbstractParent):
                class Meta:
                    abstract = True
        assert exc.value.args[0] == "Abstract document should have all its parents abstract"

    def test_property(self):
        @self.instance.register
        class MyEmbeddedDoc(EmbeddedDocument):
            _prop = fields.FloatField()

            @property
            def prop(self):
                return self._prop

            @prop.setter
            def prop(self, value):
                self._prop = value

            @prop.deleter
            def prop(self):
                del self._prop

        emb = MyEmbeddedDoc()
        emb.prop = 42
        assert emb.prop == 42
        del emb.prop
        assert emb.prop is None

    def test_equality(self):
        @self.instance.register
        class MyChildEmbeddedDocument(EmbeddedDocument):
            num_1 = fields.IntField()
            num_2 = fields.IntField()

        @self.instance.register
        class MyParentEmbeddedDocument(EmbeddedDocument):
            embedded = fields.EmbeddedField(MyChildEmbeddedDocument)

        emb_1 = MyParentEmbeddedDocument(embedded={'num_1': 1, 'num_2': 2})
        emb_2 = MyParentEmbeddedDocument(embedded={'num_1': 1, 'num_2': 2})
        emb_3 = MyParentEmbeddedDocument(embedded={})
        emb_4 = MyParentEmbeddedDocument()
        emb_5 = MyParentEmbeddedDocument(embedded={'num_2': 2})
        emb_5["embedded"]["num_1"] = 1

        assert emb_1 == emb_2
        assert emb_1 != emb_3
        assert emb_1 != emb_4
        assert emb_1 == emb_5
        assert emb_1 != None  # noqa: E711 (None comparison)
        assert emb_1 != ma.missing
        assert None != emb_1  # noqa: E711 (None comparison)
        assert ma.missing != emb_1

    def test_strict_embedded_document(self):
        @self.instance.register
        class StrictEmbeddedDoc(EmbeddedDocument):
            a = fields.IntField()

        @self.instance.register
        class NonStrictEmbeddedDoc(EmbeddedDocument):
            a = fields.IntField()

            class Meta:
                strict = False

        data_with_bonus = {'a': 42, 'b': 'foo'}
        with pytest.raises(exceptions.UnknownFieldInDBError):
            StrictEmbeddedDoc.build_from_mongo(data_with_bonus)

        non_strict_doc = NonStrictEmbeddedDoc.build_from_mongo(data_with_bonus)
        assert non_strict_doc.to_mongo() == data_with_bonus
        non_strict_doc.dump() == {'a': 42}

        with pytest.raises(ma.ValidationError) as exc:
            NonStrictEmbeddedDoc(a=42, b='foo')
        assert exc.value.messages == {'b': ['Unknown field.']}

    def test_deepcopy(self):

        @self.instance.register
        class Child(EmbeddedDocument):
            name = fields.StrField()

        @self.instance.register
        class Parent(EmbeddedDocument):
            name = fields.StrField()
            child = fields.EmbeddedField(Child)

        john = Parent(name='John Doe', child={'name': 'John Doe Jr.'})
        jane = copy(john)
        assert jane.name == john.name
        assert jane.child is john.child
        jane = deepcopy(john)
        assert jane.name == john.name
        assert jane.child == john.child
        assert jane.child is not john.child

    def test_expose_missing(self):
        @self.instance.register
        class Child(EmbeddedDocument):
            name = fields.StrField()
            age = fields.IntField()

        @self.instance.register
        class Parent(EmbeddedDocument):
            name = fields.StrField()
            child = fields.EmbeddedField(Child)

        parent = Parent(**{'child': {'age': 42}})
        assert parent.name is None
        assert parent.child.name is None
        assert parent.child.age == 42
        with ExposeMissing():
            assert parent.name is ma.missing
            assert parent.child.name is ma.missing
            assert parent.child.age == 42

    def test_mixin(self):

        @self.instance.register
        class PMixin(MixinDocument):
            pm = fields.IntField()

        @self.instance.register
        class CMixin(MixinDocument):
            cm = fields.IntField()

        @self.instance.register
        class Parent(EmbeddedDocument, PMixin):
            p = fields.StrField()

            class Meta:
                allow_inheritance = True

        @self.instance.register
        class Child(Parent, CMixin):
            c = fields.StrField()

        assert set(Parent.schema.fields.keys()) == {'p', 'pm'}
        assert set(Child.schema.fields.keys()) == {'cls', 'p', 'pm', 'c', 'cm'}

        parent_data = {'p': 'parent', 'pm': 42}
        child_data = {'c': 'child', 'cm': 12, **parent_data}

        assert Parent(**parent_data).dump() == parent_data
        assert Child(**child_data).dump() == {**child_data, 'cls': 'Child'}

        parent = Parent()
        parent.p = 'parent'
        parent.pm = 42
        assert parent.p == 'parent'
        assert parent.pm == 42
        assert parent.dump() == parent_data
        del parent.p
        del parent.pm
        assert parent.p is None
        assert parent.pm is None

        parent = Parent()
        parent['p'] = 'parent'
        parent['pm'] = 42
        assert parent['p'] == 'parent'
        assert parent['pm'] == 42
        assert parent.dump() == parent_data
        del parent['p']
        del parent['pm']
        assert parent['p'] is None
        assert parent['pm'] is None

        child = Child()
        child.c = 'child'
        child.cm = 12
        child.p = 'parent'
        child.pm = 42
        assert child.c == 'child'
        assert child.cm == 12
        assert child.dump() == {'cls': 'Child', **child_data}
        del child.c
        del child.cm
        assert child.c is None
        assert child.cm is None

        child = Child()
        child['c'] = 'child'
        child['cm'] = 12
        child['p'] = 'parent'
        child['pm'] = 42
        assert child['c'] == 'child'
        assert child['cm'] == 12
        assert child.dump() == {'cls': 'Child', **child_data}
        del child['c']
        del child['cm']
        assert child['c'] is None
        assert child['cm'] is None

    def test_mixin_override(self):

        @self.instance.register
        class PMixin(MixinDocument):
            pm = fields.IntField()

        @self.instance.register
        class CMixin(MixinDocument):
            cm = fields.IntField()

        @self.instance.register
        class Parent(EmbeddedDocument, PMixin):
            p = fields.StrField()
            pm = fields.IntField(validate=ma.validate.Range(0, 5))

            class Meta:
                allow_inheritance = True

        @self.instance.register
        class Child(Parent, CMixin):
            c = fields.StrField()
            cm = fields.IntField(validate=ma.validate.Range(0, 5))

        assert set(Parent.schema.fields.keys()) == {'p', 'pm'}
        assert set(Child.schema.fields.keys()) == {'cls', 'p', 'pm', 'c', 'cm'}

        parent_data = {'p': 'parent', 'pm': 42}
        child_data = {'c': 'Child', 'cm': 12, **parent_data}

        with pytest.raises(ma.ValidationError) as exc:
            Parent(**parent_data)
        assert set(exc.value.messages.keys()) == {'pm'}
        with pytest.raises(ma.ValidationError) as exc:
            Child(**child_data)
        assert set(exc.value.messages.keys()) == {'pm', 'cm'}
