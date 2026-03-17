import pytest

from umongo import Document, fields, exceptions

from .common import BaseTest


class TestInheritance(BaseTest):

    def test_cls_field(self):

        @self.instance.register
        class Parent(Document):
            last_name = fields.StrField()

        @self.instance.register
        class Child(Parent):
            first_name = fields.StrField()

        assert 'cls' in Child.schema.fields
        Child.schema.fields['cls']
        assert not hasattr(Parent(), 'cls')
        assert Child().cls == 'Child'

        loaded = Parent.build_from_mongo(
            {'_cls': 'Child', 'first_name': 'John', 'last_name': 'Doe'}, use_cls=True)
        assert loaded.cls == 'Child'

    def test_simple(self):

        @self.instance.register
        class Parent(Document):
            last_name = fields.StrField()

            class Meta:
                collection_name = 'parent_col'

        assert Parent.opts.abstract is False
        assert Parent.opts.collection_name == 'parent_col'
        assert Parent.collection.name == 'parent_col'

        @self.instance.register
        class Child(Parent):
            first_name = fields.StrField()

        assert Child.opts.abstract is False
        assert Child.opts.collection_name == 'parent_col'
        assert Child.collection.name == 'parent_col'
        Child(first_name='John', last_name='Doe')

    def test_abstract(self):

        # Cannot define a collection_name for an abstract doc !
        with pytest.raises(exceptions.DocumentDefinitionError):
            @self.instance.register
            class BadAbstractDoc(Document):
                class Meta:
                    abstract = True
                    collection_name = 'my_col'

        @self.instance.register
        class AbstractDoc(Document):
            abs_field = fields.StrField(default='from abstract')

            class Meta:
                abstract = True

        assert AbstractDoc.opts.abstract is True
        # Cannot instanciate also an abstract document
        with pytest.raises(exceptions.AbstractDocumentError):
            AbstractDoc()

        @self.instance.register
        class StillAbstractDoc(AbstractDoc):
            class Meta:
                abstract = True

        assert StillAbstractDoc.opts.abstract is True

        @self.instance.register
        class ConcreteDoc(AbstractDoc):
            pass

        assert ConcreteDoc.opts.abstract is False
        assert ConcreteDoc().abs_field == 'from abstract'

    def test_non_document_inheritance(self):

        class NotDoc1:
            @staticmethod
            def my_func1():
                return 24

        class NotDoc2:
            @staticmethod
            def my_func2():
                return 42

        @self.instance.register
        class Doc(NotDoc1, Document, NotDoc2):
            a = fields.StrField()

        assert issubclass(Doc, NotDoc1)
        assert issubclass(Doc, NotDoc2)
        assert isinstance(Doc(), NotDoc1)
        assert isinstance(Doc(), NotDoc2)
        assert Doc.my_func1() == 24
        assert Doc.my_func2() == 42
        doc = Doc(a='test')
        assert doc.my_func1() == 24
        assert doc.my_func2() == 42
        assert doc.a == 'test'
