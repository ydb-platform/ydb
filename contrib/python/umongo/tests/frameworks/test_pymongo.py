import datetime as dt

from unittest import mock
import pytest

from bson import ObjectId
from pymongo import MongoClient
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult
from pymongo.collection import Collection
import marshmallow as ma

from umongo import (
    Document, EmbeddedDocument, MixinDocument, fields, exceptions, Reference
)
from umongo.document import MetaDocumentImplementation
from umongo.frameworks import pymongo as framework_pymongo  # noqa

from .common import strip_indexes, name_sorted
from ..common import BaseDBTest, TEST_DB


# All dependencies here are mandatory
dep_error = None


def make_db():
    return MongoClient()[TEST_DB]


@pytest.fixture
def db():
    return make_db()


class TestPymongo(BaseDBTest):

    def test_create(self, classroom_model):
        Student = classroom_model.Student
        john = Student(name='John Doe', birthday=dt.datetime(1995, 12, 12))
        ret = john.commit()
        assert isinstance(ret, InsertOneResult)
        assert john.to_mongo() == {
            '_id': john.id,
            'name': 'John Doe',
            'birthday': dt.datetime(1995, 12, 12)
        }
        john2 = Student.find_one(john.id)
        assert john2._data == john._data
        # Double commit should do nothing
        assert john.commit() is None

    def test_update(self, classroom_model):
        Student = classroom_model.Student
        john = Student(name='John Doe', birthday=dt.datetime(1995, 12, 12))
        john.commit()
        john.name = 'William Doe'
        assert john.to_mongo(update=True) == {'$set': {'name': 'William Doe'}}
        ret = john.commit()
        assert isinstance(ret, UpdateResult)
        assert john.to_mongo(update=True) is None
        john2 = Student.find_one(john.id)
        assert john2._data == john._data
        # Update without changing anything
        john.name = john.name
        john.commit()
        # Test conditional commit
        john.name = 'Zorro Doe'
        with pytest.raises(exceptions.UpdateError):
            john.commit(conditions={'name': 'Bad Name'})
        john.commit(conditions={'name': 'William Doe'})
        john.reload()
        assert john.name == 'Zorro Doe'
        # Cannot use conditions when creating document
        with pytest.raises(exceptions.NotCreatedError):
            Student(name='Joe').commit(conditions={'name': 'dummy'})

    def test_replace(self, classroom_model):
        Student = classroom_model.Student
        john = Student(name='John Doe', birthday=dt.datetime(1995, 12, 12))
        # replace has no impact on creation
        john.commit(replace=True)
        john.name = 'William Doe'
        john.clear_modified()
        ret = john.commit(replace=True)
        assert isinstance(ret, UpdateResult)
        john2 = Student.find_one(john.id)
        assert john2._data == john._data
        # Test conditional commit
        john.name = 'Zorro Doe'
        john.clear_modified()
        with pytest.raises(exceptions.UpdateError):
            john.commit(conditions={'name': 'Bad Name'}, replace=True)
        john.commit(conditions={'name': 'William Doe'}, replace=True)
        john.reload()
        assert john.name == 'Zorro Doe'
        # Cannot use conditions when creating document
        with pytest.raises(exceptions.NotCreatedError):
            Student(name='Joe').commit(conditions={'name': 'dummy'}, replace=True)

    def test_delete(self, classroom_model):
        Student = classroom_model.Student
        Student.collection.drop()
        john = Student(name='John Doe', birthday=dt.datetime(1995, 12, 12))
        with pytest.raises(exceptions.NotCreatedError):
            john.delete()
        john.commit()
        assert Student.count_documents() == 1
        ret = john.delete()
        assert isinstance(ret, DeleteResult)
        assert not john.is_created
        assert Student.count_documents() == 0
        with pytest.raises(exceptions.NotCreatedError):
            john.delete()
        # Can re-commit the document in database
        john.commit()
        assert john.is_created
        assert Student.count_documents() == 1
        # Test conditional delete
        with pytest.raises(exceptions.DeleteError):
            john.delete(conditions={'name': 'Bad Name'})
        john.delete(conditions={'name': 'John Doe'})
        # Finally try to delete a doc no longer in database
        john.commit()
        Student.find_one(john.id).delete()
        with pytest.raises(exceptions.DeleteError):
            john.delete()

    def test_reload(self, classroom_model):
        Student = classroom_model.Student
        Student(name='Other dude').commit()
        john = Student(name='John Doe', birthday=dt.datetime(1995, 12, 12))
        with pytest.raises(exceptions.NotCreatedError):
            john.reload()
        john.commit()
        john2 = Student.find_one(john.id)
        john2.name = 'William Doe'
        john2.commit()
        john.reload()
        assert john.name == 'William Doe'

    def test_cursor(self, classroom_model):
        Student = classroom_model.Student
        Student.collection.drop()
        for i in range(10):
            Student(name='student-%s' % i).commit()
        assert Student.count_documents() == 10
        assert Student.count_documents(limit=5, skip=6) == 4
        names = []
        for elem in Student.find(limit=5, skip=6):
            assert isinstance(elem, Student)
            names.append(elem.name)
        assert sorted(names) == ['student-%s' % i for i in range(6, 10)]

        cursor = Student.find(limit=5, skip=6)
        elem0 = cursor[0]
        assert isinstance(elem0, Student)
        assert next(cursor) == elem0

        # Make sure this kind of notation doesn't create new cursor
        cursor = Student.find()
        cursor_limit = cursor.limit(5)
        cursor_skip = cursor.skip(6)
        assert cursor is cursor_limit is cursor_skip

        # Cursor slicing
        cursor = Student.find()
        names = (elem.name for elem in cursor[2:5])
        assert sorted(names) == ['student-%s' % i for i in range(2, 5)]

        # Filter + projection
        cursor = Student.find({'name': 'student-0'}, ['name'])
        students = list(cursor)
        assert len(students) == 1
        assert students[0].name == 'student-0'

    def test_classroom(self, classroom_model):
        student = classroom_model.Student(name='Marty McFly', birthday=dt.datetime(1968, 6, 9))
        student.commit()
        teacher = classroom_model.Teacher(name='M. Strickland')
        teacher.commit()
        course = classroom_model.Course(name='Hoverboard 101', teacher=teacher)
        course.commit()
        assert student.courses is None
        student.courses = []
        assert student.courses == []
        student.courses.append(course)
        student.commit()
        assert student.to_mongo() == {
            '_id': student.pk,
            'name': 'Marty McFly',
            'birthday': dt.datetime(1968, 6, 9),
            'courses': [course.pk]
        }

    def test_validation_on_commit(self, instance):

        def io_validate(field, value):
            raise ma.ValidationError('Ho boys !')

        @instance.register
        class Dummy(Document):
            required_name = fields.StrField(required=True)
            always_io_fail = fields.IntField(io_validate=io_validate)

        with pytest.raises(ma.ValidationError) as exc:
            Dummy().commit()
        assert exc.value.messages == {'required_name': ['Missing data for required field.']}
        with pytest.raises(ma.ValidationError) as exc:
            Dummy(required_name='required', always_io_fail=42).commit()
        assert exc.value.messages == {'always_io_fail': ['Ho boys !']}

        dummy = Dummy(required_name='required')
        dummy.commit()
        del dummy.required_name
        with pytest.raises(ma.ValidationError) as exc:
            dummy.commit()
        assert exc.value.messages == {'required_name': ['Missing data for required field.']}

    def test_reference(self, classroom_model):
        teacher = classroom_model.Teacher(name='M. Strickland')
        teacher.commit()
        course = classroom_model.Course(name='Hoverboard 101', teacher=teacher)
        course.commit()
        assert isinstance(course.teacher, Reference)
        teacher_fetched = course.teacher.fetch()
        assert teacher_fetched == teacher
        # Change in referenced document is not seen until referenced
        # document is committed and referencer is reloaded
        teacher.name = 'Dr. Brown'
        assert course.teacher.fetch().name == 'M. Strickland'
        teacher.commit()
        assert course.teacher.fetch().name == 'M. Strickland'
        course.reload()
        assert course.teacher.fetch().name == 'Dr. Brown'
        # But we can force reload as soon as referenced document is committed
        # without having to reload the whole referencer
        teacher.name = 'M. Strickland'
        assert course.teacher.fetch().name == 'Dr. Brown'
        assert course.teacher.fetch(force_reload=True).name == 'Dr. Brown'
        teacher.commit()
        assert course.teacher.fetch().name == 'Dr. Brown'
        assert course.teacher.fetch(force_reload=True).name == 'M. Strickland'
        # Test bad ref as well
        course.teacher = Reference(classroom_model.Teacher, ObjectId())
        with pytest.raises(ma.ValidationError) as exc:
            course.io_validate()
        assert exc.value.messages == {'teacher': ['Reference not found for document Teacher.']}
        # Test setting to None / deleting
        course.teacher = None
        course.io_validate()
        del course.teacher
        course.io_validate()

    def test_io_validate(self, instance, classroom_model):
        Student = classroom_model.Student

        io_field_value = 'io?'
        io_validate_called = False

        def io_validate(field, value):
            assert field == IOStudent.schema.fields['io_field']
            assert value == io_field_value
            nonlocal io_validate_called
            io_validate_called = True

        @instance.register
        class IOStudent(Student):
            io_field = fields.StrField(io_validate=io_validate)

        student = IOStudent(name='Marty', io_field=io_field_value)
        assert not io_validate_called

        student.io_validate()
        assert io_validate_called

    def test_io_validate_error(self, instance, classroom_model):
        Student = classroom_model.Student

        def io_validate(field, value):
            raise ma.ValidationError('Ho boys !')

        @instance.register
        class EmbeddedDoc(EmbeddedDocument):
            io_field = fields.IntField(io_validate=io_validate)

        @instance.register
        class IOStudent(Student):
            io_field = fields.StrField(io_validate=io_validate)
            list_io_field = fields.ListField(fields.IntField(io_validate=io_validate))
            dict_io_field = fields.DictField(
                fields.StrField(),
                fields.IntField(io_validate=io_validate),
            )
            reference_io_field = fields.ReferenceField(
                classroom_model.Course, io_validate=io_validate)
            embedded_io_field = fields.EmbeddedField(EmbeddedDoc, io_validate=io_validate)

        bad_reference = ObjectId()
        student = IOStudent(
            name='Marty',
            io_field='io?',
            list_io_field=[1, 2],
            dict_io_field={"1": 1, "2": 2},
            reference_io_field=bad_reference,
            embedded_io_field={'io_field': 42}
        )
        with pytest.raises(ma.ValidationError) as exc:
            student.io_validate()
        assert exc.value.messages == {
            'io_field': ['Ho boys !'],
            'list_io_field': {0: ['Ho boys !'], 1: ['Ho boys !']},
            'dict_io_field': {"1": {"value": ['Ho boys !']}, "2": {"value": ['Ho boys !']}},
            'reference_io_field': ['Ho boys !', 'Reference not found for document Course.'],
            'embedded_io_field': {'io_field': ['Ho boys !']}
        }

    def test_io_validate_multi_validate(self, instance, classroom_model):
        Student = classroom_model.Student
        called = []

        def io_validate1(field, value):
            called.append('io_validate1')

        def io_validate2(field, value):
            called.append('io_validate2')

        @instance.register
        class IOStudent(Student):
            io_field = fields.StrField(io_validate=(io_validate1, io_validate2))

        student = IOStudent(name='Marty', io_field='io?')
        student.io_validate()
        assert called == ['io_validate1', 'io_validate2']

    def test_io_validate_list(self, instance, classroom_model):
        Student = classroom_model.Student
        called = []
        values = [1, 2, 3, 4]

        def io_validate(field, value):
            called.append(value)

        @instance.register
        class IOStudent(Student):
            io_field = fields.ListField(fields.IntField(io_validate=io_validate), allow_none=True)

        student = IOStudent(name='Marty', io_field=values)
        student.io_validate()
        assert called == values

        student.io_field = None
        student.io_validate()
        del student.io_field
        student.io_validate()

    def test_io_validate_dict(self, instance, classroom_model):
        Student = classroom_model.Student
        called = []
        keys = ["1", "2", "3", "4"]
        values = [1, 2, 3, 4]

        def io_validate(field, value):
            called.append(value)

        @instance.register
        class IOStudent(Student):
            io_field = fields.DictField(
                fields.StrField(),
                fields.IntField(io_validate=io_validate),
                allow_none=True
            )

        student = IOStudent(name='Marty', io_field=dict(zip(keys, values)))
        student.io_validate()
        assert called == values

        student.io_field = None
        student.io_validate()
        del student.io_field
        student.io_validate()

    def test_io_validate_embedded(self, instance, classroom_model):
        Student = classroom_model.Student

        @instance.register
        class EmbeddedDoc(EmbeddedDocument):
            io_field = fields.IntField()

        @instance.register
        class IOStudent(Student):
            embedded_io_field = fields.EmbeddedField(EmbeddedDoc, allow_none=True)

        student = IOStudent(name='Marty', embedded_io_field={'io_field': 12})
        student.io_validate()
        student.embedded_io_field = None
        student.io_validate()
        del student.embedded_io_field
        student.io_validate()

    def test_indexes(self, instance):

        @instance.register
        class SimpleIndexDoc(Document):
            indexed = fields.StrField()
            no_indexed = fields.IntField()

            class Meta:
                collection_name = 'simple_index_doc'
                indexes = ['indexed']

        SimpleIndexDoc.collection.drop()

        # Now ask for indexes building
        SimpleIndexDoc.ensure_indexes()
        indexes = list(SimpleIndexDoc.collection.list_indexes())
        expected_indexes = [
            {
                'key': {'_id': 1},
                'name': '_id_',
            },
            {
                'key': {'indexed': 1},
                'name': 'indexed_1',
            }
        ]
        assert strip_indexes(indexes) == expected_indexes

        # Redoing indexes building should do nothing
        SimpleIndexDoc.ensure_indexes()
        indexes = list(SimpleIndexDoc.collection.list_indexes())
        assert strip_indexes(indexes) == expected_indexes

    def test_indexes_inheritance(self, instance):

        @instance.register
        class SimpleIndexDoc(Document):
            indexed = fields.StrField()
            no_indexed = fields.IntField()

            class Meta:
                indexes = ['indexed']

        SimpleIndexDoc.collection.drop()

        # Now ask for indexes building
        SimpleIndexDoc.ensure_indexes()
        indexes = list(SimpleIndexDoc.collection.list_indexes())
        expected_indexes = [
            {
                'key': {'_id': 1},
                'name': '_id_',
            },
            {
                'key': {'indexed': 1},
                'name': 'indexed_1',
            }
        ]
        assert strip_indexes(indexes) == expected_indexes

        # Redoing indexes building should do nothing
        SimpleIndexDoc.ensure_indexes()
        indexes = list(SimpleIndexDoc.collection.list_indexes())
        assert strip_indexes(indexes) == expected_indexes

    def test_unique_index(self, instance):

        @instance.register
        class UniqueIndexDoc(Document):
            not_unique = fields.StrField(unique=False)
            sparse_unique = fields.IntField(unique=True)
            required_unique = fields.IntField(unique=True, required=True)

        UniqueIndexDoc.collection.drop()

        # Now ask for indexes building
        UniqueIndexDoc.ensure_indexes()
        indexes = list(UniqueIndexDoc.collection.list_indexes())
        expected_indexes = [
            {
                'key': {'_id': 1},
                'name': '_id_',
            },
            {
                'key': {'required_unique': 1},
                'name': 'required_unique_1',
                'unique': True,
            },
            {
                'key': {'sparse_unique': 1},
                'name': 'sparse_unique_1',
                'unique': True,
                'sparse': True,
            },
        ]

        assert name_sorted(strip_indexes(indexes)) == name_sorted(expected_indexes)

        # Redoing indexes building should do nothing
        UniqueIndexDoc.ensure_indexes()
        indexes = list(UniqueIndexDoc.collection.list_indexes())
        assert name_sorted(strip_indexes(indexes)) == name_sorted(expected_indexes)

        UniqueIndexDoc(not_unique='a', required_unique=1).commit()
        UniqueIndexDoc(not_unique='a', sparse_unique=1, required_unique=2).commit()
        with pytest.raises(ma.ValidationError) as exc:
            UniqueIndexDoc(not_unique='a', required_unique=1).commit()
        assert exc.value.messages == {'required_unique': 'Field value must be unique.'}
        with pytest.raises(ma.ValidationError) as exc:
            UniqueIndexDoc(not_unique='a', sparse_unique=1, required_unique=3).commit()
        assert exc.value.messages == {'sparse_unique': 'Field value must be unique.'}

    def test_unique_index_compound(self, instance):

        @instance.register
        class UniqueIndexCompoundDoc(Document):
            compound1 = fields.IntField()
            compound2 = fields.IntField()
            not_unique = fields.StrField()

            class Meta:
                # Must define custom index to do that
                indexes = [{'key': ('compound1', 'compound2'), 'unique': True}]

        UniqueIndexCompoundDoc.collection.drop()

        # Now ask for indexes building
        UniqueIndexCompoundDoc.ensure_indexes()
        indexes = list(UniqueIndexCompoundDoc.collection.list_indexes())
        expected_indexes = [
            {
                'key': {'_id': 1},
                'name': '_id_',
            },
            {
                'key': {'compound1': 1, 'compound2': 1},
                'name': 'compound1_1_compound2_1',
                'unique': True,
            }
        ]
        assert name_sorted(strip_indexes(indexes)) == name_sorted(expected_indexes)

        # Redoing indexes building should do nothing
        UniqueIndexCompoundDoc.ensure_indexes()
        indexes = list(UniqueIndexCompoundDoc.collection.list_indexes())
        assert name_sorted(strip_indexes(indexes)) == name_sorted(expected_indexes)

        # Index is on the tuple (compound1, compound2)
        UniqueIndexCompoundDoc(not_unique='a', compound1=1, compound2=1).commit()
        UniqueIndexCompoundDoc(not_unique='a', compound1=1, compound2=2).commit()
        UniqueIndexCompoundDoc(not_unique='a', compound1=2, compound2=1).commit()
        UniqueIndexCompoundDoc(not_unique='a', compound1=2, compound2=2).commit()
        with pytest.raises(ma.ValidationError) as exc:
            UniqueIndexCompoundDoc(not_unique='a', compound1=1, compound2=1).commit()
        assert exc.value.messages == {
            'compound2': "Values of fields ['compound1', 'compound2'] must be unique together.",
            'compound1': "Values of fields ['compound1', 'compound2'] must be unique together."
        }
        with pytest.raises(ma.ValidationError) as exc:
            UniqueIndexCompoundDoc(not_unique='a', compound1=2, compound2=1).commit()
        assert exc.value.messages == {
            'compound2': "Values of fields ['compound1', 'compound2'] must be unique together.",
            'compound1': "Values of fields ['compound1', 'compound2'] must be unique together."
        }

    @pytest.mark.xfail
    def test_unique_index_inheritance(self, instance):

        @instance.register
        class UniqueIndexParentDoc(Document):
            not_unique = fields.StrField(unique=False)
            unique = fields.IntField(unique=True)

        @instance.register
        class UniqueIndexChildDoc(UniqueIndexParentDoc):
            child_not_unique = fields.StrField(unique=False)
            child_unique = fields.IntField(unique=True)
            manual_index = fields.IntField()

            class Meta:
                indexes = ['manual_index']

        UniqueIndexChildDoc.collection.drop()

        # Now ask for indexes building
        UniqueIndexChildDoc.ensure_indexes()
        indexes = list(UniqueIndexChildDoc.collection.list_indexes())
        expected_indexes = [
            {
                'key': {'_id': 1},
                'name': '_id_',
            },
            {
                'key': {'unique': 1},
                'name': 'unique_1',
                'unique': True,
            },
            {
                'key': {'manual_index': 1, '_cls': 1},
                'name': 'manual_index_1__cls_1',
            },
            {
                'key': {'_cls': 1},
                'name': '_cls_1',
                'unique': True,
            },
            {
                'key': {'child_unique': 1, '_cls': 1},
                'name': 'child_unique_1__cls_1',
                'unique': True,
            }
        ]
        assert name_sorted(strip_indexes(indexes)) == name_sorted(expected_indexes)

        # Redoing indexes building should do nothing
        UniqueIndexChildDoc.ensure_indexes()
        indexes = list(UniqueIndexChildDoc.collection.list_indexes())
        assert name_sorted(strip_indexes(indexes)) == name_sorted(expected_indexes)

    def test_inheritance_search(self, instance):

        @instance.register
        class InheritanceSearchParent(Document):
            pf = fields.IntField()

        @instance.register
        class InheritanceSearchChild1(InheritanceSearchParent):
            c1f = fields.IntField()

        @instance.register
        class InheritanceSearchChild1Child(InheritanceSearchChild1):
            sc1f = fields.IntField()

        @instance.register
        class InheritanceSearchChild2(InheritanceSearchParent):
            c2f = fields.IntField(required=True)

        InheritanceSearchParent.collection.drop()

        InheritanceSearchParent(pf=0).commit()
        InheritanceSearchChild1(pf=1, c1f=1).commit()
        InheritanceSearchChild1Child(pf=1, sc1f=1).commit()
        InheritanceSearchChild2(pf=2, c2f=2).commit()

        assert InheritanceSearchParent.count_documents() == 4
        assert InheritanceSearchChild1.count_documents() == 2
        assert InheritanceSearchChild1Child.count_documents() == 1
        assert InheritanceSearchChild2.count_documents() == 1

        res = InheritanceSearchParent.find_one({'pf': 2})
        assert isinstance(res, InheritanceSearchChild2)

        res = InheritanceSearchParent.find({'pf': 1})
        for r in res:
            assert isinstance(r, InheritanceSearchChild1)

        isc = InheritanceSearchChild1(pf=2, c1f=2)
        isc.commit()
        res = InheritanceSearchChild1.find_one(isc.id)
        assert res == isc

        res = InheritanceSearchChild1.find_one(isc.id, ['c1f'])
        assert res.c1f == 2

    def test_search(self, instance):

        @instance.register
        class Author(EmbeddedDocument):
            name = fields.StrField(attribute='an')

        @instance.register
        class Chapter(EmbeddedDocument):
            name = fields.StrField(attribute='cn')

        @instance.register
        class Book(Document):
            title = fields.StrField(attribute='t')
            author = fields.EmbeddedField(Author, attribute='a')
            chapters = fields.ListField(fields.EmbeddedField(Chapter), attribute='c')

        Book.collection.drop()
        Book(
            title='The Hobbit', author={'name': 'JRR Tolkien'},
            chapters=[
                {'name': 'An Unexpected Party'},
                {'name': 'Roast Mutton'},
                {'name': 'A Short Rest'},
                {'name': 'Over Hill And Under Hill'},
                {'name': 'Riddles In The Dark'}
            ]
        ).commit()
        Book(
            title="Harry Potter and the Philosopher's Stone",
            author={'name': 'JK Rowling'},
            chapters=[
                {'name': 'The Boy Who Lived'},
                {'name': 'The Vanishing Glass'},
                {'name': 'The Letters from No One'},
                {'name': 'The Keeper of the Keys'},
                {'name': 'Diagon Alley'}
            ]
        ).commit()
        Book(
            title='A Game of Thrones',
            author={'name': 'George RR Martin'},
            chapters=[
                {'name': 'Prologue'},
                {'name': 'Bran I'},
                {'name': 'Catelyn I'},
                {'name': 'Daenerys I'},
                {'name': 'Eddard I'},
                {'name': 'Jon I'}
            ]
        ).commit()

        assert Book.count_documents({'title': 'The Hobbit'}) == 1
        assert Book.count_documents({'author.name': {'$in': ['JK Rowling', 'JRR Tolkien']}}) == 2
        assert Book.count_documents(
            {'$and': [{'chapters.name': 'Roast Mutton'}, {'title': 'The Hobbit'}]}) == 1
        assert Book.count_documents(
            {'chapters.name': {'$all': ['Roast Mutton', 'A Short Rest']}}) == 1

    def test_pre_post_hooks(self, instance):

        callbacks = []

        @instance.register
        class Person(Document):
            name = fields.StrField()
            age = fields.IntField()

            def pre_insert(self):
                callbacks.append('pre_insert')

            def pre_update(self):
                callbacks.append('pre_update')

            def pre_delete(self):
                callbacks.append('pre_delete')

            def post_insert(self, ret):
                assert isinstance(ret, InsertOneResult)
                callbacks.append('post_insert')

            def post_update(self, ret):
                assert isinstance(ret, UpdateResult)
                callbacks.append('post_update')

            def post_delete(self, ret):
                assert isinstance(ret, DeleteResult)
                callbacks.append('post_delete')

        p = Person(name='John', age=20)
        p.commit()
        assert callbacks == ['pre_insert', 'post_insert']

        callbacks.clear()
        p.age = 22
        p.commit()
        assert callbacks == ['pre_update', 'post_update']

        callbacks.clear()
        p.delete()
        assert callbacks == ['pre_delete', 'post_delete']

    def test_modify_in_pre_hook(self, instance):

        @instance.register
        class Person(Document):
            version = fields.IntField(required=True, attribute='_version')
            name = fields.StrField()
            age = fields.IntField()

            def pre_insert(self):
                self.version = 1

            def pre_update(self):
                # Prevent concurrency by checking a version number on update
                last_version = self.version
                self.version += 1
                return {'version': last_version}

            def pre_delete(self):
                return {'version': self.version}

        p = Person(name='John', age=20)
        p.commit()

        assert p.version == 1
        p_concurrent = Person.find_one(p.pk)

        p.age = 22
        p.commit()
        assert p.version == 2

        # Concurrent should not be able to commit it modifications
        p_concurrent.name = 'John'
        with pytest.raises(exceptions.UpdateError):
            p_concurrent.commit()

        p_concurrent.reload()
        assert p_concurrent.version == 2

        p.age = 24
        p.commit()
        assert p.version == 3
        p.delete()
        p.commit()
        with pytest.raises(exceptions.DeleteError):
            p_concurrent.delete()
        p.delete()

    def test_mixin_pre_post_hooks(self, instance):

        callbacks = []

        @instance.register
        class PrePostHooksMixin(MixinDocument):

            def pre_insert(self):
                callbacks.append('pre_insert')

            def pre_update(self):
                callbacks.append('pre_update')

            def pre_delete(self):
                callbacks.append('pre_delete')

            def post_insert(self, ret):
                assert isinstance(ret, InsertOneResult)
                callbacks.append('post_insert')

            def post_update(self, ret):
                assert isinstance(ret, UpdateResult)
                callbacks.append('post_update')

            def post_delete(self, ret):
                assert isinstance(ret, DeleteResult)
                callbacks.append('post_delete')

        @instance.register
        class Person(PrePostHooksMixin, Document):
            name = fields.StrField()
            age = fields.IntField()

        p = Person(name='John', age=20)
        p.commit()
        assert callbacks == ['pre_insert', 'post_insert']

        callbacks.clear()
        p.age = 22
        p.commit()
        assert callbacks == ['pre_update', 'post_update']

        callbacks.clear()
        p.delete()
        assert callbacks == ['pre_delete', 'post_delete']

    def test_session_context_manager(self, instance):
        """Test session is passed to framework methods"""

        coll_mock = mock.Mock(Collection, wraps=instance.db['Doc'])

        class MockMetaDocumentImplementation(MetaDocumentImplementation):
            @property
            def collection(cls):
                return coll_mock

        @instance.register
        class Doc(Document):
            # Set unique to create an index
            s = fields.StringField(unique=True)

        Doc.__class__ = MockMetaDocumentImplementation

        doc = Doc(s="test")

        with instance.session() as session:
            doc.commit()
        coll_mock.insert_one.assert_called_once()
        assert coll_mock.insert_one.call_args[1]["session"] == session

        doc.s = "retest"
        with instance.session() as session:
            doc.commit()
        coll_mock.update_one.assert_called_once()
        assert coll_mock.update_one.call_args[1]["session"] == session

        doc.s = "reretest"
        with instance.session() as session:
            doc.commit(replace=True)
        coll_mock.replace_one.assert_called_once()
        assert coll_mock.replace_one.call_args[1]["session"] == session

        with instance.session() as session:
            doc.reload()
        coll_mock.find_one.assert_called_once()
        assert coll_mock.find_one.call_args[1]["session"] == session

        with instance.session() as session:
            doc.delete()
        coll_mock.delete_one.assert_called_once()
        assert coll_mock.delete_one.call_args[1]["session"] == session

        coll_mock.reset_mock()
        with instance.session() as session:
            Doc.find_one(doc.id)
        coll_mock.find_one.assert_called_once()
        assert coll_mock.find_one.call_args[1]["session"] == session

        with instance.session() as session:
            Doc.find()
        coll_mock.find.assert_called_once()
        assert coll_mock.find.call_args[1]["session"] == session

        with instance.session() as session:
            Doc.count_documents()
        coll_mock.count_documents.assert_called_once()
        assert coll_mock.count_documents.call_args[1]["session"] == session

        with instance.session() as session:
            Doc.ensure_indexes()
        coll_mock.create_indexes.assert_called_once()
        assert coll_mock.create_indexes.call_args[1]["session"] == session

    def test_2_to_3_migration(self, db):

        instance = framework_pymongo.PyMongoMigrationInstance(db)

        @instance.register
        class AbstractEmbeddedDoc(EmbeddedDocument):
            f = fields.StringField()

            class Meta:
                abstract = True

        @instance.register
        class ConcreteEmbeddedDoc(AbstractEmbeddedDoc):
            pass

        @instance.register
        class ConcreteEmbeddedDocChild(ConcreteEmbeddedDoc):
            pass

        @instance.register
        class AbstractDoc(Document):

            class Meta:
                abstract = True

        @instance.register
        class Doc(AbstractDoc):
            ec = fields.EmbeddedField(ConcreteEmbeddedDoc)
            ecc = fields.EmbeddedField(ConcreteEmbeddedDocChild)

        @instance.register
        class DocChild(Doc):
            cec = fields.EmbeddedField(ConcreteEmbeddedDoc)
            cecc = fields.EmbeddedField(ConcreteEmbeddedDocChild)

        doc_umongo_2 = {
            "ec": {"f": "Hello", "_cls": "ConcreteEmbeddedDoc"},
            "ecc": {"f": "Hi", "_cls": "ConcreteEmbeddedDocChild"},
        }
        child_doc_umongo_2 = {
            "_cls": "DocChild",
            "ec": {"f": "Hello", "_cls": "ConcreteEmbeddedDoc"},
            "ecc": {"f": "Hi", "_cls": "ConcreteEmbeddedDocChild"},
            "cec": {"f": "Hello", "_cls": "ConcreteEmbeddedDoc"},
            "cecc": {"f": "Hi", "_cls": "ConcreteEmbeddedDocChild"},
        }

        doc_umongo_3 = {
            "ec": {"f": "Hello"},
            "ecc": {"f": "Hi", "_cls": "ConcreteEmbeddedDocChild"},
        }
        child_doc_umongo_3 = {
            "_cls": "DocChild",
            "ec": {"f": "Hello"},
            "ecc": {"f": "Hi", "_cls": "ConcreteEmbeddedDocChild"},
            "cec": {"f": "Hello"},
            "cecc": {"f": "Hi", "_cls": "ConcreteEmbeddedDocChild"},
        }

        res = instance.db.doc.insert_one(doc_umongo_2)
        doc_umongo_3['_id'] = res.inserted_id
        res = instance.db.doc.insert_one(child_doc_umongo_2)
        child_doc_umongo_3['_id'] = res.inserted_id

        instance.migrate_2_to_3()

        res = instance.db.doc.find_one(doc_umongo_3['_id'])
        assert res == doc_umongo_3
        res = instance.db.doc.find_one(child_doc_umongo_3['_id'])
        assert res == child_doc_umongo_3
