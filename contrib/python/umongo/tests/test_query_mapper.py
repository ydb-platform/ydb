import datetime as dt

from bson import ObjectId

from umongo import Document, EmbeddedDocument, fields
from umongo.query_mapper import map_query

from .common import BaseTest, assert_equal_order


class TestQueryMapper(BaseTest):

    def test_query_mapper(self):

        @self.instance.register
        class Editor(Document):
            name = fields.StrField()

        @self.instance.register
        class Author(EmbeddedDocument):
            name = fields.StrField()
            birthday = fields.DateTimeField(attribute='b')

        @self.instance.register
        class Chapter(EmbeddedDocument):
            title = fields.StrField()
            pagination = fields.IntField(attribute='p')

        @self.instance.register
        class Book(Document):
            title = fields.StrField()
            length = fields.IntField(attribute='l')
            author = fields.EmbeddedField(Author, attribute='a')
            chapters = fields.ListField(fields.EmbeddedField(Chapter))
            tags = fields.ListField(fields.StrField(), attribute='t')
            editor = fields.ReferenceField(Editor, attribute='e')

        book_fields = Book.schema.fields
        # No changes needed
        assert map_query({'title': 'The Lord of The Ring'}, book_fields) == {
            'title': 'The Lord of The Ring'}
        # Single substitution
        assert map_query({'length': 350}, book_fields) == {'l': 350}
        # Multiple substitutions
        assert map_query({
            'length': 350,
            'title': 'The Hobbit',
            'author': 'JRR Tolkien'
        }, book_fields) == {'l': 350, 'title': 'The Hobbit', 'a': 'JRR Tolkien'}

        # mongo query commands should not be altered
        assert map_query({
            'title': {'$in': ['The Hobbit', 'The Lord of The Ring']},
            'author': {'$in': ['JRR Tolkien', 'Peter Jackson']}
        }, book_fields) == {
            'title': {'$in': ['The Hobbit', 'The Lord of The Ring']},
            'a': {'$in': ['JRR Tolkien', 'Peter Jackson']}
        }
        assert map_query({
            '$or': [{'author': 'JRR Tolkien'}, {'length': 350}]
        }, book_fields) == {
            '$or': [{'a': 'JRR Tolkien'}, {'l': 350}]
        }

        # Test dot notation as well
        assert map_query({
            'author.name': 'JRR Tolkien',
            'author.birthday': dt.datetime(1892, 1, 3),
            'chapters.pagination': 81
        }, book_fields) == {
            'a.name': 'JRR Tolkien',
            'a.b': dt.datetime(1892, 1, 3),
            'chapters.p': 81
        }
        assert map_query({
            'chapters.$.pagination': 81
        }, book_fields) == {
            'chapters.$.p': 81
        }

        # Test embedded document conversion
        assert map_query({
            'author': {
                'name': 'JRR Tolkien',
                'birthday': dt.datetime(1892, 1, 3)
            }
        }, book_fields) == {
            'a': {'name': 'JRR Tolkien', 'b': dt.datetime(1892, 1, 3)}
        }

        # Test list conversion
        assert map_query({
            'tags': {'$all': ['Fantasy', 'Classic']}
        }, book_fields) == {
            't': {'$all': ['Fantasy', 'Classic']}
        }
        assert map_query({
            'chapters': {'$all': [
                {'$elemMatch': {'pagination': 81}},
                {'$elemMatch': {'title': 'An Unexpected Party'}}
            ]}
        }, book_fields) == {
            'chapters': {'$all': [
                {'$elemMatch': {'p': 81}},
                {'$elemMatch': {'title': 'An Unexpected Party'}}
            ]}
        }

        # Test embedded document in query
        query = map_query({
            'author': Author(name='JRR Tolkien', birthday=dt.datetime(1892, 1, 3))
        }, book_fields)
        assert query == {
            'a': {'name': 'JRR Tolkien', 'b': dt.datetime(1892, 1, 3)}
        }
        assert isinstance(query['a'], dict)
        # Check the order is preserved when serializing the embedded document
        # in the query. This is necessary as MongoDB only matches embedded
        # documents with same order.
        expected = {'name': 'JRR Tolkien', 'b': dt.datetime(1892, 1, 3)}
        assert_equal_order(query["a"], expected)

        # Test document in query
        editor = Editor(name='Allen & Unwin')
        editor.id = ObjectId()
        query = map_query({'editor': editor}, book_fields)
        assert isinstance(query['e'], ObjectId)
        assert query['e'] == editor.id

    def test_mix(self):

        @self.instance.register
        class Person(EmbeddedDocument):
            name = fields.StrField(attribute='pn')

        @self.instance.register
        class Company(EmbeddedDocument):
            name = fields.StrField(attribute='cn')
            contact = fields.EmbeddedField(Person, attribute='cc')

        @self.instance.register
        class Team(Document):
            name = fields.StrField(attribute='n')
            leader = fields.EmbeddedField(Person, attribute='l')
            sponsors = fields.ListField(fields.EmbeddedField(Company), attribute='s')

        team_fields = Team.schema.fields
        assert map_query({'leader.name': 1}, team_fields) == {'l.pn': 1}
        assert map_query({'leader': {'name': 1}}, team_fields) == {'l': {'pn': 1}}
        assert map_query({'sponsors.name': 1}, team_fields) == {'s.cn': 1}
        assert map_query({'sponsors': {'name': 1}}, team_fields) == {'s': {'cn': 1}}
        assert map_query({'sponsors.contact.name': 1}, team_fields) == {'s.cc.pn': 1}
        assert map_query(
            {'sponsors': {'contact': {'name': 1}}}, team_fields) == {'s': {'cc': {'pn': 1}}}
