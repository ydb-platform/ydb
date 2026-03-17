import datetime
import sys
import unittest
import json

from markupsafe import escape
from peewee import *
from playhouse.postgres_ext import BinaryJSONField as PostgresBinaryJSONField
from playhouse.postgres_ext import JSONField as PostgresJSONField
from playhouse.sqlite_ext import JSONField as SQLiteJSONField
from wtforms import __version__ as wtforms_version
from wtforms import fields as wtfields
from wtforms.form import Form as WTForm
from wtforms.validators import Length, Regexp
from wtfpeewee.fields import *
from wtfpeewee.fields import wtf_choice
from wtfpeewee.orm import model_form
from wtfpeewee._compat import PY2


test_db = SqliteDatabase(':memory:')

class TestModel(Model):
    class Meta:
        database = test_db


class Blog(TestModel):
    title = CharField()

    def __str__(self):
        return self.title


class Entry(TestModel):
    pk = AutoField()
    blog = ForeignKeyField(Blog)
    title = CharField(verbose_name='Wacky title')
    content = TextField()
    pub_date = DateTimeField(default=datetime.datetime.now)

    def __str__(self):
        return '%s: %s' % (self.blog.title, self.title)


class NullEntry(TestModel):
    blog = ForeignKeyField(Blog, null=True)


class NullFieldsModel(TestModel):
    c = CharField(null=True)
    b = BooleanField(null=True)


class ChoicesModel(TestModel):
    gender = CharField(choices=(('m', 'Male'), ('f', 'Female')))
    status = IntegerField(choices=((1, 'One'), (2, 'Two')), null=True)
    salutation = CharField(null=True)
    true_or_false = BooleanField(choices=((True, 't'), (False, 'f')))


class BlankChoices(TestModel):
    status = IntegerField(choices=((1, 'One'), (2, 'Two')),
                          null=True)


class NonIntPKModel(TestModel):
    id = CharField(primary_key=True)
    value = CharField()


class PostgresJSONModel(TestModel):
    content = PostgresBinaryJSONField(null=True)


class SQLiteJSONModel(TestModel):
    content = SQLiteJSONField(null=True)


BlogForm = model_form(Blog)
EntryForm = model_form(Entry)
NullFieldsModelForm = model_form(NullFieldsModel)
ChoicesForm = model_form(ChoicesModel, field_args={'salutation': {'choices': (('mr', 'Mr.'), ('mrs', 'Mrs.'))}})
BlankChoicesForm = model_form(BlankChoices)
NonIntPKForm = model_form(NonIntPKModel, allow_pk=True)
PostgresJSONForm = model_form(PostgresJSONModel)
SQLiteJSONForm = model_form(SQLiteJSONModel)

class FakePost(dict):
    def getlist(self, key):
        val = self[key]
        if isinstance(val, list):
            return val
        return [val]


class WTFPeeweeTestCase(unittest.TestCase):
    def setUp(self):
        NullEntry.drop_table(True)
        Entry.drop_table(True)
        Blog.drop_table(True)
        NullFieldsModel.drop_table(True)
        NonIntPKModel.drop_table(True)
        SQLiteJSONModel.drop_table(True)

        Blog.create_table()
        Entry.create_table()
        NullEntry.create_table()
        NullFieldsModel.create_table()
        NonIntPKModel.create_table()
        SQLiteJSONModel.create_table()

        self.blog_a = Blog.create(title='a')
        self.blog_b = Blog.create(title='b')

        self.entry_a1 = Entry.create(blog=self.blog_a, title='a1', content='a1 content', pub_date=datetime.datetime(2011, 1, 1))
        self.entry_a2 = Entry.create(blog=self.blog_a, title='a2', content='a2 content', pub_date=datetime.datetime(2011, 1, 2))
        self.entry_b1 = Entry.create(blog=self.blog_b, title='b1', content='b1 content', pub_date=datetime.datetime(2011, 1, 1))

    def assertChoices(self, c, expected):
        self.assertEqual(list(c.iter_choices()), [wtf_choice(*i) for i in expected])

    def test_defaults(self):
        BlogFormDef = model_form(Blog, field_args={'title': {'default': 'hello world'}})

        form = BlogFormDef()
        self.assertEqual(form.data, {'title': 'hello world'})

        form = BlogFormDef(obj=self.blog_a)
        self.assertEqual(form.data, {'title': 'a'})

    def test_duplicate_validators(self):
        ''' Test whether validators are duplicated when forms share field_args
        '''
        shared_field_args = {'id': {'validators': [Regexp('test')]}}

        ValueIncludedForm = model_form(NonIntPKModel,
                                       field_args=shared_field_args,
                                       allow_pk=True)
        ValueExcludedForm = model_form(NonIntPKModel,
                                       field_args=shared_field_args,
                                       allow_pk=True,
                                       exclude=['value'])

        value_included_form = ValueIncludedForm()
        self.assertEqual(len(value_included_form.id.validators), 2)

        value_excluded_form = ValueExcludedForm()
        self.assertEqual(len(value_excluded_form.id.validators), 2)

    def test_non_int_pk(self):
        form = NonIntPKForm()
        self.assertEqual(form.data, {'value': None, 'id': None})
        self.assertFalse(form.validate())

        obj = NonIntPKModel(id='a', value='A')
        form = NonIntPKForm(obj=obj)
        self.assertEqual(form.data, {'value': 'A', 'id': 'a'})
        self.assertTrue(form.validate())

        form = NonIntPKForm(FakePost({'id': 'b', 'value': 'B'}))
        self.assertTrue(form.validate())

        obj = NonIntPKModel()
        form.populate_obj(obj)
        self.assertEqual(obj.id, 'b')
        self.assertEqual(obj.value, 'B')

        self.assertEqual(NonIntPKModel.select().count(), 0)
        obj.save(True)
        self.assertEqual(NonIntPKModel.select().count(), 1)

        # its hard to validate unique-ness because a form may be updating
        #form = NonIntPKForm(FakePost({'id': 'b', 'value': 'C'}))
        #self.assertFalse(form.validate())

    def test_choices(self):
        form = ChoicesForm()
        self.assertTrue(isinstance(form.gender, SelectChoicesField))
        self.assertTrue(isinstance(form.status, SelectChoicesField))
        self.assertTrue(isinstance(form.salutation, SelectChoicesField))
        self.assertTrue(isinstance(form.true_or_false, wtfields.BooleanField))

        self.assertChoices(form.gender, [
            ('m', 'Male', False),
            ('f', 'Female', False)])
        self.assertChoices(form.status, [
            ('__None', '----------------', True),
            (1, 'One', False),
            (2, 'Two', False)])
        self.assertChoices(form.salutation, [
            ('__None', '----------------', True),
            ('mr', 'Mr.', False),
            ('mrs', 'Mrs.', False)])

        choices_obj = ChoicesModel(gender='m', status=2, salutation=None)
        form = ChoicesForm(obj=choices_obj)
        self.assertEqual(form.data, {'gender': 'm', 'status': 2, 'salutation': None, 'true_or_false': False})
        self.assertTrue(form.validate())

        choices_obj = ChoicesModel(gender='f', status=1, salutation='mrs', true_or_false=True)
        form = ChoicesForm(obj=choices_obj)
        self.assertEqual(form.data, {'gender': 'f', 'status': 1, 'salutation': 'mrs', 'true_or_false': True})
        self.assertTrue(form.validate())

        choices_obj.gender = 'x'
        form = ChoicesForm(obj=choices_obj)
        self.assertFalse(form.validate())
        if wtforms_version[0] == '2':
            errmsg = 'Not a valid choice'
        else:
            errmsg = 'Not a valid choice.'
        self.assertEqual(form.errors, {'gender': [errmsg]})

        choices_obj.gender = 'm'
        choices_obj.status = '1'
        form = ChoicesForm(obj=choices_obj)
        self.assertEqual(form.status.data, 1)
        self.assertTrue(form.validate())

        # "3" is not a valid status.
        form = ChoicesForm(FakePost({'status': '3'}), obj=choices_obj)

        # Invalid choice -- must be 1 or 2.
        self.assertFalse(form.validate())
        self.assertTrue(list(form.errors), ['status'])

        # Nullable field with choices:
        choices_obj.status = None
        form = ChoicesForm(obj=choices_obj)
        self.assertEqual(form.status.data, None)
        self.assertTrue(form.validate())
        self.assertFalse(list(form.errors), ['status'])

        # Not-nullable field with choices:
        choices_obj.gender = None
        form = ChoicesForm(obj=choices_obj)
        self.assertEqual(form.status.data, None)
        self.assertFalse(form.validate())
        self.assertTrue(list(form.errors), ['gender'])

    def test_blank_choices(self):
        obj = BlankChoices(status=None)
        form = BlankChoicesForm(obj=obj)
        self.assertTrue(form.validate())

        # Ensure that the "None" status value is set when populating an object
        # (overwriting a previous non-empty value).
        new_obj = BlankChoices(status=1)
        form.populate_obj(new_obj)
        self.assertTrue(new_obj.status is None)

        new_obj = BlankChoices(status=1)
        form = BlankChoicesForm(FakePost({'status': ''}))
        self.assertTrue(form.validate())
        form.populate_obj(new_obj)
        self.assertTrue(new_obj.status is None)

        new_obj = BlankChoices()
        form = BlankChoicesForm(FakePost({'status': 1}))
        self.assertTrue(form.validate())
        form.populate_obj(new_obj)
        self.assertEqual(new_obj.status, 1)

        form = BlankChoicesForm(FakePost({'status': 3}))
        self.assertFalse(form.validate())

    def test_blog_form(self):
        form = BlogForm()
        self.assertEqual(list(form._fields.keys()), ['title'])
        self.assertTrue(isinstance(form.title, wtfields.StringField))
        self.assertEqual(form.data, {'title': None})

    def test_entry_form(self):
        form = EntryForm()
        self.assertEqual(sorted(form._fields.keys()), ['blog', 'content', 'pub_date', 'title'])

        self.assertTrue(isinstance(form.blog, ModelSelectField))
        self.assertTrue(isinstance(form.content, wtfields.TextAreaField))
        self.assertTrue(isinstance(form.pub_date, WPDateTimeField))
        self.assertTrue(isinstance(form.title, wtfields.StringField))

        self.assertEqual(form.title.label.text, 'Wacky title')
        self.assertEqual(form.blog.label.text, 'Blog')
        self.assertEqual(form.pub_date.label.text, 'Pub Date')

        # check that the default value appears
        self.assertTrue(isinstance(form.pub_date.data, datetime.datetime))

        # check that the foreign key defaults to none
        self.assertEqual(form.blog.data, None)

        # check that the options look right
        self.assertChoices(form.blog, [
            (self.blog_a._pk, u'a', False),
            (self.blog_b._pk, u'b', False)])

    def test_blog_form_with_obj(self):
        form = BlogForm(obj=self.blog_a)
        self.assertEqual(form.data, {'title': 'a'})
        self.assertTrue(form.validate())

    def test_entry_form_with_obj(self):
        form = EntryForm(obj=self.entry_a1)
        self.assertEqual(form.data, {
            'title': 'a1',
            'content': 'a1 content',
            'pub_date': datetime.datetime(2011, 1, 1),
            'blog': self.blog_a,
        })
        self.assertTrue(form.validate())

        # check that the options look right
        self.assertChoices(form.blog, [
            (self.blog_a._pk, u'a', True),
            (self.blog_b._pk, u'b', False)])

    def test_blog_form_saving(self):
        form = BlogForm(FakePost({'title': 'new blog'}))
        self.assertTrue(form.validate())

        blog = Blog()
        form.populate_obj(blog)
        self.assertEqual(blog.title, 'new blog')

        # no new blogs were created
        self.assertEqual(Blog.select().count(), 2)

        # explicitly calling save will create the new blog
        blog.save()

        # make sure we created a new blog
        self.assertEqual(Blog.select().count(), 3)

        form = BlogForm(FakePost({'title': 'a edited'}), obj=self.blog_a)
        self.assertTrue(form.validate())
        form.populate_obj(self.blog_a)

        self.assertEqual(self.blog_a.title, 'a edited')
        self.blog_a.save()

        # make sure no new blogs were created
        self.assertEqual(Blog.select().count(), 3)

        # grab it from the database
        a = Blog.get(title='a edited')

    def test_entry_form_saving(self):
        # check count of entries
        self.assertEqual(Entry.select().count(), 3)

        form = EntryForm(FakePost({
            'title': 'new entry',
            'content': 'some content',
            'pub_date-date': '2011-02-01',
            'pub_date-time': '00:00:00',
            'blog': self.blog_b._pk,
        }))
        self.assertTrue(form.validate())

        self.assertEqual(form.pub_date.data, datetime.datetime(2011, 2, 1))
        self.assertEqual(form.blog.data, self.blog_b)

        entry = Entry()
        form.populate_obj(entry)

        # ensure entry count hasn't changed
        self.assertEqual(Entry.select().count(), 3)

        entry.save()
        self.assertEqual(Entry.select().count(), 4)
        self.assertEqual(self.blog_a.entry_set.count(), 2)
        self.assertEqual(self.blog_b.entry_set.count(), 2)

        # make sure the blog object came through ok
        self.assertEqual(entry.blog, self.blog_b)

        # edit entry a1
        form = EntryForm(FakePost({
            'title': 'a1 edited',
            'content': 'a1 content',
            'pub_date': '2011-01-01 00:00:00',
            'blog': self.blog_b._pk,
        }), obj=self.entry_a1)
        self.assertTrue(form.validate())

        form.populate_obj(self.entry_a1)
        self.entry_a1.save()

        self.assertEqual(self.entry_a1.blog, self.blog_b)

        self.assertEqual(self.blog_a.entry_set.count(), 1)
        self.assertEqual(self.blog_b.entry_set.count(), 3)

        # pull from the db just to be 100% sure
        a1 = Entry.get(title='a1 edited')

        form = EntryForm(FakePost({
            'title': 'new',
            'content': 'blah',
            'pub_date': '2011-01-01 00:00:00',
            'blog': 10000
        }))
        self.assertFalse(form.validate())

    def test_null_form_saving(self):
        form = NullFieldsModelForm(FakePost({'c': ''}))
        self.assertTrue(form.validate())

        nfm = NullFieldsModel()
        form.populate_obj(nfm)
        self.assertEqual(nfm.c, None)

        # this is a bit odd, but since checkboxes do not send a value if they
        # are unchecked this will evaluate to false (and passing in an empty
        # string evalutes to true) since the wtforms booleanfield blindly coerces
        # to bool
        self.assertEqual(nfm.b, False)

        form = NullFieldsModelForm(FakePost({'c': '', 'b': ''}))
        self.assertTrue(form.validate())

        nfm = NullFieldsModel()
        form.populate_obj(nfm)
        self.assertEqual(nfm.c, None)

        # again, this is for the purposes of documenting behavior -- nullable
        # booleanfields won't work without a custom field class
        # Passing an empty string will evalute to False
        # https://bitbucket.org/simplecodes/wtforms/commits/35c5f7182b7f0c62a4d4db7a1ec8719779b4b018
        self.assertEqual(nfm.b, False)

        form = NullFieldsModelForm(FakePost({'c': 'test'}))
        self.assertTrue(form.validate())

        nfm = NullFieldsModel()
        form.populate_obj(nfm)
        self.assertEqual(nfm.c, 'test')

    def test_form_with_only_exclude(self):
        frm = model_form(Entry, only=('title', 'content',))()
        self.assertEqual(sorted(frm._fields.keys()), ['content', 'title'])

        frm = model_form(Entry, exclude=('title', 'content',))()
        self.assertEqual(sorted(frm._fields.keys()), ['blog', 'pub_date'])

    def test_form_multiple(self):
        class TestForm(WTForm):
            blog = SelectMultipleQueryField(query=Blog.select())

        frm = TestForm()
        self.assertChoices(frm.blog, [
            (self.blog_a.id, 'a', False),
            (self.blog_b.id, 'b', False)])

        frm = TestForm(FakePost({'blog': [self.blog_b.id]}))
        self.assertChoices(frm.blog, [
            (self.blog_a.id, 'a', False),
            (self.blog_b.id, 'b', True)])
        self.assertEqual(frm.blog.data, [self.blog_b])
        self.assertTrue(frm.validate())

        frm = TestForm(FakePost({'blog': [self.blog_b.id, self.blog_a.id]}))
        self.assertChoices(frm.blog, [
            (self.blog_a.id, 'a', True),
            (self.blog_b.id, 'b', True)])
        self.assertEqual(frm.blog.data, [self.blog_a, self.blog_b])
        self.assertTrue(frm.validate())

        bad_id = [x for x in range(1,4) if x not in [self.blog_a.id, self.blog_b.id]][0]
        frm = TestForm(FakePost({'blog': [self.blog_b.id, bad_id]}))
        self.assertTrue(frm.validate())

    def test_hidden_field(self):
        class TestEntryForm(WTForm):
            blog = HiddenQueryField(query=Blog.select())
            title = wtfields.StringField()
            content = wtfields.TextAreaField()

        form = TestEntryForm(FakePost({
            'title': 'new entry',
            'content': 'some content',
            'blog': self.blog_b._pk,
        }))

        # check the htmlz for the form's hidden field
        html = form._fields['blog']()
        self.assertEqual(html, u'<input id="blog" name="blog" type="hidden" value="%s">' % self.blog_b._pk)

        self.assertTrue(form.validate())
        self.assertEqual(form.blog.data, self.blog_b)

        entry = Entry()
        form.populate_obj(entry)

        # ensure entry count hasn't changed
        self.assertEqual(Entry.select().count(), 3)

        entry.save()
        self.assertEqual(Entry.select().count(), 4)
        self.assertEqual(self.blog_a.entry_set.count(), 2)
        self.assertEqual(self.blog_b.entry_set.count(), 2)

        # make sure the blog object came through ok
        self.assertEqual(entry.blog, self.blog_b)

        # edit entry a1
        form = TestEntryForm(FakePost({
            'title': 'a1 edited',
            'content': 'a1 content',
            'blog': self.blog_b._pk,
        }), obj=self.entry_a1)

        # check the htmlz for the form's hidden field
        html = form._fields['blog']()
        self.assertEqual(html, u'<input id="blog" name="blog" type="hidden" value="%s">' % self.blog_b._pk)

        self.assertTrue(form.validate())

        form.populate_obj(self.entry_a1)
        self.entry_a1.save()

        self.assertEqual(self.entry_a1.blog, self.blog_b)

        self.assertEqual(self.blog_a.entry_set.count(), 1)
        self.assertEqual(self.blog_b.entry_set.count(), 3)

        # pull from the db just to be 100% sure
        a1 = Entry.get(title='a1 edited')

    def test_hidden_field_none(self):
        class TestNullEntryForm(WTForm):
            blog = HiddenQueryField(query=Blog.select())

        form = TestNullEntryForm(FakePost({
            'blog': '',
        }))

        # check the htmlz for the form's hidden field
        html = form._fields['blog']()
        self.assertEqual(html, u'<input id="blog" name="blog" type="hidden" value="">')

        self.assertTrue(form.validate())
        self.assertEqual(form.blog.data, None)

        entry = NullEntry()
        form.populate_obj(entry)

        # ensure entry count hasn't changed
        self.assertEqual(NullEntry.select().count(), 0)

        entry.save()
        self.assertEqual(NullEntry.select().count(), 1)

        # make sure the blog object came through ok
        self.assertEqual(entry.blog, None)

        # edit entry a1
        form = TestNullEntryForm(FakePost({
            'blog': None,
        }), obj=self.entry_a1)

        # check the htmlz for the form's hidden field
        html = form._fields['blog']()
        self.assertEqual(html, u'<input id="blog" name="blog" type="hidden" value="">')

        self.assertTrue(form.validate())

    def test_postgres_json_field(self):
        form = PostgresJSONForm()
        self.assertTrue(isinstance(form.content, WPJSONAreaField))

        # test empty string
        form = PostgresJSONForm(FakePost({
            'content': '',
        }))

        self.assertTrue(form.validate())
        self.assertEqual(form.content.data, None)

        # test None content
        form = PostgresJSONForm(FakePost({
            'content': None,
        }))

        self.assertTrue(form.validate())
        self.assertEqual(form.content.data, None)

        # test simple empty array
        form = PostgresJSONForm(FakePost({
            'content': '[]',
        }))

        self.assertTrue(form.validate())
        self.assertEqual(form.content.data, [])

        # test more complex valid json object
        teststruct = {'arr': [2.7183, 3.1416, 42], 'str': 'the answer'}

        form = PostgresJSONForm(FakePost({
            'content': json.dumps(teststruct),
        }))

        self.assertTrue(form.validate())
        self.assertEqual(form.content.data, teststruct)

        # test invalid json string
        form = PostgresJSONForm(FakePost({
            'content': 'somerandomstringwithoutquotes'
        }))

        self.assertFalse(form.validate())
        self.assertEqual(form.content.data, None)

        # test invalid json string (syntax error)
        form = PostgresJSONForm(FakePost({
            'content': '{"str": "the answer", }'
        }))

        self.assertFalse(form.validate())
        self.assertEqual(form.content.data, None)

    def test_sqlite_json_field(self):
        # most of the form tests are done in test_postgres_json_field
        # here we check the string generated when loading from the database

        teststruct = {'arr': [2.7183, 3.1416, 42], 'str': 'the answer'}
        teststring = json.dumps(teststruct)

        entry = SQLiteJSONModel.create(content=teststruct)
        form = SQLiteJSONForm(obj=entry)

        self.assertTrue(isinstance(form.content, WPJSONAreaField))
        self.assertTrue(form.validate())
        self.assertEqual(form.content.data, teststruct)

    def test_check_form_data(self):
        class A(TestModel):
            key = TextField()
            value = TextField()

        Form = model_form(A)
        form = Form()
        self.assertEqual(form.data, {'key': None, 'value': None})

        form = Form(FakePost({'key': 'asdf'}))
        self.assertEqual(form.data, {'key': 'asdf', 'value': None})

    def test_optional_foreign_key(self):
        Form = model_form(NullEntry)
        self.assertTrue(Form(FakePost({'blog': self.blog_a.id})).validate())
        self.assertFalse(Form(FakePost({'blog': '10000'})).validate())

        Form = model_form(NullEntry, field_args={'blog': {
            'validators': [Length(max=10)]}})
        form = Form(FakePost({'blog': 'xyz'}))
        self.assertFalse(form.validate())
        self.assertEqual(form.errors, {'blog': ['Not a valid choice.']})

        form = Form(FakePost({'blog': 'xyzxyzxyzxyz'}))
        self.assertFalse(form.validate())
        self.assertEqual(form.errors, {'blog': [
            'Not a valid choice.',
            'Field cannot be longer than 10 characters.']})


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
