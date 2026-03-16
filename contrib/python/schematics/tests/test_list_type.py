from collections import OrderedDict

import pytest

from schematics.models import Model
from schematics.translator import _
from schematics.types import IntType, StringType
from schematics.types.compound import ModelType, ListType
from schematics.exceptions import (
    ConversionError, ValidationError, StopValidationError, DataError,
    MockCreationError)


def test_list_field():
    class User(Model):
        ids = ListType(StringType, required=True)

    c = User({
        "ids": []
    })

    c.validate({'ids': []})

    assert c.ids == []


def test_list_with_default_type():
    class CategoryStatsInfo(Model):
        slug = StringType()

    class PlayerInfo(Model):
        categories = ListType(ModelType(CategoryStatsInfo))

    math_stats = CategoryStatsInfo(dict(slug="math"))
    twilight_stats = CategoryStatsInfo(dict(slug="twilight"))
    info = PlayerInfo({
        "categories": [{"slug": "math"}, {"slug": "twilight"}]
    })

    assert info.categories == [math_stats, twilight_stats]

    d = info.serialize()
    assert d == {
        "categories": [{"slug": "math"}, {"slug": "twilight"}],
    }


def test_set_default():
    class CategoryStatsInfo(Model):
        slug = StringType()

    class PlayerInfo(Model):
        categories = ListType(ModelType(CategoryStatsInfo),
                              default=lambda: [],
                              serialize_when_none=True)

    info = PlayerInfo()
    assert info.categories == []

    d = info.serialize()
    assert d == {
        "categories": [],
    }


def test_list_defaults_to_none():
    class PlayerInfo(Model):
        following = ListType(StringType)

    info = PlayerInfo()

    assert info.following is None

    assert info.serialize() == {
        "following": None,
    }


def test_list_default_to_none_embedded_model():
    class QuestionResource(Model):
        url = StringType()

    class QuestionResources(Model):
        pictures = ListType(ModelType(QuestionResource))

    class Question(Model):
        id = StringType()
        resources = ModelType(QuestionResources)

    class QuestionPack(Model):
        id = StringType()
        questions = ListType(ModelType(Question))

    question_pack = QuestionPack({
        "id": "1",
        "questions": [
            {
                "id": "1",
            },
            {
                "id": "2",
                "resources": {
                    "pictures": [],
                }
            },
            {
                "id": "3",
                "resources": {
                    "pictures": [{
                        "url": "http://www.mbl.is/djok",
                    }]
                }
            },
        ]
    })

    assert question_pack.questions[0].resources is None
    assert question_pack.questions[1].resources["pictures"] == []

    resource = QuestionResource({"url": "http://www.mbl.is/djok"})
    assert question_pack.questions[2].resources["pictures"][0] == resource


def test_validation_with_size_limits():
    class User(Model):
        name = StringType()

    class Card(Model):
        users = ListType(ModelType(User), min_size=1, max_size=2, required=True)

    with pytest.raises(DataError) as exception:
        c = Card({"users": None})
        c.validate()

    assert exception.value.messages['users'] == [u'This field is required.']

    with pytest.raises(DataError) as exception:
        c = Card({"users": []})
        c.validate()

    assert exception.value.messages['users'] == [u'Please provide at least 1 item.']

    with pytest.raises(DataError) as exception:
        c = Card({"users": [User(), User(), User()]})
        c.validate()

    assert exception.value.messages['users'] == [u'Please provide no more than 2 items.']


def test_list_field_required():
    class User(Model):
        ids = ListType(StringType(required=True))

    c = User({
        "ids": []
    })

    c.ids = []
    c.validate()

    c.ids = [1]
    c.validate()

    c.ids = [None]
    with pytest.raises(DataError):
        c.validate()


def test_list_field_convert():
    class User(Model):
        ids = ListType(IntType)

    c = User({'ids': ["1", "2"]})

    assert c.ids == [1, 2]


def test_list_coercion():
    field = ListType(StringType)
    assert field(('foobar',)) == ['foobar']
    assert field(set(('foobar',))) == ['foobar']
    with pytest.raises(ConversionError):
        field({1: 'bar', 2: 'baz', 0: 'foo'})
    with pytest.raises(ConversionError):
        field('foobar')
    with pytest.raises(ConversionError):
        field(None)


def test_list_model_field():
    class User(Model):
        name = StringType()

    class Card(Model):
        users = ListType(ModelType(User), min_size=1, required=True)

    data = {'users': [{'name': u'Doggy'}]}
    c = Card(data)

    c.users = None
    with pytest.raises(DataError) as exception:
        c.validate()

    errors = exception.value.messages
    assert errors['users'] == [u'This field is required.']


def test_list_model_field_exception_with_full_message():
    class User(Model):
        name = StringType(max_length=1)

    class Group(Model):
        users = ListType(ModelType(User))

    g = Group({'users': [{'name': "ToLongName"}]})

    with pytest.raises(DataError) as exception:
        g.validate()

    def _loc(text):
        return _(text, lazy=False)

    assert exception.value.messages == {'users': {0: {'name': [_loc('String value is too long.')]}}}


def test_compound_fields():
    comments = ListType(ListType, compound_field=StringType)

    assert isinstance(comments.field, ListType)


def test_mock_object():
    assert type(ListType(IntType, required=True).mock()) is list

    assert ListType(
        StringType, min_size=0, max_size=0, required=True,
    ).mock() == []

    with pytest.raises(MockCreationError) as exception:
        ListType(IntType, min_size=10, max_size=1, required=True).mock()


def test_mock_object_with_model_type():
    class User(Model):
        name = StringType(required=True)
        age = IntType(required=True)

    assert isinstance(
        ListType(ModelType(User), min_size=1, required=True).mock()[-1],
        User
    )


def test_issue_453_list_model_field_recursive_import():
    class User(Model):
        name = StringType()

    class Card(Model):
        id = IntType(default=1)
        users = ListType(ModelType(User), min_size=1, required=True)

    card = Card()
    card.users = [
        User({'name': 'foo'}),
        User({'name': 'bar'}),
        User({'name': 'baz'}),
    ]
    card.import_data({'id': '2', 'users': [User({'name': 'xyz'})]},
        recursive=True)
    assert card.serialize() == {'id': 2, 'users': [{'name': 'xyz'}]}
