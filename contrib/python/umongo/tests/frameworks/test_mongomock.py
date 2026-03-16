import datetime as dt

import pytest

from ..common import TEST_DB

DEP_ERROR = 'Missing mongomock'

try:
    from mongomock import MongoClient
except ImportError:
    dep_error = True
else:
    dep_error = False


if not dep_error:  # Make sure the module is valid by importing it
    from umongo.frameworks import mongomock  # noqa


def make_db():
    return MongoClient()[TEST_DB]


@pytest.fixture
def db():
    return make_db()


# MongoMockBuilder is 100% based on PyMongoBuilder so no need for really heavy tests
@pytest.mark.skipif(dep_error, reason=DEP_ERROR)
def test_mongomock(classroom_model):
    Student = classroom_model.Student
    john = Student(name='John Doe', birthday=dt.datetime(1995, 12, 12))
    john.commit()
    assert john.to_mongo() == {
        '_id': john.id,
        'name': 'John Doe',
        'birthday': dt.datetime(1995, 12, 12)
    }
    john2 = Student.find_one(john.id)
    assert john2._data == john._data
    johns = Student.find()
    assert list(johns) == [john]
