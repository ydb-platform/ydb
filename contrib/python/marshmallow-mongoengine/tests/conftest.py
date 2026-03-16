import mongoengine as me
import pytest

TEST_DB = 'marshmallow_mongoengine-test'


@pytest.fixture
def mongoengine_connection(mongodb_server):
    db = me.connect(TEST_DB, host=mongodb_server.uri)
    db.drop_database(TEST_DB)
    yield db
    db.drop_database(TEST_DB)
    db.close()
