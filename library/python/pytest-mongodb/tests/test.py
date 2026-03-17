from pymongo import MongoClient


def test_started(mongodb_server):
    client = MongoClient(host=mongodb_server.uri)
    assert client.admin.command('ismaster')


def test_still_working(mongodb_server):
    client = MongoClient(host=mongodb_server.uri)
    assert client.admin.command('ismaster')


def test_mongodb_uri(mongodb_server):
    client = MongoClient(mongodb_server.uri)
    assert client.admin.command('ismaster')
