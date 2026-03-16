import pytest
from pytest_mongodb.config import ArcConfig
from pytest_mongodb.server import MongoDBServer


@pytest.fixture(scope="session")
def mongodb_server():
    mongodb_config = ArcConfig()
    server = MongoDBServer(mongodb_config)
    server.start_local()
    yield mongodb_config
    server.stop_local()
