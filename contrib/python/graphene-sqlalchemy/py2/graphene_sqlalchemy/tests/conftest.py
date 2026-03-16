import pytest
from sqlalchemy import create_engine


@pytest.fixture(scope="session")
def db():
    return create_engine("sqlite:///test_sqlalchemy.sqlite3")
