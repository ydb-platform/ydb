from collections.abc import Generator

import pytest

from pyroute2 import NDB


@pytest.fixture
def ndb(nsname: str) -> Generator[NDB]:
    '''NDB instance.

    * **Name**: ndb
    * **Scope**: function
    * **Depends**: nsname

    Yield `NDB` instance running in the test network namespace.
    '''
    with NDB(sources=[{'target': 'localhost', 'netns': nsname}]) as ndb:
        yield ndb
