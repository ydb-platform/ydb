
# from os import path
from typing import List, Sequence

import yatest

import pytest

selected_items: List[pytest.Function] = []


def pytest_collection_finish(session: pytest.Session):
    global selected_items
    selected_items = session.items
