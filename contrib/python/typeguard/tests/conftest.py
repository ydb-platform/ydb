import random
import re
import string
import sys
import typing
from itertools import count
from pathlib import Path

import pytest
import typing_extensions

version_re = re.compile(r"_py(\d)(\d)\.py$")
pytest_plugins = ["pytester"]


def pytest_ignore_collect(
    collection_path: Path, config: pytest.Config
) -> typing.Optional[bool]:
    match = version_re.search(collection_path.name)
    if match:
        version = tuple(int(x) for x in match.groups())
        if sys.version_info < version:
            return True

    return None


@pytest.fixture
def sample_set() -> set:
    # Create a set which, when iterated, returns "bb" as the first item
    for num in count():
        letter = random.choice(string.ascii_lowercase)
        dummy_set = {letter, num}
        if next(iter(dummy_set)) == letter:
            return dummy_set


@pytest.fixture(
    params=[
        pytest.param(typing, id="typing"),
        pytest.param(typing_extensions, id="typing_extensions"),
    ]
)
def typing_provider(request):
    return request.param
