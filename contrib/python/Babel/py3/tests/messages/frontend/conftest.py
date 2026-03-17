import pathlib
import time

import pytest


@pytest.fixture
def pot_file(tmp_path) -> pathlib.Path:
    return tmp_path / f'po-{time.time()}.pot'
