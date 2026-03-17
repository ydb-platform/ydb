import os.path
import pytest

from flex.core import load

import yatest.common


@pytest.mark.parametrize(
    'path',
    (
        pytest.param('example_schemas/uber.yaml', marks=pytest.mark.xfail),
        pytest.param('example_schemas/petstore.yaml', marks=pytest.mark.xfail),
        'example_schemas/petstore-expanded.yaml',
        'example_schemas/api-with-examples.yaml',
    )
)
def test_load_and_parse_schema(path):
    load(os.path.join(yatest.common.test_source_path('core'), path))
