import pytest

from .test_base import JSON, JSON_EVENTS


@pytest.mark.pull_only
def test_string_stream(adaptor):
    with pytest.deprecated_call():
        events = adaptor.basic_parse(JSON.decode('utf-8'))
    assert JSON_EVENTS == events


@pytest.mark.pull_only
@pytest.mark.parametrize("buf_size", (2 ** exp for exp in range(0, 13, 2)))
def test_different_buf_sizes(adaptor, buf_size):
    assert JSON_EVENTS == adaptor.basic_parse(JSON, buf_size=buf_size)
