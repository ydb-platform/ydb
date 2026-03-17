"""Module containing tests for requests_toolbelt.threaded API."""

try:
    from unittest import mock
except ImportError:
    import mock
import pytest

from requests_toolbelt._compat import queue
from requests_toolbelt import threaded


def test_creates_a_pool_for_the_user():
    """Assert a Pool object is used correctly and as we expect.

    This just ensures that we're not jumping through any extra hoops with our
    internal usage of a Pool object.
    """
    mocked_pool = mock.Mock(spec=['join_all', 'responses', 'exceptions'])
    with mock.patch('requests_toolbelt.threaded.pool.Pool') as Pool:
        Pool.return_value = mocked_pool
        threaded.map([{}, {}])

    assert Pool.called is True
    _, kwargs = Pool.call_args
    assert 'job_queue' in kwargs
    assert isinstance(kwargs['job_queue'], queue.Queue)
    mocked_pool.join_all.assert_called_once_with()
    mocked_pool.responses.assert_called_once_with()
    mocked_pool.exceptions.assert_called_once_with()


def test_raises_a_value_error_for_non_dictionaries():
    """Exercise our lazy valdation."""
    with pytest.raises(ValueError):
        threaded.map([[], []])


def test_raises_a_value_error_for_falsey_requests():
    """Assert that the requests param is truthy."""
    with pytest.raises(ValueError):
        threaded.map([])

    with pytest.raises(ValueError):
        threaded.map(None)


def test_passes_on_kwargs():
    """Verify that we pass on kwargs to the Pool constructor."""
    mocked_pool = mock.Mock(spec=['join_all', 'responses', 'exceptions'])
    with mock.patch('requests_toolbelt.threaded.pool.Pool') as Pool:
        Pool.return_value = mocked_pool
        threaded.map([{}, {}], num_processes=1000,
                     initializer=test_passes_on_kwargs)

    _, kwargs = Pool.call_args
    assert 'job_queue' in kwargs
    assert 'num_processes' in kwargs
    assert 'initializer' in kwargs

    assert kwargs['num_processes'] == 1000
    assert kwargs['initializer'] == test_passes_on_kwargs
