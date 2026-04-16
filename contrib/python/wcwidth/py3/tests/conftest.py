"""Pytest configuration and fixtures."""
# 3rd party
import pytest

try:
    # 3rd party
    from pytest_codspeed import BenchmarkFixture  # noqa: F401  pylint:disable=unused-import
except ImportError:
    # Provide a no-op benchmark fixture when pytest-codspeed is not installed
    @pytest.fixture
    def benchmark():
        """No-op benchmark fixture for environments without pytest-codspeed."""
        def _passthrough(func, *args, **kwargs):
            return func(*args, **kwargs)
        return _passthrough
