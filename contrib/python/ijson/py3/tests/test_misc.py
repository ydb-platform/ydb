import importlib.util
import io

import pytest

from ijson import common

from .test_base import JSON, JSON_EVENTS, JSON_PARSE_EVENTS, JSON_OBJECT,\
    JSON_KVITEMS


class TestMisc:
    """Miscellaneous unit tests"""

    def test_common_number_is_deprecated(self):
        with pytest.deprecated_call():
            common.number("1")

    def test_yajl2_c_loadable(self):
        spec = importlib.util.find_spec("ijson.backends._yajl2")
        if spec is None:
            pytest.skip("yajl2_c is not built")
        importlib.util.module_from_spec(spec)


class TestMainEntryPoints:
    """Tests that main API entry points work against different types of inputs automatically"""

    def _assert_invalid_type(self, routine, *args, **kwargs):
        # Functions are not valid inputs
        with pytest.raises(ValueError):
            routine(lambda _: JSON, *args, **kwargs)

    def _assert_bytes(self, expected_results, routine, *args, **kwargs):
        results = list(routine(JSON, *args, **kwargs))
        assert expected_results == results

    def _assert_str(self, expected_results, routine, *args, **kwargs):
        with pytest.deprecated_call():
            results = list(routine(JSON.decode("utf-8"), *args, **kwargs))

    def _assert_file(self, expected_results, routine, *args, **kwargs):
        results = list(routine(io.BytesIO(JSON), *args, **kwargs))
        assert expected_results == results

    def _assert_async_file(self, expected_results, routine, *args, **kwargs):
        from .support.async_ import get_all
        results = get_all(routine, JSON, *args, **kwargs)
        expected_results == results

    def _assert_async_types_coroutine(self, expected_results, routine, *args, **kwargs):
        from .support.async_types_coroutines import get_all
        results = get_all(routine, JSON, *args, **kwargs)
        assert expected_results == results

    def _assert_events(self, expected_results, previous_routine, routine, *args, **kwargs):
        events = previous_routine(io.BytesIO(JSON))
        # Using a different generator to make the point that we can chain
        # user-provided code
        def event_yielder():
            for evt in events:
                yield evt
        results = list(routine(event_yielder(), *args, **kwargs))
        assert expected_results == results

    def _assert_entry_point(self, expected_results, previous_routine, routine,
                            *args, **kwargs):
        self._assert_invalid_type(routine, *args, **kwargs)
        self._assert_bytes(expected_results, routine, *args, **kwargs)
        self._assert_str(expected_results, routine, *args, **kwargs)
        self._assert_file(expected_results, routine, *args, **kwargs)
        self._assert_async_file(expected_results, routine, *args, **kwargs)
        self._assert_async_types_coroutine(expected_results, routine, *args, **kwargs)
        if previous_routine:
            self._assert_events(expected_results, previous_routine, routine, *args, **kwargs)

    def test_rich_basic_parse(self, backend):
        self._assert_entry_point(JSON_EVENTS, None, backend.basic_parse)

    def test_rich_parse(self, backend):
        self._assert_entry_point(JSON_PARSE_EVENTS, backend.basic_parse, backend.parse)

    def test_rich_items(self, backend):
        self._assert_entry_point([JSON_OBJECT], backend.parse, backend.items, '')

    def test_rich_kvitems(self, backend):
        self._assert_entry_point(JSON_KVITEMS, backend.parse, backend.kvitems, 'docs.item')