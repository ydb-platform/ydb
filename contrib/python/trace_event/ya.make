PY3_LIBRARY()

# original name: py_trace_event at https://chromium.googlesource.com/external/py_trace_event/

LICENSE(BSD-3-Clause)

VERSION(862b032+dev)

PY_SRCS(
    TOP_LEVEL
    trace_event.py
    trace_event_impl/__init__.py
    trace_event_impl/decorators.py
    trace_event_impl/log.py
    trace_event_impl/parsed_trace_events.py
    trace_event_impl/multiprocessing_shim.py
    trace_event_impl/multiprocessing_context_shim.py
    trace_event_impl/subprocess_shim.py
)

NO_LINT()

END()

RECURSE_FOR_TESTS(
    tests
)
