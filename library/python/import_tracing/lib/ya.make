PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    event.py
    import_tracer.py
    constants.py
    regulator.py
    converters/base.py
    converters/chrometrace.py
    converters/raw.py
)

PEERDIR_FROM_POLICY(
    ALLOW
    library/python/import_tracing/constructor
    library/python/testing/import_test
)

PEERDIR_FROM_POLICY(DENY .*)

END()
