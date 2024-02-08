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

END()
