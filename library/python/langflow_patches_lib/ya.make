PY3_LIBRARY()

PY_SRCS(
    __init__.py
    utils.py
    components_blacklist.py
    code_execution_guard.py
)

END()

RECURSE(
    index_builder
)
