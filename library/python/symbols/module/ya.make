PY23_LIBRARY()

ENABLE(USE_LIGHT_PY2CC)

NO_PYTHON_INCLUDES()

SRCS(
    module.cpp
)

PY_REGISTER(
    library.python.symbols.module.syms
)

PY_SRCS(
    __init__.py
)

END()
