PY2TEST()

PEERDIR(
    contrib/python/greenlet
)

SRCDIR(
    contrib/python/greenlet/py2/greenlet/tests
)

TEST_SRCS(
    __init__.py
    test_contextvars.py
    test_cpp.py
    test_extension_interface.py
    test_gc.py
    test_generator.py
    test_generator_nested.py
    test_greenlet.py
    test_leaks.py
    test_stack_saved.py
    test_throw.py
    test_tracing.py
    test_weakref.py
)

SRCS(
    _test_extension.c
    _test_extension_cpp.cpp
)

PY_REGISTER(__tests__._test_extension)

PY_REGISTER(__tests__._test_extension_cpp)

NO_COMPILER_WARNINGS()

NO_LINT()

END()
