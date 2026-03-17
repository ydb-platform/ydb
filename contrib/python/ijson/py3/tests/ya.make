PY3TEST()

PEERDIR(
    contrib/python/ijson
)

TEST_SRCS(
    conftest.py
    __init__.py
    support/_async_common.py
    support/async_.py
    support/async_types_coroutines.py
    support/coroutines.py
    support/generators.py
    support/__init__.py
    test_base.py
    test_basic_parse.py
    # test_benchmark.py
    # test_dump.py
    test_generators.py
    test_items.py
    test_kvitems.py
    test_memleaks.py
    test_misc.py
    test_parse.py
    test_pulling.py
    test_subinterpreter.py
)

NO_LINT()

END()
