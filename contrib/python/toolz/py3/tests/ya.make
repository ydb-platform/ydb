PY3TEST()

PEERDIR(
    contrib/python/toolz
)

SRCDIR(contrib/python/toolz/py3/toolz/tests)

TEST_SRCS(
    test_compatibility.py
    test_curried.py
    test_curried_doctests.py
    test_dicttoolz.py
    test_functoolz.py
    test_inspect_args.py
    test_itertoolz.py
    test_recipes.py
    test_serialization.py
    test_signatures.py
    test_tlz.py
    test_utils.py
)

NO_LINT()

END()
