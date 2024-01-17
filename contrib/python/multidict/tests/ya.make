PY3TEST()

PEERDIR(
    contrib/python/multidict
)

TEST_SRCS(
    conftest.py
    test_abc.py
    test_copy.py
    test_guard.py
    test_istr.py
    test_multidict.py
    test_mutable_multidict.py
    test_mypy.py
    test_pickle.py
    test_types.py
    test_update.py
    test_version.py
)

DATA(
    arcadia/contrib/python/multidict/tests
)


NO_LINT()

END()
