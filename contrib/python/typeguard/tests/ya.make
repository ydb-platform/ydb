PY3TEST()

PEERDIR(
    contrib/python/typeguard
    contrib/python/typing-extensions
)

TEST_SRCS(
    conftest.py
    dummymodule.py
    test_importhook.py
    test_typeguard.py
    test_typeguard_py36.py
)

DATA(
    arcadia/contrib/python/typeguard/tests
)

NO_LINT()

END()
