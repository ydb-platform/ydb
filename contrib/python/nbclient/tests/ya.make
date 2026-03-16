PY3TEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/python/nbclient
    contrib/python/xmltodict
    contrib/python/ipython
    contrib/python/ipywidgets
    contrib/python/flaky
    contrib/python/nbconvert
    contrib/python/pytest-asyncio
    contrib/python/tornado
    contrib/python/ipykernel
)

TEST_SRCS(
    __init__.py
    base.py
    conftest.py
    fake_kernelmanager.py
    test_client.py
    test_util.py
)

DATA(
    arcadia/contrib/python/nbclient/tests/files
)

ENV(PYTHONWARNINGS=ignore)

FORK_SUBTESTS()

NO_LINT()

END()
