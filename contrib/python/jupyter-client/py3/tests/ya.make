PY3TEST()

PEERDIR(
    contrib/python/anyio
    contrib/python/ipython
    contrib/python/ipykernel
    contrib/python/jupyter-client
    contrib/python/pyzmq
    contrib/python/pytest-asyncio
    contrib/python/pytest-timeout
    contrib/python/pytest-jupyter
    contrib/python/tornado
)

DEPENDS(
    contrib/python/jupyter-client/py3/bin/jupyter_kernel
    contrib/python/jupyter-client/py3/bin/jupyter_kernelspec
)

PY_SRCS(
    NAMESPACE tests
    problemkernel.py
    signalkernel.py
    utils.py
)

TEST_SRCS(
    __init__.py
    conftest.py
    test_adapter.py
    test_client.py
    test_connect.py
    test_consoleapp.py
    test_jsonutil.py
    test_kernelapp.py
    test_kernelmanager.py
    test_kernelspec.py
    test_kernelspecapp.py
    test_localinterfaces.py
    test_manager.py
    test_multikernelmanager.py
    test_provisioning.py
    test_public_api.py
    test_restarter.py
    test_session.py
    test_ssh.py
)

FORK_SUBTESTS()

# 201 - total number of tests
# some tests are broking when launching in one process
SPLIT_FACTOR(201)

NO_LINT()

END()
