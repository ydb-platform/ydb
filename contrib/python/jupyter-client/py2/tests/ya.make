PY2TEST()

PEERDIR(
    contrib/python/ipython
    contrib/python/ipykernel
    contrib/python/jupyter-client
    contrib/python/msgpack
    contrib/python/pyzmq
    contrib/python/mock
)

DEPENDS(
    contrib/python/jupyter-client/py2/bin/jupyter_kernel
    contrib/python/jupyter-client/py2/bin/jupyter_kernelspec
)

SRCDIR(contrib/python/jupyter-client/py2)

TEST_SRCS(
    jupyter_client/tests/__init__.py
    jupyter_client/tests/test_adapter.py
    jupyter_client/tests/test_client.py
    jupyter_client/tests/test_connect.py
    jupyter_client/tests/test_jsonutil.py
    jupyter_client/tests/test_kernelapp.py
    jupyter_client/tests/test_kernelmanager.py
    jupyter_client/tests/test_kernelspec.py
    jupyter_client/tests/test_localinterfaces.py
    jupyter_client/tests/test_multikernelmanager.py
    jupyter_client/tests/test_public_api.py
    jupyter_client/tests/test_session.py
    jupyter_client/tests/utils.py
)

FORK_SUBTESTS()

# 112 - total number of tests
# some tests are broking when launching in one process
SPLIT_FACTOR(111)

NO_LINT()

END()
