PY3TEST()

PEERDIR(
    contrib/python/Parsley
    contrib/python/Twisted
)

NO_LINT()

SRCDIR(contrib/python/Parsley/py3)

TEST_SRCS(
    ometa/test/test_builder.py
    ometa/test/test_protocol.py
    ometa/test/test_pymeta.py
    ometa/test/test_runtime.py
    ometa/test/test_tube.py
    ometa/test/test_vm_builder.py
    terml/test/test_quasiterm.py
    terml/test/test_terml.py
    test_parsley.py
)

END()
