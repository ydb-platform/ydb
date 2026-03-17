PY3TEST()

PEERDIR (
    contrib/python/defusedxml
    contrib/python/lxml
)

DATA(
    arcadia/contrib/python/defusedxml/py3/xmltestdata
)

SRCDIR(contrib/python/defusedxml/py3)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
