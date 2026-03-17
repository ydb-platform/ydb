PY2TEST()

PEERDIR (
    contrib/python/defusedxml
    contrib/python/lxml
)

DATA(
    arcadia/contrib/python/defusedxml/py2/xmltestdata
)

SRCDIR(contrib/python/defusedxml/py2)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
