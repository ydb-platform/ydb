PY3TEST()

PEERDIR(
    contrib/python/Jinja2
    contrib/python/trio
)

DATA(
    arcadia/contrib/python/Jinja2/py3/tests
)

PY_SRCS(
    TOP_LEVEL
    res/__init__.py
)

RESOURCE_FILES(
    PREFIX contrib/python/Jinja2/py3/tests/
    res/templates/broken.html
    res/templates/foo/test.html
    res/templates/mojibake.txt
    res/templates/syntaxerror.html
    res/templates/test.html
    res/templates2/foo
)

ALL_PYTEST_SRCS()

NO_LINT()

END()
