PY3TEST()

PEERDIR(
    contrib/python/Babel
    contrib/python/Jinja2
    contrib/python/freezegun
    contrib/python/tzdata
)

DATA(
    arcadia/contrib/python/Babel/py3
)

FORK_SUBTESTS()

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
