PY3TEST()

PEERDIR(
    contrib/python/Flask-Humanize
    contrib/python/pytest
    contrib/python/pytest-flask
)

TEST_SRCS(
    conftest.py
    test_humanize.py
)

ENV(TZ=MSK)

NO_LINT()

END()
