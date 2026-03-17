PY3TEST()

SIZE(MEDIUM)

FORK_SUBTESTS()

NO_LINT()

PEERDIR(
    contrib/python/PyYAML
    contrib/python/google-re2
    contrib/python/ua-parser
)

ALL_PYTEST_SRCS()

DATA(
    sbr://3354299037  # test_data/
)

END()
