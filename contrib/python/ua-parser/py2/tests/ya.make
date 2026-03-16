PY2TEST()

SIZE(MEDIUM)

FORK_SUBTESTS()

NO_LINT()

PEERDIR(
    contrib/python/PyYAML
    contrib/python/six
    contrib/python/ua-parser
)

SRCDIR(contrib/python/ua-parser/py2/ua_parser)

DATA(
    sbr://3354299037  # test_data/
)

TEST_SRCS(
    user_agent_parser_test.py
)

END()
