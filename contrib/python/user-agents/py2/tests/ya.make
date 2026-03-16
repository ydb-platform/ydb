PY2TEST()

PEERDIR(
    contrib/python/user-agents
)

SRCDIR(contrib/python/user-agents/py2/user_agents)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
