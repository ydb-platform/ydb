PY3TEST()

PEERDIR(
    contrib/python/user-agents
)

SRCDIR(contrib/python/user-agents/py3/user_agents)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
