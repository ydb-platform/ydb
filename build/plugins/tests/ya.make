PY3TEST()

OWNER(g:ymake)

TEST_SRCS(
    test_common.py
    test_requirements.py
)

PEERDIR(
    build/plugins
    build/plugins/lib/proxy
    build/plugins/lib/test_const/proxy
    build/plugins/tests/fake_ymake
)

END()
