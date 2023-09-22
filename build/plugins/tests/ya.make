PY2TEST()

OWNER(g:ymake)

TEST_SRCS(
    test_code_generator.py
    test_common.py
    test_requirements.py
    test_ssqls.py
)

PEERDIR(
    build/plugins
    build/plugins/lib/proxy
    build/plugins/lib/test_const/proxy
    build/plugins/tests/fake_ymake
)

END()
