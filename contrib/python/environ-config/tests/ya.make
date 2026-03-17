PY3TEST()

PEERDIR(
    contrib/python/environ-config
)

TEST_SRCS(
    test_class_generate_help.py
    test_class_to_config.py
    test_environ_config.py
    test_secrets.py
)

NO_LINT()

END()
