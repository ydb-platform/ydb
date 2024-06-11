PY3TEST()

SUBSCRIBER(g:frontend_build_platform)

TEST_SRCS(
    test_ts_config.py
    test_ts_glob.py
)

PEERDIR(
    build/plugins/lib/nots/typescript
)

END()
