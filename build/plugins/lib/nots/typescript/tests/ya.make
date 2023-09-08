PY23_TEST()

OWNER(g:frontend-build-platform)

TEST_SRCS(
    ts_config.py
    test_ts_glob.py
)

PEERDIR(
    build/plugins/lib/nots/typescript
)

END()
