PY3TEST()

SUBSCRIBER(g:frontend_build_platform)

ALL_RESOURCE_FILES(
    json
    test-data/tsconfig-real-files
)

TEST_SRCS(
    test_ts_config.py
    test_ts_glob.py
)

PEERDIR(
    library/python/resource
    build/plugins/lib/nots/typescript
)

END()
