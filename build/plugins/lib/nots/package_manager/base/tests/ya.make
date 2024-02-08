PY23_TEST()

OWNER(g:frontend-build-platform)

TEST_SRCS(
    package_json.py
    utils.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager/base
)

END()
