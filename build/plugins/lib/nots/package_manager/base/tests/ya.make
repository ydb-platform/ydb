SUBSCRIBER(g:frontend-build-platform)

PY3TEST()

TEST_SRCS(
    package_json.py
    utils.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager/base
)

END()
