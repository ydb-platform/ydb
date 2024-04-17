SUBSCRIBER(g:frontend-build-platform)

PY3TEST()

TEST_SRCS(
    test_lockfile.py
    test_workspace.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager/base
    build/plugins/lib/nots/package_manager/pnpm
)

END()
