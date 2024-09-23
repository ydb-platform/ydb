SUBSCRIBER(g:frontend_build_platform)

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
