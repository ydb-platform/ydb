PY23_TEST()

OWNER(g:frontend-build-platform)

TEST_SRCS(
    test_lockfile.py
    test_workspace.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager/base
    build/plugins/lib/nots/package_manager/pnpm
)

END()
