PY23_TEST()

OWNER(dankolesnikov)

TEST_SRCS(
    lockfile.py
    workspace.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager/base
    build/plugins/lib/nots/package_manager/pnpm
)

END()
