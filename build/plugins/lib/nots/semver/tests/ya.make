PY3TEST()

OWNER(g:frontend-build-platform)

PEERDIR(
    build/plugins/lib/nots/semver
)

TEST_SRCS(
    test_version_range.py
    test_version.py
)

END()
