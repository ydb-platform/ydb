SUBSCRIBER(g:frontend_build_platform)

PY3TEST()

TEST_SRCS(
    test_common_config.py
    test_lockfile.py
    test_package_json.py
    test_utils.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager
)

DATA(
    arcadia/devtools/frontend_build_platform/nots/constants/src/tier0-settings.yaml
)

END()
