PY3TEST()

SIZE(MEDIUM)
FORK_SUBTESTS()
# Each current scenario gets its own timeout, including the BSC warmup.
SPLIT_FACTOR(32)

TEST_SRCS(test_browser.py)

DEPENDS(ydb/apps/bridge_skipper)

PEERDIR(
    build/platform/nodejs/24.22.1
    ydb/apps/bridge_skipper/acceptance/browser/resources
    library/python/testing/yatest_common
    ydb/apps/bridge_skipper/acceptance/lib
)

DATA(
    arcadia/ydb/apps/bridge_skipper/acceptance/browser/browser.cjs
    arcadia/ydb/apps/bridge_skipper/acceptance/browser/resources/registration.json
)

# Add the two approved platform resource IDs here after preparation and upload.
# The test fails explicitly while a matching bundle is not registered. It never
# installs npm packages, downloads a browser, or uses a host-installed browser.

END()
