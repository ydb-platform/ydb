PY3TEST()

SIZE(MEDIUM)
FORK_SUBTESTS()
SPLIT_FACTOR(2)

TEST_SRCS(test_bundle_provenance.py)

PEERDIR(
    library/python/testing/yatest_common
    ydb/apps/bridge_skipper/acceptance/lib
)

DATA(
    arcadia/ydb/apps/bridge_skipper/acceptance/browser/test_browser.py
    arcadia/ydb/apps/bridge_skipper/acceptance/browser/prepare_bundle.py
    arcadia/ydb/apps/bridge_skipper/acceptance/browser/resources/upstream.json
)

END()
