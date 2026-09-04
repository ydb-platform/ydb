PY3TEST()

DEPENDS(
    yql/essentials/tools/yql_language_server
)

DATA(
    arcadia/yql/essentials/tools/yql_language_server/testing/functional/traces
)

PEERDIR(
    yql/essentials/tools/yql_language_server/lsp/testing
)

TEST_SRCS(
    replay.py
)

FORK_TESTS()
FORK_SUBTESTS()

STYLE_JSON(
    DIRS_RECURSE traces
)

END()
