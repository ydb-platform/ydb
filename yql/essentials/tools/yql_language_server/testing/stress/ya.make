PY3TEST()

DEPENDS(
    yql/essentials/tools/yql_language_server
)

PEERDIR(
    yql/essentials/tools/yql_language_server/lsp/testing
)

TEST_SRCS(
    lsp_random.py
    smoke.py
)

END()
