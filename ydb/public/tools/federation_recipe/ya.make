PY3_PROGRAM(federation_recipe)

SRCDIR(
    ydb/public/tools/federation_recipe
)

PY_SRCS(
    __main__.py
    cm_requests.py
)

PEERDIR(
    library/python/port_manager
    library/python/testing/recipe
    library/python/testing/yatest_common
    ydb/public/tools/lib/cmds
    ydb/public/tools/federation_recipe/proto
    ydb/tests/library
    contrib/python/grpcio
    contrib/python/ydb/py3
)

DEPENDS(
    ydb/public/tools/federation_recipe/bin
)

END()
