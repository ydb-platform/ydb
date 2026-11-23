YQL_LIBRARY()

SRCS(
    compile_cache.h
    compile_cache.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/kqp/runtime/common
    ydb/core/sys_view/common
    ydb/library/actors/core
    ydb/library/ydb_issue
)

END()
