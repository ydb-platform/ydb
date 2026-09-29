LIBRARY()

SRCS(
    compile_cache.h
    compile_cache.cpp
)

PEERDIR(
    library/cpp/json
    ydb/library/actors/core
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
    ydb/library/ydb_issue
)

YQL_LAST_ABI_VERSION()

END()
