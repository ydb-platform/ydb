LIBRARY()

SRCS(
    compile_service.cpp
)

PEERDIR(
    ydb/core/fq/libs/row_dispatcher/events

    ydb/library/actors/core

    ydb/library/yql/public/purecalc/common/no_pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
