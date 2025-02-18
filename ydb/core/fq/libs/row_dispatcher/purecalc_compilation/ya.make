LIBRARY()

SRCS(
    compile_service.cpp
)

PEERDIR(
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/fq/libs/row_dispatcher/format_handler/common
    ydb/core/fq/libs/row_dispatcher/purecalc_no_pg_wrapper

    ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()
