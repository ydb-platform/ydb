UNITTEST()

SRCS(
    yql_yt_gc_service_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/gc_service/impl
    yt/yql/providers/yt/fmr/table_data_service/local/impl
)

YQL_LAST_ABI_VERSION()

END()
