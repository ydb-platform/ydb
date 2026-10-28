UNITTEST()

SRCS(
    dynamic_group_line_ut.cpp
    compressed_line_storage_ut.cpp
    group_line_frontend_ut.cpp
    inmemory_metrics_ut.cpp
)

PEERDIR(
    ydb/library/actors/metrics
)

END()
