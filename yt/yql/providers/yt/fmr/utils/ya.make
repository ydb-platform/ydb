LIBRARY()

SRCS(
    parse_records.cpp
    table_data_service_key.cpp
)

PEERDIR(
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/yt_service/impl
    yt/yql/providers/yt/codec
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
