LIBRARY()

SRCS(
    signals.cpp
    signals.h
    utils.cpp
    utils.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/logger/global
    library/cpp/protobuf/json
    library/cpp/json/yson
    ydb/library/yql/utils/log
    ydb/library/yql/utils/backtrace
    ydb/library/yql/providers/yt/lib/log
)

END()
