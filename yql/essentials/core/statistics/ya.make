LIBRARY()

SRCS(
    yql_statistics.cpp
)

PEERDIR(
    ydb/core/kqp/opt/cbo
    yql/essentials/core/minsketch
    yql/essentials/core/histogram
    library/cpp/json
    library/cpp/string_utils/base64
)

END()
