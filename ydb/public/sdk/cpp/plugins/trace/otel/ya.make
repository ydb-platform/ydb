LIBRARY()

SRCS(
    trace.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/trace
    contrib/libs/opentelemetry-cpp/api
)

END()
