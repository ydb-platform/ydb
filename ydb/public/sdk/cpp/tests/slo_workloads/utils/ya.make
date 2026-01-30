LIBRARY()

SRCS(
    executor.cpp
    generator.cpp
    job.cpp
    metrics.cpp
    statistics.cpp
    utils.cpp
)

IF (REF)
    CFLAGS(-DREF=${REF})
ENDIF()

PEERDIR(
    contrib/libs/opentelemetry-cpp
    library/cpp/json/writer
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/iam
)

END()
