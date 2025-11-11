LIBRARY()

SRCS(
    executor.cpp
    generator.cpp
    job.cpp
    statistics.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/json/writer
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/iam
)

END()
