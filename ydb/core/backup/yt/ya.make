LIBRARY()
SRCS(format.cpp first_key.cpp)
PEERDIR(
    contrib/libs/zlib
    library/cpp/yson/node
    ydb/core/protos
    ydb/core/scheme
    ydb/public/api/protos
    yt/yt/core
)
END()
RECURSE_FOR_TESTS(ut)
