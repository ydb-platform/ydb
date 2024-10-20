LIBRARY()

YQL_ABI_VERSION(
    2
    27
    0
)

PEERDIR(
    library/cpp/containers/absl_flat_hash
    library/cpp/json
    ydb/library/conclusion
    ydb/library/yql/minikql/dom
    contrib/libs/simdjson
)

SRCS(
    format.cpp
    read.cpp
    write.cpp
)

GENERATE_ENUM_SERIALIZATION(format.h)

END()

RECURSE_FOR_TESTS(
    ut
    ut_benchmark
)
