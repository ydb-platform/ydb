LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    batch_request.cpp
    client.cpp
    client_method_options.cpp
    common.cpp
    config.cpp
    cypress.cpp
    errors.cpp
    format.cpp
    helpers.cpp
    job_counters.cpp
    job_statistics.cpp
    io.cpp
    operation.cpp
    protobuf_format.cpp
    serialize.cpp
    skiff_row.cpp
    tvm.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/type_info
    library/cpp/threading/future
    library/cpp/yson/node
    yt/cpp/mapreduce/interface/logging
    yt/yt_proto/yt/formats
    yt/yt/library/tvm
    yt/yt/core
)

GENERATE_ENUM_SERIALIZATION(client_method_options.h)
GENERATE_ENUM_SERIALIZATION(client.h)
GENERATE_ENUM_SERIALIZATION(common.h)
GENERATE_ENUM_SERIALIZATION(config.h)
GENERATE_ENUM_SERIALIZATION(cypress.h)
GENERATE_ENUM_SERIALIZATION(job_counters.h)
GENERATE_ENUM_SERIALIZATION(job_statistics.h)
GENERATE_ENUM_SERIALIZATION(operation.h)
GENERATE_ENUM_SERIALIZATION(protobuf_format.h)

END()

RECURSE_FOR_TESTS(
    ut
)
