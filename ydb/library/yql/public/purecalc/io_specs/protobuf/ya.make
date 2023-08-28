LIBRARY()

PEERDIR(
    ydb/library/yql/public/purecalc/common
    ydb/library/yql/public/purecalc/io_specs/protobuf_raw
)

SRCS(
    spec.cpp
    proto_variant.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
