LIBRARY()

SRCS(
    acl.cpp
    action.cpp
    counters.cpp
    dlq_helpers.cpp
    events_writer.cpp
    helpers.cpp
    probes.cpp
    queue_attributes.cpp
    queue_id.cpp
    run_query.cpp
    secure_protobuf_printer.cpp
    events_writer_iface.h
)

GENERATE_ENUM_SERIALIZATION(query_id.h)

GENERATE_ENUM_SERIALIZATION(cloud_enums.h)

PEERDIR(
    contrib/libs/openssl
    library/cpp/cgiparam
    library/cpp/ipmath
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
    library/cpp/scheme
    library/cpp/string_utils/base64
    library/cpp/unified_agent_client
    ydb/core/base
    ydb/core/protos
    ydb/core/ymq/proto
    ydb/core/kqp/common
    ydb/library/aclib
    ydb/library/http_proxy/authorization
    ydb/library/http_proxy/error
    ydb/library/protobuf_printer
    ydb/public/lib/scheme_types
)

END()

RECURSE_FOR_TESTS(
    ut
)
