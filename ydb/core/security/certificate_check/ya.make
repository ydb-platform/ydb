LIBRARY()

SRCS(
    cert_auth_utils.cpp
    dynamic_node_auth_processor.cpp
)


PEERDIR(
    # ydb/library/actors/core
    # ydb/library/actors/http
    # ydb/library/grpc/actor_client
    # library/cpp/monlib/service/pages
    # library/cpp/openssl/io
    # ydb/core/base
    ydb/core/protos
    # ydb/library/aclib
    # ydb/library/aclib/protos
    # ydb/library/login
    # ydb/library/ncloud/impl
    # ydb/library/security
    # ydb/library/ycloud/api
    # ydb/library/ycloud/impl
    contrib/libs/openssl
)

END()

RECURSE_FOR_TESTS(
    ut
)
