LIBRARY()

SRCS(
    connector_client_mock.cpp
    database_resolver_mock.cpp
    defaults.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/testing/gmock_in_unittest
    library/cpp/testing/unittest
    ydb/core/formats/arrow/serializer
    ydb/core/kqp/ut/common
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/common/structured_token
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/providers/generic/connector/libcpp
)

YQL_LAST_ABI_VERSION()

END()
