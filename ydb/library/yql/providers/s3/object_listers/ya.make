LIBRARY()

SRCS(
    yql_s3_list.cpp
    yql_s3_path.cpp
)

GENERATE_ENUM_SERIALIZATION(yql_s3_list.h)

PEERDIR(
    contrib/libs/re2
    library/cpp/string_utils/quote
    library/cpp/xml/document
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/s3/credentials
    yql/essentials/utils
    ydb/library/yql/utils/actor_log
    yql/essentials/utils/threading
)

END()

RECURSE_FOR_TESTS(
    ut
)
