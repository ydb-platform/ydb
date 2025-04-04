LIBRARY()

SRCS(
    database_type.h
    database_type.cpp
    db_async_resolver.h
    mdb_endpoint_generator.h
)

PEERDIR(
    library/cpp/threading/future
    yql/essentials/providers/common/proto
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(database_type.h)

END()
