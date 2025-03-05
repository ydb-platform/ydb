LIBRARY()

SRCS(
    db_async_resolver.h
    mdb_endpoint_generator.h
)

PEERDIR(
    library/cpp/threading/future
    yql/essentials/providers/common/proto
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(db_async_resolver.h)

END()
