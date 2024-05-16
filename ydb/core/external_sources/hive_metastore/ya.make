LIBRARY()

SRCS(
    # thrift -r --gen cpp hive_metastore.thrift
    gen-cpp/FacebookService.cpp
    gen-cpp/ThriftHiveMetastore.cpp
    gen-cpp/fb303_types.cpp
    gen-cpp/hive_metastore_constants.cpp
    gen-cpp/hive_metastore_types.cpp

    # hive metastore external data source
    hive_metastore_client.cpp
)

PEERDIR(
    contrib/restricted/thrift
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)