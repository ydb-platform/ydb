LIBRARY()

SRCS(
    gen-cpp/FacebookService.cpp
    gen-cpp/ThriftHiveMetastore.cpp
    gen-cpp/fb303_types.cpp
    gen-cpp/hive_metastore_constants.cpp
    gen-cpp/hive_metastore_types.cpp
)

PEERDIR(
    contrib/restricted/thrift
)

END()
