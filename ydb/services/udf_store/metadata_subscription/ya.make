LIBRARY()
YQL_LAST_ABI_VERSION()

SRCS(
    udf_meta.cpp
    manager.cpp
    snapshot.cpp
)

GENERATE_ENUM_SERIALIZATION(udf_meta.h)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/keyvalue
    ydb/core/tx/scheme_cache
    ydb/library/aclib
    ydb/library/table_creator
    ydb/services/metadata/request
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
    ydb/services/metadata
    yql/essentials/minikql
    library/cpp/digest/md5
)

END()
