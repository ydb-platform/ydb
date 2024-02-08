LIBRARY()

SRCS(
    add_data.cpp
    add_index.cpp
    executor.cpp
    activation.cpp
    deleting.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/services/ext_index/metadata
    ydb/services/ext_index/common
    ydb/library/yql/minikql/jsonpath
    ydb/public/api/protos
)

END()
