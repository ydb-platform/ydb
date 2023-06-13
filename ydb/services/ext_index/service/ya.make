LIBRARY()

SRCS(
    add_data.cpp
    add_index.cpp
    executor.cpp
    activation.cpp
    deleting.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/services/ext_index/metadata
    ydb/services/ext_index/common
    ydb/library/yql/minikql/jsonpath
    ydb/public/api/protos
)

END()
