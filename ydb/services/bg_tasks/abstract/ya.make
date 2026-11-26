LIBRARY()

SRCS(
    interface.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/json/writer
    library/cpp/string_utils/base64
    ydb/core/base
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/library/services
    ydb/services/bg_tasks/protos
)

END()
