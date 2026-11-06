UNITTEST()

SRCS(
    state_file_ut.cpp
    ydb_clients_ut.cpp
    skipper_ut.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/logger
    ydb/apps/bridge_skipper/runtime
    ydb/library/actors/testlib
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
)

END()
