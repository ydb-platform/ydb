LIBRARY()

SRCS(
    actors.cpp
    common.cpp
    connection_ddl.cpp
    fq_runner.cpp
    fq_setup.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/protobuf/json
    ydb/library/actors/wilson
    library/cpp/protobuf/interop
    library/cpp/testing/unittest
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/common
    ydb/core/fq/libs/control_plane_proxy/events
    ydb/core/fq/libs/init
    ydb/core/fq/libs/mock
    ydb/core/fq/libs/wasm_services/mock
    ydb/core/testlib
    ydb/library/actors/core
    ydb/library/folder_service/mock
    ydb/library/grpc/server/actors
    ydb/library/security
    ydb/library/protobuf_printer
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yql/providers/pq/provider
    ydb/library/yql/providers/s3/actors
    ydb/tests/tools/kqprun/runlib
    yql/essentials/minikql
    yql/essentials/sql/v1/translation
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4
)

YQL_LAST_ABI_VERSION()

END()
