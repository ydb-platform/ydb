FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/ydb_convert
    ydb/core/base
    ydb/core/engine
    ydb/core/protos
    ydb/core/formats/arrow/switch
    ydb/core/scheme
    ydb/library/conclusion
    ydb/library/mkql_proto/protos
    ydb/library/ydb_issue
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/value
    library/cpp/protobuf/json
    yql/essentials/core
    yql/essentials/minikql/dom
    yql/essentials/public/udf
    yql/essentials/public/udf/service/stub
    yql/essentials/public/udf/arrow
    yql/essentials/sql/pg_dummy
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
)

CFLAGS(
    -Wno-deprecated-declarations
)

YQL_LAST_ABI_VERSION()

END()
