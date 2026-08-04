FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/ydb_convert
    ydb/core/protos
    ydb/core/scheme
    ydb/core/engine
    ydb/library/mkql_proto/protos
    ydb/library/ydb_issue
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/value
    yql/essentials/core
    yql/essentials/minikql/dom
    yql/essentials/public/udf
    yql/essentials/public/udf/service/stub
    yql/essentials/public/udf/arrow
    yql/essentials/sql/pg_dummy
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
)

CFLAGS(-Wno-deprecated-declarations)

YQL_LAST_ABI_VERSION()

END()
