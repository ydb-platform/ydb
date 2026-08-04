FUZZ()

PEERDIR(
    ydb/core/protos
    ydb/core/base
    ydb/core/scheme
    ydb/core/scheme_types
    ydb/public/lib/scheme_types
    ydb/library/pretty_types_print/protobuf
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/minikql/dom
    yql/essentials/public/udf
    yql/essentials/public/udf/service/stub
    yql/essentials/public/udf/arrow
    yql/essentials/sql/pg_dummy
    yql/essentials/utils
    library/cpp/yson_pull
    library/cpp/yson
    ydb/core/ymq/actor
    contrib/libs/protobuf-mutator
)

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    main.cpp
)

END()
