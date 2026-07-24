FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

SRCS(
    main.cpp
    ../../change_record.cpp
    ../../change_record_body_serializer.cpp
    ../../change_record_cdc_serializer.cpp
)

PEERDIR(
    ydb/core/change_exchange
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/core/tx/datashard
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
