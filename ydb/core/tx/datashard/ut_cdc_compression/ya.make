UNITTEST()

SIZE(SMALL)

PEERDIR(
    contrib/libs/zstd
    library/cpp/digest/md5
    library/cpp/json
    library/cpp/json/yson
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    ydb/core/base
    ydb/core/change_exchange
    ydb/core/io_formats/json
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/library/aclib
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/value
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/decimal
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    yql/essentials/types/uuid
)

YQL_LAST_ABI_VERSION()

ADDINCL(
    ydb/core/tx/datashard
)

SRCS(
    datashard_ut_cdc_compression.cpp
)

SRCDIR(
    ydb/core/tx/datashard
)

SRCS(
    change_record.cpp
    change_record_cdc_serializer.cpp
    type_serialization.cpp
)

END()
