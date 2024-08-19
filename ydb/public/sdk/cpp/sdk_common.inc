ADDINCL(
    GLOBAL ydb/public/sdk/cpp/include
)

ADDINCL(
    ydb/public/sdk/cpp
)

CFLAGS(
    -DYDB_SDK_USE_TSTRING
)
