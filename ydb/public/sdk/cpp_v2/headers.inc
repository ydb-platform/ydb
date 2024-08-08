ADDINCL(
    GLOBAL ydb/public/sdk/cpp_v2/include
)

ADDINCL(
    ydb/public/sdk/cpp_v2
)

CFLAGS(
    -DYDB_SDK_USE_TSTRING
)
