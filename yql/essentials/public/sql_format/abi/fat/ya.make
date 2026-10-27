BUILD_ONLY_IF(OS_LINUX)

BUILD_ONLY_IF(ARCH_X86_64)

YQL_INTERNALIZED_LIBRARY_FAT(sql_format_abi)

YQL_EXPORT_SYMBOLS(
    YqlSqlFormatV1
    YqlSqlFormatFreeV1
    YqlSqlFormatStatusV1
    YqlSqlFormatOutputV1
    YqlSqlFormatErrorV1
)

PEERDIR(
    yql/essentials/public/sql_format/abi/impl
)

END()

RECURSE_FOR_TESTS(
    ut
)
