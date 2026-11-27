BUILD_ONLY_IF(OS_LINUX)

BUILD_ONLY_IF(ARCH_X86_64)

YQL_INTERNALIZED_LIBRARY_FAT(sql_inspect_abi)

YQL_EXPORT_SYMBOLS(
    YqlSqlInspectV1
    YqlSqlInspectFreeV1
    YqlSqlInspectResultV1
    YqlSqlInspectErrorV1
)

PEERDIR(
    yql/essentials/public/sql_inspect/abi/impl
)

END()

RECURSE_FOR_TESTS(
    ut
)
