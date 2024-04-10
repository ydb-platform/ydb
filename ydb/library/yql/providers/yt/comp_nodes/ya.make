OWNER(
    g:yql
    g:yql_ydb_core
)

LIBRARY()

END()

RECURSE(
    llvm14
    no_llvm
)

RECURSE_FOR_TESTS(
    ut
)