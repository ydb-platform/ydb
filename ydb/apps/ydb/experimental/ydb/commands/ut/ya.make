UNITTEST_FOR(ydb/apps/ydb/experimental/ydb/commands)

SIZE(SMALL)

SRCS(
    udf_package_ut.cpp
)

DATA(
    arcadia/ydb/apps/ydb/experimental/ydb/commands/ut/data/python_pax_package.tar.gz
)

END()
