UNITTEST()

SRCS(
    empty_stream.h
    fake_spec.cpp
    fake_spec.h
    test_schema.cpp
    test_sexpr.cpp
    test_sql.cpp
    test_pg.cpp
    test_udf.cpp
    test_user_data.cpp
    test_eval.cpp
    test_pool.cpp
)

PEERDIR(
    ydb/library/yql/public/purecalc
    ydb/library/yql/public/purecalc/io_specs/protobuf
    ydb/library/yql/public/purecalc/ut/protos
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

YQL_LAST_ABI_VERSION()

END()
