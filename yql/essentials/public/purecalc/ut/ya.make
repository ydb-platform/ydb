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
    test_fatal_err.cpp
    test_pool.cpp
    test_mixed_allocators.cpp
)

PEERDIR(
    yql/essentials/public/purecalc
    yql/essentials/public/purecalc/io_specs/protobuf
    yql/essentials/public/purecalc/ut/protos
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
