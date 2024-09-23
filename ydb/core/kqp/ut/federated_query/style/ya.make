# See https://a.yandex-team.ru/arcadia/devtools/ya/docs/internal/discussions/cpp_style.md?rev=11812028#pikantnyj-nyuans
CPP_STYLE_TEST_16()

SIZE(SMALL)

STYLE(
    ydb/core/kqp/ut/federated_query/common/**/*.cpp
    ydb/core/kqp/ut/federated_query/common/**/*.h
    ydb/core/kqp/ut/federated_query/generic/**/*.cpp
    ydb/core/kqp/ut/federated_query/generic/**/*.h
)

END()
