# See https://a.yandex-team.ru/arcadia/devtools/ya/docs/internal/discussions/cpp_style.md?rev=11812028#pikantnyj-nyuans
CPP_STYLE_TEST_16()

SIZE(SMALL)

STYLE(
    ydb/library/yql/providers/generic/**/*.cpp
    ydb/library/yql/providers/generic/**/*.h
)

END()
