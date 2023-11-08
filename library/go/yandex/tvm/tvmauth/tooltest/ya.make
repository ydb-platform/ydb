GO_TEST()

ENV(GODEBUG="cgocheck=2")

INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe_with_default_cfg.inc)

GO_TEST_SRCS(
    client_test.go
    logger_test.go
)

END()
