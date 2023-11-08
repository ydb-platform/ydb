GO_TEST()

ENV(GODEBUG="cgocheck=2")

INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmapi/recipe.inc)

GO_TEST_SRCS(client_test.go)

END()
