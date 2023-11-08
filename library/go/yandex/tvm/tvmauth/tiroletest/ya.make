GO_TEST()

ENV(GODEBUG="cgocheck=2")

GO_TEST_SRCS(client_test.go)

# tirole
INCLUDE(${ARCADIA_ROOT}/library/recipes/tirole/recipe.inc)
USE_RECIPE(
    library/recipes/tirole/tirole
    --roles-dir library/go/yandex/tvm/tvmauth/tiroletest/roles
)

# tvmapi - to provide service ticket for tirole
INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmapi/recipe.inc)

# tvmtool
INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe.inc)
USE_RECIPE(
    library/recipes/tvmtool/tvmtool
    library/go/yandex/tvm/tvmauth/tiroletest/tvmtool.cfg
    --with-roles-dir library/go/yandex/tvm/tvmauth/tiroletest/roles
)

END()
