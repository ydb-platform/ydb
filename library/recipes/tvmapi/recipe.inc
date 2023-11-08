DEPENDS(
    contrib/tools/sqlite3
    library/recipes/tvmapi
    passport/infra/daemons/tvmapi/daemon
)

DATA(
    arcadia/library/recipes/tvmapi/clients
    arcadia/library/recipes/tvmapi/data
)

USE_RECIPE(library/recipes/tvmapi/tvmapi)
