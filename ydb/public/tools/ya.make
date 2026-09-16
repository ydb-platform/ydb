RECURSE(
    lib
    local_ydb
    ydb_recipe
)

RECURSE_FOR_TESTS(
    federation_discovery_recipe
    federation_recipe
)
