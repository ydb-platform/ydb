GO_TEST()

GO_TEST_SRCS(go_test.go)

# To use docker-compose.yml from another directory, set DOCKER_COMPOSE_FILE variable with Arcadia relative path to the file
# and do not forget to add the directory to the DATA macro, e.g.:
# SET(DOCKER_COMPOSE_FILE library/recipes/docker_compose/test/docker-compose-1.yml)
# DATA(arcadia/library/recipes/docker_compose/test)

SET(DOCKER_TEST_HOST test)
INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

TAG(
    ya:external
    ya:force_sandbox
    ya:dirty
)

END()
