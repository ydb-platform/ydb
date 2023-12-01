
PY3TEST()

TEST_SRCS(
    test_docker_compose.py
)

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/large.inc)
INCLUDE(${ARCADIA_ROOT}/devtools/ya/chameleon_bin/recipe.inc)

TAG(
    ya:external
    ya:force_sandbox
    ya:dirty
)

REQUIREMENTS(
    container:4467981730  # bionic with fuse allowed
    cpu:all
    dns:dns64
)

DATA(
   arcadia/library/recipes/docker_compose/example
   arcadia/library/recipes/docker_compose/example_with_context
   arcadia/library/recipes/docker_compose/example_test_container
   arcadia/library/recipes/docker_compose/example_with_recipe_config
)

PEERDIR(
    devtools/ya/test/tests/lib/common
)

DEPENDS(
    devtools/ya/test/programs/test_tool/bin
    devtools/ya/test/programs/test_tool/bin3
    devtools/ymake/bin
)

END()
