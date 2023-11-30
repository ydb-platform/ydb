PY2TEST()

TEST_SRCS(
    test_docker_context.py
)

PEERDIR(
    contrib/python/PyYAML
    library/recipes/docker_compose/lib
)

DEPENDS(
    devtools/dummy_arcadia/hello_world
)

END()
