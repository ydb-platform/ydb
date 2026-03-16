PY3TEST()

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

TAG(
    ya:force_sandbox
)

REQUIREMENTS(
    cpu:16
    yav:DOCKER_TOKEN_PATH=file:sec-01e0d4agf6pfvwdjwxp61n3fvg:docker_token
)

PEERDIR(
    contrib/python/aiodocker
    contrib/python/async-timeout
    contrib/python/pytest-asyncio
)

NO_LINT()

ALL_PYTEST_SRCS(RECURSIVE)

DATA(
    arcadia/contrib/python/aiodocker/tests/certs
    arcadia/contrib/python/aiodocker/tests/docker
)

END()
