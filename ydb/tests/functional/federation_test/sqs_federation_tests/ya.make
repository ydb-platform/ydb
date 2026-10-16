PY3TEST()

TEST_SRCS(
    tests_sqs_compatibility.py
)

PEERDIR(
    contrib/python/boto3
    contrib/python/botocore
    contrib/python/grpcio
    ydb/public/api/grpc
    ydb/public/api/protos
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/federation_recipe/recipe.inc)

# Share one federation recipe across the scenarios.
SIZE(MEDIUM)
TIMEOUT(600)
REQUIREMENTS(ram:16)

END()
