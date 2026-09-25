DEPENDS(
    ydb/tests/fq/streaming_common/iam_grpc_emulator/bin
    ydb/tests/fq/streaming_common/iam_grpc_emulator/recipe
)

USE_RECIPE(
    ydb/tests/fq/streaming_common/iam_grpc_emulator/recipe/iam_grpc_recipe
)
