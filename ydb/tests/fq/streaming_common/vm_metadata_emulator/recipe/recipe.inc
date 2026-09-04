DEPENDS(
    ydb/tests/fq/streaming_common/vm_metadata_emulator/bin
    ydb/tests/fq/streaming_common/vm_metadata_emulator/recipe
)

USE_RECIPE(
    ydb/tests/fq/streaming_common/vm_metadata_emulator/recipe/iam_recipe
)
