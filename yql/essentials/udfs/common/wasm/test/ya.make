SUBSCRIBER(g:yql)

YQL_UDF_TEST()

TIMEOUT(900)

SIZE(LARGE)

DEPENDS(yql/essentials/udfs/common/wasm)

DATA(
    arcadia/yql/essentials/udfs/common/wasm/test/data/add_mul.wat
    arcadia/yql/essentials/udfs/common/wasm/test/data/local_udf.function_descriptor.yson
    arcadia/yql/essentials/udfs/common/wasm/test/data/local_udf.wat
    arcadia/yql/essentials/udfs/common/wasm/test/data/memory_args.wat
    arcadia/yql/essentials/udfs/common/wasm/test/data/scalar_exports.wat
    arcadia/yql/essentials/udfs/common/wasm/test/data/wasm_registry/local_udf/function_descriptor.yson
    arcadia/yql/essentials/udfs/common/wasm/test/data/wasm_registry/local_udf/local_udf.wat
    arcadia/yql/essentials/udfs/common/wasm/test/data/env_registry_complex/base64/function_descriptor.yson
)

END()
