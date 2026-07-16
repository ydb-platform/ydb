#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" {
    __attribute__((visibility("default"))) void add(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* arg0,
    TUnversionedValue* arg1)
{
    if (arg0->Type == EValueType::Null || arg1->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }

    result->Type = EValueType::Int64;
    result->Data.Int64 = arg0->Data.Int64 + arg1->Data.Int64;
}
}
