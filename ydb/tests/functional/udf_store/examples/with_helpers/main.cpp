#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

using namespace NYdb::NUdfStore::NAbi;

// Linked at query time from modules entry type=LIBRARY name "helpers"
// (required_libraries: ["sdk", "helpers"]).
__attribute__((import_module("helpers"), import_name("helpers_scale")))
extern "C" long long helpers_scale(long long value);

extern "C" {
    __attribute__((visibility("default"))) void scale(
        TExpressionContext* /*context*/,
        TUnversionedValue* result,
        TUnversionedValue* arg0)
    {
        if (arg0->Type == EValueType::Null) {
            result->Type = EValueType::Null;
            return;
        }

        result->Type = EValueType::Int64;
        result->Data.Int64 = helpers_scale(arg0->Data.Int64);
    }
}
