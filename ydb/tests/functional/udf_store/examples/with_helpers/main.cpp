#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>
#include <ydb/services/udf_store/wasm/abi/bridge.h>

using namespace NYdb::NUdfStore::NAbi;

// Linked at query time from modules entry type=LIBRARY name "helpers"
// (required_libraries: ["sdk", "helpers"]).
__attribute__((import_module("helpers"), import_name("helpers_scale")))
extern "C" long long helpers_scale(long long value);

extern "C" {
    __attribute__((visibility("default"))) void scale(
        TExpressionContext* /*context*/,
        uint64_t* result,
        uint64_t arg0)
    {
        if (BridgeIsNull(arg0)) {
            *result = MakeNull().Release();
            return;
        }

                *result = MakeInt64(helpers_scale(BridgeGetInt64(arg0))).Release();
    }
}
