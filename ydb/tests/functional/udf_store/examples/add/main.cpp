#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>
#include <ydb/services/udf_store/wasm/abi/bridge.h>

using namespace NYdb::NUdfStore::NAbi;

extern "C" {
    __attribute__((visibility("default"))) void add(
    TExpressionContext* /*context*/,
    uint64_t* result,
    uint64_t arg0,
    uint64_t arg1)
{
    if (BridgeIsNull(arg0) || BridgeIsNull(arg1)) {
        *result = MakeNull().Release();
        return;
    }

        *result = MakeInt64(BridgeGetInt64(arg0) + BridgeGetInt64(arg1)).Release();
}
}
