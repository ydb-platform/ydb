#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" {
    __attribute__((visibility("default"))) void fail(
        TExpressionContext* /*context*/,
        TUnversionedValue* /*result*/)
    {
        ThrowException("boom-from-wasm");
    }
}
