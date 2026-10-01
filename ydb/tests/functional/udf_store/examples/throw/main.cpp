#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>


//! Nested helpers with stable wasm export names for readable call stacks.
extern "C" {

__attribute__((visibility("default"))) void boom_leaf() {
    ThrowException("boom-from-wasm");
}

__attribute__((visibility("default"))) void boom_middle() {
    boom_leaf();
}

__attribute__((visibility("default"))) void fail(
    TExpressionContext* /*context*/,
    uint64_t* /*result*/)
{
    boom_middle();
}

} // extern "C"
