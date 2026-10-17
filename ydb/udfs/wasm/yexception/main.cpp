#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <util/generic/yexception.h>

#include <unwind.h>

//! Guest C++ exception (yexception), not the host ThrowException import.
//! Nested helpers keep stable wasm names for readable call stacks.
extern "C" {

__attribute__((visibility("default"))) void boom_leaf() {
    ythrow yexception() << "boom-yexception-from-wasm";
}

__attribute__((visibility("default"))) void boom_middle() {
    boom_leaf();
}

// The __cpp_exception payload is an unwind exception pointer. Decode it in
// the guest, where the C++ ABI and the thrown yexception type are known.
__attribute__((visibility("default"))) uint64_t __ydb_wasm_exception_message(uint64_t unwindException) {
    // libc++abi places the thrown object immediately after _Unwind_Exception.
    // adjustedPtr is not initialized when the exception escapes without a guest catch.
    auto* object = reinterpret_cast<yexception*>(
        reinterpret_cast<_Unwind_Exception*>(unwindException) + 1);
    return reinterpret_cast<uint64_t>(object->what());
}

__attribute__((visibility("default"))) void fail(
    TExpressionContext* /*context*/,
    uint64_t* /*result*/)
{
    boom_middle();
}

} // extern "C"
