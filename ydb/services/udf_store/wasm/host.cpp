#include "invocation_context.h"

#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>

#include <util/generic/yexception.h>

extern "C" char* AllocateBytes(TExpressionContext* context, size_t byteCount) {
    auto* invocationContext = reinterpret_cast<NKikimr::NUdfStore::NWasm::TWasmUdfInvocationContext*>(context);
    return invocationContext->WebAssemblyPool.AllocateUnaligned(byteCount);
}

extern "C" void ThrowException(const char* error) {
    ythrow yexception()
        << "Error while executing UDF: "
        << NYdb::NWasm::PtrFromVM(NYdb::NWasm::GetCurrentCompartment(), error);
}
