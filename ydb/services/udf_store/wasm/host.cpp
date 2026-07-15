#include "invocation_context.h"

#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>

#include <library/cpp/yt/error/error.h>
#include <library/cpp/yt/string/format_string.h>

extern "C" char* AllocateBytes(TExpressionContext* context, size_t byteCount) {
    auto* invocationContext = reinterpret_cast<NKikimr::NUdfStore::NWasm::TWasmUdfInvocationContext*>(context);
    return invocationContext->WebAssemblyPool.AllocateUnaligned(byteCount);
}

extern "C" void ThrowException(const char* error) {
    THROW_ERROR_EXCEPTION("Error while executing UDF")
        << NYT::TError(NYT::TRuntimeFormat(NYdb::NWasm::PtrFromVM(
            NYdb::NWasm::GetCurrentCompartment(),
            error)));
}
