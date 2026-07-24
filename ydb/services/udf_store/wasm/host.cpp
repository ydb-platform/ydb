#include "invocation_context.h"

#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>
#include <ydb/library/wasm/api/type_builder.h>
#include <ydb/library/wasm/engine/intrinsics.h>
#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <util/generic/yexception.h>

#include <util/generic/scope.h>
#include <util/system/types.h>

#include <bit>

extern "C" char* AllocateBytes(TExpressionContext* context, size_t byteCount) {
    auto* invocationContext = reinterpret_cast<NKikimr::NUdfStore::NWasm::TWasmUdfInvocationContext*>(context);
    return invocationContext->WebAssemblyPool.AllocateUnaligned(byteCount);
}

extern "C" void ThrowException(const char* error) {
    ythrow yexception()
        << "Error while executing UDF: "
        << NYdb::NWasm::PtrFromVM(NYdb::NWasm::GetCurrentCompartment(), error);
}

namespace NKikimr::NUdfStore::NWasm {
namespace {

using namespace NYdb::NWasm;

template <class TSignature>
struct TMakeUdfHostIntrinsic;

template <class TResult, class... TArgs>
struct TMakeUdfHostIntrinsic<TResult(TArgs...)>
{
    template <TResult(*FunctionPtr)(TArgs...)>
    static TResult Wrapper(WAVM::Runtime::ContextRuntimeData*, TArgs... args)
    {
        auto* compartmentBeforeCall = GetCurrentCompartment();
        Y_DEFER {
            auto* compartmentAfterCall = GetCurrentCompartment();
            YT_VERIFY(compartmentBeforeCall == compartmentAfterCall);
        };
        return FunctionPtr(args...);
    }
};

char* AllocateBytesHost(void* context, ui64 byteCount)
{
    return ::AllocateBytes(
        reinterpret_cast<TExpressionContext*>(context),
        static_cast<size_t>(byteCount));
}

void ThrowExceptionHost(const char* error)
{
    ::ThrowException(error);
}

constexpr auto IntrinsicAllocateBytes =
    &TMakeUdfHostIntrinsic<decltype(AllocateBytesHost)>::Wrapper<&AllocateBytesHost>;
[[gnu::used]] static WAVM::Intrinsics::Function IntrinsicFunctionAllocateBytes(
    getIntrinsicModule_standard(),
    "AllocateBytes",
    reinterpret_cast<void*>(IntrinsicAllocateBytes),
    WAVM::IR::FunctionType(WAVM::IR::FunctionType::Encoding{
        std::bit_cast<WAVM::Uptr>(TFunctionTypeBuilder<true, decltype(AllocateBytesHost)>::Get())
    }));

constexpr auto IntrinsicThrowException =
    &TMakeUdfHostIntrinsic<decltype(ThrowExceptionHost)>::Wrapper<&ThrowExceptionHost>;
[[gnu::used]] static WAVM::Intrinsics::Function IntrinsicFunctionThrowException(
    getIntrinsicModule_standard(),
    "ThrowException",
    reinterpret_cast<void*>(IntrinsicThrowException),
    WAVM::IR::FunctionType(WAVM::IR::FunctionType::Encoding{
        std::bit_cast<WAVM::Uptr>(TFunctionTypeBuilder<true, decltype(ThrowExceptionHost)>::Get())
    }));

} // namespace

void EnsureUdfHostIntrinsicsRegistered()
{
    // Keep host.o and intrinsic statics linked into ydbd.
    Y_UNUSED(IntrinsicFunctionAllocateBytes);
    Y_UNUSED(IntrinsicFunctionThrowException);
}

} // namespace NKikimr::NUdfStore::NWasm
