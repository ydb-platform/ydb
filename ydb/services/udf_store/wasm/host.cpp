#include "invocation_context.h"

#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>
#include <ydb/library/wasm/api/type_builder.h>
#include <ydb/library/wasm/engine/intrinsics.h>
#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <util/generic/yexception.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/system/types.h>

#include <bit>

namespace {

//! Keep only user-module wasm frames: "wasm!Module!func+off" → "func".
//! Drops host!, thnk!, and wasm!env! (imports / host thunks).
bool TrySimplifyUserWasmFrame(TStringBuf frame, TString& outName) {
    static constexpr TStringBuf Prefix = "wasm!";
    if (!frame.StartsWith(Prefix)) {
        return false;
    }
    frame.SkipPrefix(Prefix);
    if (frame.StartsWith("env!")) {
        return false;
    }
    const auto lastBang = frame.rfind('!');
    if (lastBang == TStringBuf::npos) {
        return false;
    }
    TStringBuf name = frame.Tail(lastBang + 1);
    if (const auto plus = name.find('+'); plus != TStringBuf::npos) {
        name = name.Head(plus);
    }
    if (name.empty() || name.StartsWith("thunk:")) {
        return false;
    }
    outName = TString(name);
    return true;
}

TString FormatWasmCallStack(WAVM::Uptr omitTopFrames = 1) {
    const auto callStack = WAVM::Platform::captureCallStack(omitTopFrames);
    const auto description = WAVM::Runtime::describeCallStack(callStack);
    TStringBuilder backtrace;
    int i = 0;
    for (const auto& item : description) {
        TString name;
        if (!TrySimplifyUserWasmFrame(item, name)) {
            continue;
        }
        backtrace << i++ << ". " << name << '\n';
    }
    if (i == 0) {
        backtrace << "<no user wasm frames>\n";
    }
    return backtrace;
}

} // namespace

extern "C" char* AllocateBytes(TExpressionContext* context, size_t byteCount) {
    auto* invocationContext = reinterpret_cast<NKikimr::NUdfStore::NWasm::TWasmUdfInvocationContext*>(context);
    return invocationContext->WebAssemblyPool.AllocateUnaligned(byteCount);
}

extern "C" void ThrowException(const char* error) {
    const char* message = "(null)";
    if (error) {
        if (auto* compartment = NYdb::NWasm::GetCurrentCompartment()) {
            message = NYdb::NWasm::PtrFromVM(compartment, error);
        }
    }

    // Stack capture must never replace the original UDF error (e.g. if describeCallStack
    // hits a WAVM assert on a partial frame).
    TString stack;
    try {
        stack = FormatWasmCallStack();
    } catch (const std::exception& ex) {
        stack = TStringBuilder() << "<wasm call stack unavailable: " << ex.what() << ">\n";
    } catch (...) {
        stack = "<wasm call stack unavailable>\n";
    }

    ythrow yexception()
        << "Error while executing UDF: "
        << message
        << "\n\n"
        << stack;
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
