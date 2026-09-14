#include "call_stack.h"
#include "host.h"
#include "host_intrinsic.h"
#include "invocation_context.h"

#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>

#include <util/generic/yexception.h>
#include <util/generic/utility.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/system/types.h>

#include <bit>


extern "C" char* AllocateBytes(TExpressionContext* /*context*/, size_t byteCount) {
    // Never trust the guest-provided context pointer. Host sets the current
    // invocation context via TLS for the duration of Run()/EnsureObject().
    auto* invocationContext = NKikimr::NUdfStore::NWasm::GetCurrentInvocationContext();
    if (!invocationContext) {
        ythrow yexception() << "AllocateBytes called without an active WASM UDF invocation context";
    }
    return invocationContext->WebAssemblyPool.AllocateUnaligned(byteCount);
}

extern "C" void ThrowException(const char* error) {
    TString message = "(null)";
    if (error) {
        if (auto* compartment = NYdb::NWasm::GetCurrentCompartment()) {
            const auto offset = std::bit_cast<uintptr_t>(error);
            const size_t memSize = compartment->GetLinearMemorySize();
            if (offset >= memSize) {
                ythrow yexception() << "ThrowException: error pointer is outside linear memory";
            }
            // Cap scan so we never ask WAVM to validate the entire remaining linear memory.
            constexpr size_t kMaxErrorMessageBytes = 64 * 1024;
            const size_t maxLen = Min(memSize - offset, kMaxErrorMessageBytes);
            const char* hostPtr = NYdb::NWasm::PtrFromVM(compartment, error, maxLen);
            size_t len = 0;
            while (len < maxLen && hostPtr[len] != '\0') {
                ++len;
            }
            message = TString(hostPtr, len);
        }
    }

    // Stack capture must never replace the original UDF error (e.g. if describeCallStack
    // hits a WAVM assert on a partial frame).
    TString stack;
    try {
        stack = NKikimr::NUdfStore::NWasm::FormatUserWasmCallStackFromCurrent();
    } catch (const std::exception& ex) {
        stack = TStringBuilder() << "<wasm call stack unavailable: " << ex.what() << ">\n";
    } catch (...) {
        stack = "<wasm call stack unavailable>\n";
    }

    // Plain throw: do not prefix with host.cpp:line (ythrow) — that is internal
    // and must not appear in the query failure reason shown to users.
    throw yexception()
        << "Error while executing UDF: "
        << message
        << "\n\n"
        << stack;
}

namespace NKikimr::NUdfStore::NWasm {
namespace {

using namespace NYdb::NWasm;

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

WASM_INTRINSIC(AllocateBytes, AllocateBytesHost, decltype(AllocateBytesHost))
WASM_INTRINSIC(ThrowException, ThrowExceptionHost, decltype(ThrowExceptionHost))

WAVM::Intrinsics::Function* const HostIntrinsicAnchors[] = {
    &IntrinsicFunctionAllocateBytes,
    &IntrinsicFunctionThrowException,
};

} // namespace

void EnsureUdfHostIntrinsicsRegistered()
{
    // The intrinsics register themselves from static initializers, which only
    // run if the linker keeps host.o and bridge_host.o. Being called from
    // outside is what makes that happen; the volatile read is here so the
    // anchor array survives optimization.
    [[maybe_unused]] WAVM::Intrinsics::Function* volatile anchor = HostIntrinsicAnchors[0];
    KeepBridgeHostIntrinsicsLinked();
}

} // namespace NKikimr::NUdfStore::NWasm
