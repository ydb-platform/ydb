#include "intrinsics.h"

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>
#include <ydb/library/wasm/api/type_builder.h>

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/yt/error/error.h>
#include <util/generic/scope.h>

#include <chrono>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

WAVM_DEFINE_INTRINSIC_MODULE(empty);
WAVM_DEFINE_INTRINSIC_MODULE(standard);

////////////////////////////////////////////////////////////////////////////////

template <class TSignature>
struct TMakeIntrinsic;

template <class TResult, class... TArgs>
struct TMakeIntrinsic<TResult(TArgs...)> {
    template <TResult (*FunctionPtr)(TArgs...)>
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

#define REGISTER_WEB_ASSEMBLY_INTRINSIC(intrinsic)                                                            \
    constexpr auto Intrinsic##intrinsic = &TMakeIntrinsic<decltype(intrinsic)>::Wrapper<&intrinsic>;          \
    static WAVM::Intrinsics::Function IntrinsicFunction##intrinsic(getIntrinsicModule_standard(), #intrinsic, \
        reinterpret_cast<void*>(Intrinsic##intrinsic),                                                        \
        WAVM::IR::FunctionType(                                                                               \
            WAVM::IR::FunctionType::Encoding{ std::bit_cast<WAVM::Uptr>(TFunctionTypeBuilder<true, decltype(intrinsic)>::Get()) }));

#define DEFINE_WEB_ASSEMBLY_SYSCALL_STUB(name, result, ...)                                              \
    result name(__VA_ARGS__) {                                                                           \
        auto callStack = WAVM::Platform::captureCallStack(1);                                            \
        auto description = WAVM::Runtime::describeCallStack(callStack);                                  \
        auto backtrace = std::string();                                                                  \
        int i = 0;                                                                                       \
        for (auto& item : description) {                                                                 \
            backtrace += std::to_string(i++) + ". ";                                                     \
            backtrace += item;                                                                           \
            backtrace += '\n';                                                                           \
        }                                                                                                \
        THROW_ERROR_EXCEPTION("WebAssembly call to forbidden system call: %Qv\n\n%v", #name, backtrace); \
    }                                                                                                    \
    REGISTER_WEB_ASSEMBLY_INTRINSIC(name);

FOREACH_WEB_ASSEMBLY_SYSCALL(DEFINE_WEB_ASSEMBLY_SYSCALL_STUB);

////////////////////////////////////////////////////////////////////////////////

// WASI preview1 clock IDs and errno values, independent of the host's errno ABI.
// Reads are immediate and also work while module constructors run. Precision is
// a tolerance hint: returning the full available precision satisfies it.
i32 clock_time_get(i32 clockId, ui64 /*precision*/, ui64 output)
{
    constexpr i32 Fault = 21;
    constexpr i32 Invalid = 28;
    constexpr i32 NotSupported = 58;
    constexpr i32 Overflow = 61;
    if (clockId != 0 && clockId != 1) {
        return clockId == 2 || clockId == 3 ? NotSupported : Invalid;
    }
    auto* compartment = GetCurrentCompartment();
    if (!compartment) {
        return Fault;
    }
    const auto size = compartment->GetLinearMemorySize();
    if (output > size || size - output < sizeof(ui64)) {
        return Fault;
    }
    using namespace std::chrono;
    const auto timestamp = clockId == 0 ? duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count()
                                        : duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    if (timestamp < 0) {
        return Overflow;
    }
    // WASM memory is little-endian and the guest address need not be aligned.
    auto* bytes = static_cast<unsigned char*>(compartment->GetHostPointer(output, sizeof(ui64)));
    for (unsigned i = 0; i < sizeof(ui64); ++i) {
        bytes[i] = static_cast<ui64>(timestamp) >> (8 * i);
    }
    return 0;
}

REGISTER_WEB_ASSEMBLY_INTRINSIC(clock_time_get);

void emscripten_notify_memory_growth(i64)   // NOLINT
{
    // Do nothing.
}

REGISTER_WEB_ASSEMBLY_INTRINSIC(emscripten_notify_memory_growth);

i32 logAPIs()   // NOLINT
{
    return 0;
}

REGISTER_WEB_ASSEMBLY_INTRINSIC(logAPIs);

void emscripten_dbg_backtrace(i64)   // NOLINT
{
    // Do nothing.
}

REGISTER_WEB_ASSEMBLY_INTRINSIC(emscripten_dbg_backtrace);

i32 args_sizes_get(i64* argc, i64* argvBufSize)
{
    auto* compartment = GetCurrentCompartment();
    *(PtrFromVM(compartment, argc)) = 0;
    *(PtrFromVM(compartment, argvBufSize)) = 0;
    return 0;
}

REGISTER_WEB_ASSEMBLY_INTRINSIC(args_sizes_get);

i32 environ_sizes_get(i64* argc, i64* argvBufSize)
{
    auto* compartment = GetCurrentCompartment();
    *(PtrFromVM(compartment, argc)) = 0;
    *(PtrFromVM(compartment, argvBufSize)) = 0;
    return 0;
}

REGISTER_WEB_ASSEMBLY_INTRINSIC(environ_sizes_get);

i32 environ_get(i64* first, i64* second)
{
    auto* compartment = GetCurrentCompartment();
    *(PtrFromVM(compartment, first)) = 0;
    *(PtrFromVM(compartment, second)) = 0;
    return 0;
}

REGISTER_WEB_ASSEMBLY_INTRINSIC(environ_get);

void _emscripten_get_progname(char*, i32)
{
}

REGISTER_WEB_ASSEMBLY_INTRINSIC(_emscripten_get_progname);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NWasm
