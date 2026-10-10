#include <ydb/services/udf_store/wasm/abi/async.h>

#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>

using namespace NYdb::NWasm::NAsync;

namespace {

// A bounded, reusable allocator makes frame lifetime observable without libc.
alignas(16) unsigned char Arena[32][1024];
bool Used[32];
uint64_t LiveObjects = 0;

} // namespace

void* operator new(std::size_t size) {
    if (size > sizeof(Arena[0])) {
        __builtin_trap();
    }
    for (unsigned i = 0; i < 32; ++i) {
        if (!Used[i]) {
            Used[i] = true;
            ++LiveObjects;
            return Arena[i];
        }
    }
    __builtin_trap();
}

void operator delete(void* pointer) noexcept {
    const auto index = (static_cast<unsigned char*>(pointer) - Arena[0]) / sizeof(Arena[0]);
    if (index >= 32 || !Used[index]) {
        __builtin_trap();
    }
    Used[index] = false;
    --LiveObjects;
}

void operator delete(void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}

namespace {

struct TArguments {
    uint64_t Mode;
    uint64_t Value;
};

struct TResult {
    EOperationStatus Status;
    uint64_t Value;
};

TTask<TResult> Fetch(TCallContext& ctx, uint64_t value) {
    TOperation operation(&value, sizeof(value));
    auto status = co_await operation.Wait(ctx);
    if (status != EOperationStatus::Ready) {
        co_return TResult{status, 0};
    }
    if (operation.Size() != sizeof(value)) {
        co_return TResult{EOperationStatus::Failed, 0};
    }
    operation.Read(&value, sizeof(value));
    co_return TResult{status, value};
}

TTask<TResult> Run(TCallContext& ctx, const TArguments* arguments) {
    if (arguments->Mode == 6) {
        auto timer = TOperation::Timer(1000);
        auto status = co_await timer.Wait(ctx);
        co_return TResult{status, arguments->Value};
    }
    if (arguments->Mode == 0) {
        co_return TResult{EOperationStatus::Ready, arguments->Value};
    }
    if (arguments->Mode == 5) {
        for (;;) {
            asm volatile("" ::: "memory");
        }
    }
    if (arguments->Mode == 2) {
        auto first = arguments->Value;
        auto second = first + 1;
        TOperation a(&first, sizeof(first));
        TOperation b(&second, sizeof(second));
        auto statusB = co_await b.Wait(ctx);
        auto statusA = co_await a.Wait(ctx);
        if (statusA != EOperationStatus::Ready || statusB != EOperationStatus::Ready) {
            co_return TResult{EOperationStatus::Failed, 0};
        }
        a.Read(&first, sizeof(first));
        b.Read(&second, sizeof(second));
        co_return TResult{EOperationStatus::Ready, first + second};
    }
    auto first = co_await Fetch(ctx, arguments->Value);
    if (arguments->Mode == 4) {
        __builtin_trap();
    }
    if (first.Status != EOperationStatus::Ready || arguments->Mode == 3) {
        co_return first;
    }
    // Read arguments again after suspend: a subsequent entry must not invalidate them.
    co_return co_await Fetch(ctx, first.Value + arguments->Value);
}

struct TCall {
    TCallContext Context;
    std::optional<TTask<TResult>> Task;

    explicit TCall(const TArguments* arguments) {
        Task.emplace(Run(Context, arguments));
        Context.Runnable = Task->Handle();
    }
};

} // namespace

extern "C" uint32_t WasmAsyncAbiVersion() {
    return AbiVersion;
}

extern "C" uint64_t WasmAsyncCallStart(uint64_t arguments, uint64_t size) {
    if (size != sizeof(TArguments)) {
        __builtin_trap();
    }
    return reinterpret_cast<uint64_t>(new TCall(reinterpret_cast<const TArguments*>(arguments)));
}

extern "C" void WasmAsyncCallPoll(uint64_t frame) {
    auto& call = *reinterpret_cast<TCall*>(frame);
    call.Context.Resume();
    if (call.Task->Done()) {
        auto result = call.Task->TakeResult();
        WasmAsyncCallComplete(&result.Value, sizeof(result.Value), static_cast<uint32_t>(result.Status));
    }
}

extern "C" void WasmAsyncCallCancel(uint64_t frame) {
    reinterpret_cast<TCall*>(frame)->Task.reset();
}

extern "C" void WasmAsyncCallDrop(uint64_t frame) {
    delete reinterpret_cast<TCall*>(frame);
}

extern "C" uint64_t WasmAsyncLiveObjects() {
    return LiveObjects;
}
