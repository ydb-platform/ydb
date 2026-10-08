#include <ydb/core/fq/libs/wasm_services/wire.h>
#include <ydb/services/udf_store/wasm/abi/async.h>

#include <memory>
#include <new>
#include <optional>

using namespace NYdb::NWasm::NAsync;
using namespace NFq::NWasmServices;

namespace {

// Reusable fixture-only storage: observable lifetime without libc++ string imports.
constexpr unsigned Slots = 32;
alignas(16) unsigned char Arena[Slots][17408];
bool Used[Slots];
uint64_t LiveObjects = 0;

} // namespace

void* operator new(std::size_t size) {
    if (size > sizeof(Arena[0])) {
        __builtin_trap();
    }
    for (unsigned i = 0; i < Slots; ++i) {
        if (!Used[i]) {
            Used[i] = true;
            ++LiveObjects;
            return Arena[i];
        }
    }
    __builtin_trap();
}

void operator delete(void* pointer) noexcept {
    if (!pointer) {
        return;
    }
    const auto offset = reinterpret_cast<uintptr_t>(pointer) - reinterpret_cast<uintptr_t>(Arena);
    const auto index = offset / sizeof(Arena[0]);
    if (index >= Slots || offset % sizeof(Arena[0]) || !Used[index]) {
        __builtin_trap();
    }
    Used[index] = false;
    --LiveObjects;
}

void operator delete(void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}

void* operator new[](std::size_t size) {
    return ::operator new(size);
}

void operator delete[](void* pointer) noexcept {
    ::operator delete(pointer);
}

void operator delete[](void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}

namespace {

struct TBytes {
    std::unique_ptr<char[]> Buffer;
    size_t Size = 0;

    TBytes() = default;
    explicit TBytes(size_t size)
        : Buffer(size ? new char[size] : nullptr), Size(size)
    {}

    std::string_view View() const {
        return {Buffer.get(), Size};
    }
};

template <class T> TBytes Pack(const T& header, std::string_view payload) {
    TBytes bytes(sizeof(T) + payload.size());
    std::memcpy(bytes.Buffer.get(), &header, sizeof(T));
    if (!payload.empty()) {
        std::memcpy(bytes.Buffer.get() + sizeof(T), payload.data(), payload.size());
    }
    return bytes;
}

struct TReply {
    EOperationStatus Status;
    TBytes Bytes;
};

TTask<TReply> Fetch(TCallContext& context, uint32_t binding, std::string_view body) {
    const auto request = Pack(TRequestHeader{WireVersion, binding, body.size()}, body);
    TOperation operation(request.Buffer.get(), request.Size);
    auto status = co_await operation.Wait(context);
    TReply reply{status, {}};
    const auto size = operation.Size();
    if (size > 8192) {
        co_return TReply{EOperationStatus::Failed, {}};
    }
    reply.Bytes = TBytes(size);
    operation.Read(reply.Bytes.Buffer.get(), size);
    TResponseHeader header;
    std::string_view payload;
    if (!Decode(reply.Bytes.View(), header, payload) || header.Version != WireVersion || header.PayloadBytes != payload.size()) {
        reply.Status = EOperationStatus::Failed;
    }
    co_return reply;
}

TReply Collect(TReply first, std::optional<TReply> second = {}) {
    TResultHeader header{WireVersion, second ? 2u : 1u, first.Bytes.Size, second ? second->Bytes.Size : 0};
    TBytes result(sizeof(header) + header.FirstBytes + header.SecondBytes);
    std::memcpy(result.Buffer.get(), &header, sizeof(header));
    if (header.FirstBytes) {
        std::memcpy(result.Buffer.get() + sizeof(header), first.Bytes.Buffer.get(), header.FirstBytes);
    }
    if (header.SecondBytes) {
        std::memcpy(result.Buffer.get() + sizeof(header) + header.FirstBytes, second->Bytes.Buffer.get(), header.SecondBytes);
    }
    return {first.Status == EOperationStatus::Ready && (!second || second->Status == EOperationStatus::Ready) ? EOperationStatus::Ready
                                                                                                              : EOperationStatus::Failed,
            std::move(result)};
}

TTask<TReply> Run(TCallContext& context, const TArgumentsHeader* args) {
    const auto body = std::string_view(reinterpret_cast<const char*>(args + 1), args->PayloadBytes);
    if (args->Mode == 0) {
        co_return Collect(co_await Fetch(context, args->BindingA, body));
    }
    if (args->Mode == 1) {
        auto first = co_await Fetch(context, args->BindingA, body);
        if (first.Status != EOperationStatus::Ready) {
            co_return Collect(std::move(first));
        }
        TResponseHeader header;
        std::string_view payload;
        if (!Decode(first.Bytes.View(), header, payload)) {
            __builtin_trap();
        }
        auto second = co_await Fetch(context, args->BindingB, payload);
        co_return Collect(std::move(first), std::move(second));
    }
    // Start both operations before awaiting either: transport can run concurrently.
    const auto requestA = Pack(TRequestHeader{WireVersion, args->BindingA, body.size()}, body);
    const auto requestB = Pack(TRequestHeader{WireVersion, args->BindingB, body.size()}, body);
    TOperation a(requestA.Buffer.get(), requestA.Size);
    TOperation b(requestB.Buffer.get(), requestB.Size);
    auto statusB = co_await b.Wait(context);
    auto statusA = co_await a.Wait(context);
    if (a.Size() > 8192 || b.Size() > 8192) {
        co_return TReply{EOperationStatus::Failed, {}};
    }
    TReply first{statusA, TBytes(a.Size())};
    TReply second{statusB, TBytes(b.Size())};
    a.Read(first.Bytes.Buffer.get(), first.Bytes.Size);
    b.Read(second.Bytes.Buffer.get(), second.Bytes.Size);
    co_return Collect(std::move(first), std::move(second));
}

struct TCall {
    TCallContext Context;
    std::optional<TTask<TReply>> Task;
    explicit TCall(const TArgumentsHeader* args) {
        Task.emplace(Run(Context, args));
        Context.Runnable = Task->Handle();
    }
};

} // namespace

extern "C" uint32_t WasmAsyncAbiVersion() {
    return AbiVersion;
}

extern "C" uint64_t WasmAsyncCallStart(uint64_t arguments, uint64_t size) {
    if (size < sizeof(TArgumentsHeader)) {
        __builtin_trap();
    }
    const auto* args = reinterpret_cast<const TArgumentsHeader*>(arguments);
    if (args->PayloadBytes != size - sizeof(*args) || args->Mode > 2) {
        __builtin_trap();
    }
    return reinterpret_cast<uint64_t>(new TCall(args));
}

extern "C" void WasmAsyncCallPoll(uint64_t frame) {
    auto& call = *reinterpret_cast<TCall*>(frame);
    call.Context.Resume();
    if (call.Task->Done()) {
        auto reply = call.Task->TakeResult();
        WasmAsyncCallComplete(reply.Bytes.Buffer.get(), reply.Bytes.Size, static_cast<uint32_t>(reply.Status));
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
