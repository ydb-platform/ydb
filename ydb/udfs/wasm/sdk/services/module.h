#pragma once

#include <ydb/services/udf_store/wasm/abi/async.h>
#include "rows.h"
#include "transport.h"

#include <memory>
#include <optional>

namespace NYdb::NWasm::NServices {

struct TModuleBytes {
    std::unique_ptr<char[]> Buffer;
    size_t Size = 0;
    TModuleBytes() = default;
    explicit TModuleBytes(size_t size)
        : Buffer(size ? new char[size] : nullptr), Size(size)
    {
    }
    std::string_view View() const {
        return {Buffer.get(), Size};
    }
};

template <class T> TModuleBytes ModulePack(const T& header, std::string_view payload = {}) {
    TModuleBytes bytes(sizeof(T) + payload.size());
    std::memcpy(bytes.Buffer.get(), &header, sizeof(T));
    if (!payload.empty())
        std::memcpy(bytes.Buffer.get() + sizeof(T), payload.data(), payload.size());
    return bytes;
}

struct TModuleReply {
    NAsync::EOperationStatus Status;
    TModuleBytes Bytes;
};

using TModuleHandler = NAsync::TTask<TModuleReply> (*)(NAsync::TCallContext&, const void*, size_t);

struct TModuleCall {
    NAsync::TCallContext Context;
    std::optional<NAsync::TTask<TModuleReply>> Task;
    TModuleCall(TModuleHandler handler, const void* arguments, size_t size) {
        Task.emplace(handler(Context, arguments, size));
        Context.Runnable = Task->Handle();
    }
    void Poll() {
        Context.Resume();
        if (Task->Done()) {
            auto reply = Task->TakeResult();
            WasmAsyncCallComplete(reply.Bytes.Buffer.get(), reply.Bytes.Size, static_cast<uint32_t>(reply.Status));
        }
    }
};

} // namespace NYdb::NWasm::NServices

#define WASM_SERVICE_MODULE(Handler)                                                                                                       \
    extern "C" uint32_t WasmAsyncAbiVersion() {                                                                                            \
        return NYdb::NWasm::NAsync::AbiVersion;                                                                                            \
    }                                                                                                                                      \
    extern "C" uint64_t WasmAsyncCallStart(uint64_t arguments, uint64_t size) {                                                            \
        return reinterpret_cast<uint64_t>(                                                                                                 \
            new NYdb::NWasm::NServices::TModuleCall(Handler, reinterpret_cast<const void*>(arguments), size));                             \
    }                                                                                                                                      \
    extern "C" void WasmAsyncCallPoll(uint64_t frame) {                                                                                    \
        reinterpret_cast<NYdb::NWasm::NServices::TModuleCall*>(frame)->Poll();                                                             \
    }                                                                                                                                      \
    extern "C" void WasmAsyncCallCancel(uint64_t frame) {                                                                                  \
        reinterpret_cast<NYdb::NWasm::NServices::TModuleCall*>(frame)->Task.reset();                                                       \
    }                                                                                                                                      \
    extern "C" void WasmAsyncCallDrop(uint64_t frame) {                                                                                    \
        delete reinterpret_cast<NYdb::NWasm::NServices::TModuleCall*>(frame);                                                              \
    }
