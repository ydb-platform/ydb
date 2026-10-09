#include <ydb/udfs/wasm/sdk/services/module.h>
#include <ydb/udfs/wasm/sdk/services/example_allocator.h>
#include <ydb/udfs/wasm/echo/contract/service_methods.h>

using namespace NYdb::NWasm::NAsync;
using namespace NYdb::NWasm::NServices;
using namespace NYdb::NWasm::NServices::NGenerated::NModuleEcho;

namespace {

TModuleReply Failure(uint32_t error, uint32_t detail = 0) {
    return {EOperationStatus::Ready, ModulePack(TServiceResult{ServiceVersion, 0, error, detail})};
}

TTask<TModuleReply> Run(TCallContext& context, const void* arguments, size_t size) {
    TServiceRequest header;
    std::string_view rows;
    if (!ReadServiceRequest({static_cast<const char*>(arguments), size}, header, rows) || header.Protocol ||
        (header.Method != MethodEcho && header.Method != MethodLength))
        co_return Failure(1);
    std::string_view messages[MaxServiceBatchRows];
    TRowReader input(rows);
    for (uint32_t i = 0; i < header.Count; ++i)
        if (!input.String(messages[i], 1024))
            co_return Failure(1);
    if (!input.Remaining().empty())
        co_return Failure(1);

    // This example's HTTP service echoes count + length-prefixed binary strings.
    auto body = ModulePack(header.Count, rows);
    auto request = ModulePack(TRequestHeader{WireVersion, header.Binding, body.Size}, body.View());
    TOperation operation(request.Buffer.get(), request.Size);
    const auto status = co_await operation.Wait(context);
    if (operation.Size() > sizeof(TResponseHeader) + header.MaxBytes)
        co_return Failure(2);
    TModuleBytes bytes(operation.Size());
    operation.Read(bytes.Buffer.get(), bytes.Size);
    TResponseHeader response;
    std::string_view payload;
    if (!Decode(bytes.View(), response, payload) || response.Version != WireVersion || response.PayloadBytes != payload.size() ||
        status != EOperationStatus::Ready || response.Error != EClientError::None)
        co_return Failure(2);

    TRowReader reply(payload);
    uint32_t count;
    if (!reply.Get(count) || count != header.Count)
        co_return Failure(3);
    TModuleBytes output(header.MaxBytes);
    TRowWriter writer(output.Buffer.get(), output.Size);
    writer.Put(TServiceResult{ServiceVersion, count});
    for (uint32_t i = 0; i < count; ++i) {
        std::string_view message;
        if (!reply.String(message, 1024) || message != messages[i])
            co_return Failure(3);
        if (header.Method == MethodEcho) {
            if (!writer.String(message) || !writer.Put(uint64_t(message.size())) || !writer.Put(uint8_t(message.empty())) ||
                !writer.Put(-static_cast<int64_t>(message.size())))
                co_return Failure(4);
        } else if (!writer.Put(uint32_t(message.size())))
            co_return Failure(4);
    }
    if (!reply.Remaining().empty())
        co_return Failure(3);
    output.Size = writer.Size();
    co_return TModuleReply{EOperationStatus::Ready, std::move(output)};
}

} // namespace

WASM_SERVICE_MODULE(Run)

extern "C" uint64_t WasmAsyncLiveObjects() {
    return NYdb::NWasm::NServices::NExampleAllocator::LiveObjects;
}
