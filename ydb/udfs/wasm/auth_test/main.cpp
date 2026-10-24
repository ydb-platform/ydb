#include <ydb/udfs/wasm/sdk/services/auth.h>
#include <ydb/udfs/wasm/sdk/services/example_allocator.h>
#include <ydb/udfs/wasm/sdk/services/module.h>

using namespace NYdb::NWasm::NAsync;
using namespace NYdb::NWasm::NServices;
namespace NAuth = NYdb::NWasm::NAuth;

namespace {
TModuleReply Failure() {
    return { EOperationStatus::Failed, {} };
}

// Synthetic protocol only: POST the test secret to the configured bootstrap
// endpoint. Its response contains the public auth-result envelope and fake ticket.
TTask<TModuleReply> Run(TCallContext& context, const void* arguments, size_t size) {
    TRowReader input({ static_cast<const char*>(arguments), size });
    NAuth::TRequest header;
    if (!input.Get(header) || header.AbiVersion != NAuth::Version || header.Reserved || header.ParametersBytes > NAuth::MaxParametersBytes ||
        header.SecretCount != 1 || input.Remaining().size() != header.ParametersBytes + sizeof(uint64_t)) {
        co_return Failure();
    }
    const auto bytes = input.Remaining();
    uint64_t handle;
    std::memcpy(&handle, bytes.data() + header.ParametersBytes, sizeof(handle));
    TModuleBytes secret;
    {
        auto operation = TOperation::Secret(handle);
        if (co_await operation.Wait(context) != EOperationStatus::Ready || !operation.Size() || operation.Size() > 4096) {
            co_return Failure();
        }
        secret = TModuleBytes(operation.Size());
        operation.Read(secret.Buffer.get(), secret.Size);
    }
    auto request = ModuleRequest(0, secret.View(), "POST", "/auth", "auth.test");
    TOperation operation(request.Buffer.get(), request.Size);
    if (co_await operation.Wait(context) != EOperationStatus::Ready ||
        operation.Size() > sizeof(TResponseHeader) + sizeof(NAuth::TResult) + NAuth::MaxTicketBytes) {
        co_return Failure();
    }
    TModuleBytes response(operation.Size());
    operation.Read(response.Buffer.get(), response.Size);
    TResponseHeader transport;
    std::string_view payload;
    NAuth::TResult result;
    std::string_view ticket;
    if (!Decode(response.View(), transport, payload) || transport.Error != EClientError::None || transport.Code != 200 ||
        !NAuth::ReadResult(payload, result, ticket)) {
        co_return Failure();
    }
    co_return TModuleReply{ EOperationStatus::Ready, ModulePack(result, ticket) };
}
}   // namespace

WASM_SERVICE_MODULE(Run)
