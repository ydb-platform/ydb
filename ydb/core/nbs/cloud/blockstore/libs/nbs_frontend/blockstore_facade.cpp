#include "blockstore_facade.h"

#include "frontend_state.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service_method.h>

#include <util/string/builder.h>
#include <util/system/yassert.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Implements the classic IBlockStore boundary for the NBS2 frontend skeleton.
class TNbsFrontendBlockStore final
    : public NYdb::NBS::NNbs1CompatApi::NBlockStore::TBlockStoreImpl<
          TNbsFrontendBlockStore,
          NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStore>
{
public:
    // Shares metadata, admission and session state with the frontend runtime.
    explicit TNbsFrontendBlockStore(
        std::shared_ptr<TFrontendState> frontendState,
        TLog log);

    // Opens the admission gate for requests.
    void Start() override;

    // Closes the admission gate and revokes the active session.
    void Stop() override;

    // The skeleton does not allocate data-path buffers.
    NYdb::NBS::NNbs1CompatApi::NBlockStore::TStorageBuffer AllocateBuffer(
        size_t bytesCount) override;

    // Handles control requests and validates I/O before the backend is
    // connected.
    template <typename TMethod>
    NThreading::TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request);

private:
    const std::shared_ptr<TFrontendState> FrontendState;
    TLog Log;
};

////////////////////////////////////////////////////////////////////////////////

TNbsFrontendBlockStore::TNbsFrontendBlockStore(
    std::shared_ptr<TFrontendState> frontendState,
    TLog log)
    : FrontendState(std::move(frontendState))
    , Log(std::move(log))
{
    Y_ABORT_UNLESS(FrontendState);
}

void TNbsFrontendBlockStore::Start()
{
    FrontendState->Start();
}

void TNbsFrontendBlockStore::Stop()
{
    FrontendState->Stop();
}

NYdb::NBS::NNbs1CompatApi::NBlockStore::TStorageBuffer
TNbsFrontendBlockStore::AllocateBuffer(size_t bytesCount)
{
    Y_UNUSED(bytesCount);
    return nullptr;
}

template <typename TMethod>
NThreading::TFuture<typename TMethod::TResponse>
TNbsFrontendBlockStore::Execute(
    TCallContextPtr callContext,
    std::shared_ptr<typename TMethod::TRequest> request)
{
    Y_UNUSED(callContext);

    STORAGE_DEBUG(
        TMethod::Name << " RequestId=" << request->GetHeaders().GetRequestId());

    using TResponse = typename TMethod::TResponse;

    TResponse response;
    if constexpr (
        std::is_same_v<
            TMethod,
            NNbs1CompatApi::NBlockStore::TBlockStoreMountVolumeMethod>)
    {
        response = FrontendState->MountVolume(*request);
    } else if constexpr (
        std::is_same_v<
            TMethod,
            NNbs1CompatApi::NBlockStore::TBlockStoreUnmountVolumeMethod>)
    {
        *response.MutableError() = FrontendState->UnmountVolume(
            request->GetDiskId(),
            request->GetHeaders().GetClientId(),
            request->GetSessionId());
    } else if constexpr (
        std::is_same_v<
            TMethod,
            NYdb::NBS::NNbs1CompatApi::NBlockStore::TBlockStorePingMethod>)
    {
        *response.MutableError() = FrontendState->CheckAcceptingRequests();
        if (HasError(response)) {
            STORAGE_DEBUG(
                "Ping RequestId=" << request->GetHeaders().GetRequestId()
                                  << " Error="
                                  << FormatError(response.GetError()));
        }
    } else {
        auto error = FrontendState->ValidateIoSession(
            request->GetDiskId(),
            request->GetHeaders().GetClientId(),
            request->GetSessionId());
        if (!HasError(error)) {
            error = MakeError(
                E_NOT_IMPLEMENTED,
                TStringBuilder()
                    << "NBS2 frontend does not implement " << TMethod::Name);
        }
        *response.MutableError() = std::move(error);
    }

    return NThreading::MakeFuture(std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
CreateNbsFrontendBlockStore(
    std::shared_ptr<TFrontendState> frontendState,
    TLog log)
{
    return std::make_shared<TNbsFrontendBlockStore>(
        std::move(frontendState),
        std::move(log));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
