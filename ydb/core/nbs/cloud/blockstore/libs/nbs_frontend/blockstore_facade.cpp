#include "blockstore_facade.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/service_method.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>
#include <util/system/yassert.h>

#include <atomic>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf NotAcceptingRequestsMessage =
    "NBS2 frontend is not accepting requests";

// Implements the classic IBlockStore boundary for the NBS2 frontend skeleton.
class TNbsFrontendBlockStore final
    : public NCloud::NBlockStore::TBlockStoreImpl<
          TNbsFrontendBlockStore,
          NCloud::NBlockStore::IBlockStore>
{
public:
    // Opens the admission gate for requests.
    void Start() override
    {
        AcceptingRequests.store(true, std::memory_order_release);
    }

    // Closes the admission gate for requests.
    void Stop() override
    {
        AcceptingRequests.store(false, std::memory_order_release);
    }

    // The skeleton does not allocate data-path buffers.
    NCloud::NBlockStore::TStorageBuffer AllocateBuffer(
        size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    // Executes Ping or returns the controlled skeleton error for another RPC.
    template <typename TMethod>
    NThreading::TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        using TResponse = typename TMethod::TResponse;

        TResponse response;
        if (!AcceptingRequests.load(std::memory_order_acquire)) {
            *response.MutableError() =
                MakeError(E_REJECTED, TString(NotAcceptingRequestsMessage));
        } else if constexpr (
            !std::
                is_same_v<TMethod, NCloud::NBlockStore::TBlockStorePingMethod>)
        {
            *response.MutableError() = MakeError(
                E_NOT_IMPLEMENTED,
                TStringBuilder()
                    << "NBS2 frontend does not implement " << TMethod::Name);
        }

        return NThreading::MakeFuture(std::move(response));
    }

private:
    std::atomic<bool> AcceptingRequests = false;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

NCloud::NBlockStore::IBlockStorePtr CreateNbsFrontendBlockStore()
{
    return std::make_shared<TNbsFrontendBlockStore>();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
