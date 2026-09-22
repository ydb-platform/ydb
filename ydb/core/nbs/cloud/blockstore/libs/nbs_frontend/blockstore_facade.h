#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/partition_session_state.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service_method.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/system/mutex.h>

#include <atomic>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {
struct IPartitionSessionControl;
}

namespace NYdb::NBS::NBlockStore {

// Adapts classic RPCs and owns the host-local DiskId -> partition registry.
// Session ownership stays with each partition, not with this shared facade.
class TNbsBlockStoreFacade final
    : public NNbs1CompatApi::NBlockStore::TBlockStoreImpl<
          TNbsBlockStoreFacade,
          NNbs1CompatApi::NBlockStore::IBlockStore>
{
public:
    // Creates an empty registry with request admission closed.
    explicit TNbsBlockStoreFacade(TLog log);
    ~TNbsBlockStoreFacade() noexcept override;

    // Opens admission without changing partition sessions.
    void Start() override;

    // Closes admission only; live partitions retain their sessions.
    void Stop() override;

    // Classic callers own their protobuf buffers; no separate allocation pool.
    NNbs1CompatApi::NBlockStore::TStorageBuffer AllocateBuffer(
        size_t bytesCount) override;

    // Publishes a matched control target and session/backend incarnation.
    // Control must target the same incarnation as state and remain callable by
    // requests retaining this registration, even after removal or replacement.
    TResultOrError<TString> RegisterVolume(
        NStorage::NPartitionDirect::TPartitionSessionStateHolderPtr
            sessionState,
        std::shared_ptr<NStorage::NPartitionDirect::IPartitionSessionControl>
            sessionControl);

    // Removes only the specified disk incarnation; stale tokens are harmless.
    void UnregisterVolume(const TString& diskId, const TString& registrationId);

    // Adapts classic RPCs to partition session control and I/O.
    template <typename TMethod>
    NThreading::TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request);

private:
    struct TPartitionRegistration;
    struct TPartitionRegistry;

    TResultOrError<std::shared_ptr<const TPartitionRegistration>> FindPartition(
        const TString& diskId) const;

    NThreading::TFuture<
        NNbs1CompatApi::NBlockStore::NProto::TMountVolumeResponse>
    ExecuteMountVolume(
        const NNbs1CompatApi::NBlockStore::NProto::TMountVolumeRequest& request,
        const TPartitionRegistration& registration);

    NThreading::TFuture<
        NNbs1CompatApi::NBlockStore::NProto::TUnmountVolumeResponse>
    ExecuteUnmountVolume(
        const NNbs1CompatApi::NBlockStore::NProto::TUnmountVolumeRequest&
            request,
        const TPartitionRegistration& registration);

    NThreading::TFuture<
        NNbs1CompatApi::NBlockStore::NProto::TReadBlocksResponse>
    ExecuteReadBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NNbs1CompatApi::NBlockStore::NProto::TReadBlocksRequest>
            request,
        NStorage::NPartitionDirect::TPartitionIoBackend backend);

    NThreading::TFuture<
        NNbs1CompatApi::NBlockStore::NProto::TWriteBlocksResponse>
    ExecuteWriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<
            NNbs1CompatApi::NBlockStore::NProto::TWriteBlocksRequest> request,
        NStorage::NPartitionDirect::TPartitionIoBackend backend);

    std::atomic_bool AcceptingRequests = false;
    // Serializes registry updates and admission closure with session dispatch.
    TMutex RegistryMutex;
    // Publishes only partition registrations; read-modify-write needs the
    // mutex.
    THotSwap<TPartitionRegistry> PartitionRegistry;
    TLog Log;
};

// Creates one facade shared by TNbsService and transport services.
std::shared_ptr<TNbsBlockStoreFacade> CreateNbsBlockStoreFacade(TLog log);

}   // namespace NYdb::NBS::NBlockStore
