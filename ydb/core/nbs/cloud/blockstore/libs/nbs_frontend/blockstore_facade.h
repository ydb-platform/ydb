#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/partition_session_state.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service_method.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>

#include <util/system/mutex.h>

namespace NActors {
class TActorSystem;
struct TActorId;
}   // namespace NActors

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
    // The first registration binds this facade to one actor system.
    TResultOrError<TString> RegisterVolume(
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& actorId,
        std::shared_ptr<NStorage::NPartitionDirect::TPartitionSessionState>
            sessionState);

    // Removes only the specified disk incarnation; stale tokens are harmless.
    void UnregisterVolume(const TString& diskId, const TString& registrationId);

    // Dispatches RPCs; mount/unmount execute on the owning partition actor.
    template <typename TMethod>
    NThreading::TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request);

private:
    struct TPartitionRegistration;
    struct TSnapshot;

    TResultOrError<std::shared_ptr<const TPartitionRegistration>> FindPartition(
        const TString& diskId) const;

    template <typename TEvent>
    auto SendSessionRequest(
        const std::shared_ptr<const TPartitionRegistration>& partition,
        std::unique_ptr<TEvent> event);

    NThreading::TFuture<
        NNbs1CompatApi::NBlockStore::NProto::TMountVolumeResponse>
    ExecuteMountVolume(
        const NNbs1CompatApi::NBlockStore::NProto::TMountVolumeRequest& request,
        std::shared_ptr<const TPartitionRegistration> partition);

    NThreading::TFuture<
        NNbs1CompatApi::NBlockStore::NProto::TUnmountVolumeResponse>
    ExecuteUnmountVolume(
        const NNbs1CompatApi::NBlockStore::NProto::TUnmountVolumeRequest&
            request,
        std::shared_ptr<const TPartitionRegistration> partition);

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

    // Serializes registry/admission updates and session request dispatch.
    TMutex RegistryMutex;
    // Non-owning; all registered partitions belong to this actor system.
    NActors::TActorSystem* ActorSystem = nullptr;
    TTrueAtomicSharedPtr<TSnapshot> Snapshot;
    TLog Log;
};

// Creates one facade shared by TNbsService and transport services.
std::shared_ptr<TNbsBlockStoreFacade> CreateNbsBlockStoreFacade(TLog log);

}   // namespace NYdb::NBS::NBlockStore
