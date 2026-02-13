#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/mon/mon.h>

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group/direct_block_group.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/protos/blockstore_config.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NYdb::NBS::NProto;
using namespace NKikimrBlockStore;

////////////////////////////////////////////////////////////////////////////////

class TPartitionActor
    : public TActorBootstrapped<TPartitionActor>
{
private:
    TStorageConfig StorageConfig;
    TVolumeConfig VolumeConfig;

    TActorId BSControllerPipeClient;

    std::unique_ptr<TDirectBlockGroup> DirectBlockGroup;

    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

    TIntrusivePtr<NMonitoring::TDynamicCounters> CountersBase;
    std::vector<std::pair<TString, TString>> CountersChain;

    struct {
        struct {
            NMonitoring::TDynamicCounters::TCounterPtr Requests;
            NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
            NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;
            NMonitoring::TDynamicCounters::TCounterPtr Bytes;

            void Request(ui32 bytes = 0) {
                if (Requests) {
                    ++*Requests;
                }
                if (bytes && Bytes) {
                    *Bytes += bytes;
                }
            }

            void Reply(bool ok, ui32 bytes = 0) {
                if (ok && ReplyOk) {
                    ++*ReplyOk;
                } else if (!ok && ReplyErr) {
                    ++*ReplyErr;
                }
                if (bytes && Bytes) {
                    *Bytes += bytes;
                }
            }
        } WriteBlocks, ReadBlocks;
    } Counters;

public:
    TPartitionActor(
        TStorageConfig storageConfig,
        TVolumeConfig volumeConfig,
        const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters = nullptr);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void CreateBSControllerPipeClient(const TActorContext& ctx);

    void AllocateDDiskBlockGroup(const TActorContext& ctx);

    void HandleControllerAllocateDDiskBlockGroupResult(
        const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteBlocksRequest(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadBlocksRequest(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    // Forward events to DirectBlockGroup
    void HandleDDiskConnectResult(
        const NDDisk::TEvConnectResult::TPtr& ev,
        const TActorContext& ctx);

    void HandlePersistentBufferWriteResult(
        const NDDisk::TEvWritePersistentBufferResult::TPtr& ev,
        const TActorContext& ctx);

    void HandlePersistentBufferFlushResult(
        const NDDisk::TEvFlushPersistentBufferResult::TPtr& ev,
        const TActorContext& ctx);

    void HandlePersistentBufferEraseResult(
        const NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
        const TActorContext& ctx);

    template <typename TEvent>
    void HandleReadResult(
        const typename TEvent::TPtr& ev,
        const TActorContext& ctx);

    template<typename TEvPtr>
    void AddTraceId(const TEvPtr& ev, const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

template<typename TEvPtr>
void TPartitionActor::AddTraceId(const TEvPtr& ev, const NActors::TActorContext& ctx)
{
    if (!ev->TraceId) {
        // Generate new trace id with throttling to avoid overwhelming the tracing system
        ev->TraceId = NWilson::TTraceId::NewTraceIdThrottled(
            15,                 // verbosity
            4095,               // timeToLive
            LastTraceTs,        // atomic counter for throttling
            ctx.Monotonic(),    // current monotonic time
            TraceSamplePeriod   // 100ms between samples
        );
    }
}

} // namespace NYdb::NBS:NBlockStore::NStorage::NPartitionDirect
