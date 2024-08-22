#pragma once
#include "kqp_scan_common.h"
#include "kqp_compute_actor.h"
#include "kqp_compute_state.h"
#include "kqp_scan_compute_stat.h"
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/wilson/wilson_profile_span.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/base/tablet_pipecache.h>
#include "kqp_scan_events.h"

namespace NKikimr::NKqp::NScanPrivate {

class IExternalObjectsProvider {
public:
    virtual std::unique_ptr<TEvDataShard::TEvKqpScan> BuildEvKqpScan(const ui32 scanId, const ui32 gen, const TSmallVec<TSerializedTableRange>& ranges) const = 0;
    virtual const TVector<NScheme::TTypeInfo>& GetKeyColumnTypes() const = 0;
};

class TComputeTaskData;

class TShardScannerInfo {
private:
    std::optional<TActorId> ActorId;
    const ui64 ScanId;
    const ui64 TabletId;
    const ui64 Generation;
    i64 DataChunksInFlightCount = 0;
    bool TracingStarted = false;
    const ui64 FreeSpace = (ui64)8 << 20;
    bool NeedAck = true;
    bool Finished = false;

    void DoAck() {
        if (Finished) {
            AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "scan_ack_on_finished")("actor_id", ActorId);
            return;
        }
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "scan_ack")("actor_id", ActorId);
        AFL_ENSURE(NeedAck);
        NeedAck = false;
        AFL_ENSURE(ActorId);
        AFL_ENSURE(!DataChunksInFlightCount)("data_chunks_in_flight_count", DataChunksInFlightCount);
        ui32 flags = IEventHandle::FlagTrackDelivery;
        if (!TracingStarted) {
            flags |= IEventHandle::FlagSubscribeOnSession;
            TracingStarted = true;
        }
        if (NActors::TlsActivationContext) {
            NActors::TActivationContext::AsActorContext().Send(*ActorId, new TEvKqpCompute::TEvScanDataAck(FreeSpace, Generation, 1), flags, TabletId);
        }
    }
public:
    TShardScannerInfo(const ui64 scanId, TShardState& state, const IExternalObjectsProvider& externalObjectsProvider)
        : ScanId(scanId)
        , TabletId(state.TabletId)
        , Generation(++state.Generation)
    {
        const bool subscribed = std::exchange(state.SubscribedOnTablet, true);

        const auto& keyColumnTypes = externalObjectsProvider.GetKeyColumnTypes();
        auto ranges = state.GetScanRanges(keyColumnTypes);
        auto ev = externalObjectsProvider.BuildEvKqpScan(ScanId, Generation, ranges);

        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "start_scanner")("tablet_id", TabletId)("generation", Generation)
            ("info", state.ToString(keyColumnTypes))("range", DebugPrintRanges(keyColumnTypes, ranges, *AppData()->TypeRegistry))
            ("subscribed", subscribed);

        NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(ev.release(), TabletId, !subscribed), IEventHandle::FlagTrackDelivery);
    }

    void Stop(const bool finalFlag, const TString& message) {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "stop_scanner")("actor_id", ActorId)("message", message)("final_flag", finalFlag);
        if (ActorId) {
            auto abortEv = std::make_unique<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, message ? message : "stop from fetcher");
            NActors::TActivationContext::AsActorContext().Send(*ActorId, std::move(abortEv));
            if (finalFlag) {
                NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(TabletId));
                NActors::TActivationContext::AsActorContext().Send(TActivationContext::InterconnectProxy(ActorId->NodeId()), new TEvents::TEvUnsubscribe());
            }
            ActorId = {};
        }
    }

    void Start(const TActorId& actorId) {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "start_scanner")("actor_id", actorId);
        AFL_ENSURE(!ActorId);
        ActorId = actorId;
        DoAck();
    }

    std::vector<std::unique_ptr<TComputeTaskData>> OnReceiveData(TEvKqpCompute::TEvScanData& data, const std::shared_ptr<TShardScannerInfo>& selfPtr);

    void FinishWaitSendData() {
        --DataChunksInFlightCount;
        AFL_ENSURE(DataChunksInFlightCount >= 0)("data_chunks_in_flight_count", DataChunksInFlightCount);
        if (!DataChunksInFlightCount && !!ActorId) {
            DoAck();
        }
    }
};

class TComputeTaskData {
private:
    std::shared_ptr<TShardScannerInfo> Info;
    std::unique_ptr<TEvScanExchange::TEvSendData> Event;
    bool Finished = false;
    const std::optional<ui32> ComputeShardId;
public:
    ui32 GetRowsCount() const {
        AFL_ENSURE(Event);
        return Event->GetRowsCount();
    }

    std::unique_ptr<TEvScanExchange::TEvSendData> ExtractEvent() {
        return std::move(Event);
    }

    const std::optional<ui32>& GetComputeShardId() const {
        return ComputeShardId;
    }

    TComputeTaskData(const std::shared_ptr<TShardScannerInfo>& info, std::unique_ptr<TEvScanExchange::TEvSendData>&& ev, const std::optional<ui32> computeShardId = std::optional<ui32>())
        : Info(info)
        , Event(std::move(ev))
        , ComputeShardId(computeShardId)
    {
    }

    void Finish() {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "stop_scanner");
        AFL_ENSURE(!Finished);
        Finished = true;
        Info->FinishWaitSendData();
    }
};

class TInFlightComputes {
public:
    class TComputeActorInfo {
    private:
        YDB_READONLY_DEF(NActors::TActorId, ActorId);
        YDB_READONLY(ui64, FreeSpace, 0);
        std::deque<std::unique_ptr<TComputeTaskData>> DataQueue;

        bool SendData() {
            AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "send_data_to_compute")("space", FreeSpace)("queue", DataQueue.size())("compute_actor_id", ActorId)
                ("rows", DataQueue.size() ? DataQueue.front()->GetRowsCount() : 0);
            if (FreeSpace && DataQueue.size()) {
                NActors::TActivationContext::AsActorContext().Send(ActorId, DataQueue.front()->ExtractEvent());
                DataQueue.front()->Finish();
                DataQueue.pop_front();
                FreeSpace = 0;
                return true;
            }
            return false;
        }

    public:
        ui32 GetPacksToSendCount() const {
            return DataQueue.size();
        }

        TComputeActorInfo(const NActors::TActorId& actorId)
            : ActorId(actorId) {

        }

        void OnAckReceived(const ui64 freeSpace) {
            AFL_ENSURE(!FreeSpace);
            AFL_ENSURE(freeSpace);
            FreeSpace = freeSpace;
            SendData();
        }

        void AddDataToSend(std::unique_ptr<TComputeTaskData>&& sendTask) {
            AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "add_data_to_compute")("rows", sendTask->GetRowsCount());
            DataQueue.emplace_back(std::move(sendTask));
            SendData();
        }

        bool IsFree() const {
            return FreeSpace && DataQueue.empty();
        }
    };

private:
    std::deque<std::unique_ptr<TComputeTaskData>> UndefinedShardTaskData;
    std::deque<TComputeActorInfo> ComputeActors;
    THashMap<TActorId, TComputeActorInfo*> ComputeActorsById;
public:
    ui32 GetPacksToSendCount() const {
        ui32 result = UndefinedShardTaskData.size();
        for (auto&& i : ComputeActors) {
            result += i.GetPacksToSendCount();
        }
        return result;
    }

    TInFlightComputes(const std::vector<NActors::TActorId>& computeActorIds) {
        for (auto&& i : computeActorIds) {
            ComputeActors.emplace_back(i);
            ComputeActorsById.emplace(i, &ComputeActors.back());
        }
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "ca=" << ComputeActors.size() << ";"
            ;
        return sb;
    }

    bool OnComputeAck(const TActorId& computeActorId, const ui64 freeSpace) {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "ack")("compute_actor_id", computeActorId);
        auto it = ComputeActorsById.find(computeActorId);
        AFL_ENSURE(it != ComputeActorsById.end())("compute_actor_id", computeActorId);
        if (it->second->IsFree()) {
            return false;
        }
        it->second->OnAckReceived(freeSpace);
        if (it->second->IsFree() && UndefinedShardTaskData.size()) {
            it->second->AddDataToSend(std::move(UndefinedShardTaskData.front()));
            UndefinedShardTaskData.pop_front();
        }
        return true;
    }

    void OnReceiveData(const std::optional<ui32> computeShardId, std::unique_ptr<TComputeTaskData>&& sendTask) {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "on_receive")("compute_shard_id", computeShardId);
        if (!computeShardId) {
            for (auto&& i : ComputeActors) {
                if (i.IsFree()) {
                    i.AddDataToSend(std::move(sendTask));
                    return;
                }
            }
            UndefinedShardTaskData.emplace_back(std::move(sendTask));
        } else {
            AFL_ENSURE(*computeShardId < ComputeActors.size())("compute_shard_id", *computeShardId);
            ComputeActors[*computeShardId].AddDataToSend(std::move(sendTask));
        }
    }
};

class TInFlightShards: public NComputeActor::TScanShardsStatistics {
private:
    using TTabletsData = THashMap<ui64, TShardState::TPtr>;
    TTabletsData Shards;
    THashMap<NActors::TActorId, TShardState::TPtr> ShardsByActorId;
    bool IsActiveFlag = true;
    THashMap<ui64, std::shared_ptr<TShardScannerInfo>> ShardScanners;
    const ui64 ScanId;
    const IExternalObjectsProvider& ExternalObjectsProvider;
public:

    void AbortAllScanners(const TString& errorMessage) {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "abort_all_scanners")("error_message", errorMessage);
        for (auto&& itTablet : ShardScanners) {
            itTablet.second->Stop(true, errorMessage);
        }
    }

    std::shared_ptr<TShardState> GetShardStateByActorId(const NActors::TActorId& actorId) const {
        auto it = ShardsByActorId.find(actorId);
        if (it == ShardsByActorId.end()) {
            return nullptr;
        }
        return it->second;
    }

    const std::shared_ptr<TShardState>& GetShardStateVerified(const ui64 tabletId) const {
        auto it = Shards.find(tabletId);
        AFL_ENSURE(it != Shards.end())("tablet_id", tabletId);
        return it->second;
    }

    std::shared_ptr<TShardState> GetShardState(const ui64 tabletId) const {
        auto it = Shards.find(tabletId);
        if (it != Shards.end()) {
            return it->second;
        } else {
            return nullptr;
        }
    }

    void RegisterScannerActor(const ui64 tabletId, const ui64 generation, const TActorId& scanActorId) {
        auto state = GetShardState(tabletId);
        if (!state || generation != state->Generation) {
            AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "register_scanner_actor_dropped")
                ("actor_id", scanActorId)("is_state_initialized", !!state)("generation", generation);
            return;
        }

        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "register_scanner_actor")("actor_id", scanActorId)
            ("state", state->State)("tablet_id", state->TabletId)("generation", state->Generation);

        AFL_ENSURE(state->State == NComputeActor::EShardState::Starting)("state", state->State);
        AFL_ENSURE(!state->ActorId)("actor_id", state->ActorId);

        state->State = NComputeActor::EShardState::Running;
        state->ActorId = scanActorId;
        state->ResetRetry();
        AFL_ENSURE(ShardsByActorId.emplace(scanActorId, state).second);

        GetShardScannerVerified(tabletId)->Start(scanActorId);
    }

    void StartScanner(TShardState& state) {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "start_scanner")
            ("state", state.State)("tablet_id", state.TabletId)("generation", state.Generation);

        AFL_ENSURE(state.State == NComputeActor::EShardState::Initial)("state", state.State);
        AFL_ENSURE(state.TabletId);
        AFL_ENSURE(!state.ActorId)("actor_id", state.ActorId);
        state.State = NComputeActor::EShardState::Starting;
        auto newScanner = std::make_shared<TShardScannerInfo>(ScanId, state, ExternalObjectsProvider);
        AFL_ENSURE(ShardScanners.emplace(state.TabletId, newScanner).second);
    }

    void StopScanner(const ui64 tabletId, const bool stopShard = true) {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "scanner_finished")("tablet_id", tabletId)("stop_shard", stopShard);
        auto& state = GetShardStateVerified(tabletId);
        const auto actorId = state->ActorId;
        if (actorId) {
            AFL_ENSURE(ShardsByActorId.erase(actorId));
        }
        if (state->State != NComputeActor::EShardState::Initial) {
            state->RetryAttempt++;
            state->TotalRetries++;
            state->ActorId = {};
            state->State = NComputeActor::EShardState::Initial;
            state->SubscribedOnTablet = false;

            auto it = ShardScanners.find(tabletId);
            AFL_ENSURE(it != ShardScanners.end())("tablet_id", tabletId);
            it->second->Stop(true, "");
            ShardScanners.erase(it);
        }

        if (stopShard) {
            AFL_ENSURE(Shards.erase(tabletId));
        }
    }

    const std::shared_ptr<TShardScannerInfo>& GetShardScannerVerified(const ui64 tabletId) const {
        auto it = ShardScanners.find(tabletId);
        AFL_ENSURE(it != ShardScanners.end())("tablet_id", tabletId);
        return it->second;
    }

    std::shared_ptr<TShardScannerInfo> GetShardScanner(const ui64 tabletId) const {
        auto it = ShardScanners.find(tabletId);
        if (it != ShardScanners.end()) {
            return it->second;
        }
        return nullptr;
    }

    TInFlightShards(const ui64 scanId, const IExternalObjectsProvider& externalObjectsProvider)
        : ScanId(scanId)
        , ExternalObjectsProvider(externalObjectsProvider)
    {
    }
    bool IsActive() const {
        return IsActiveFlag;
    }
    void Stop() {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "wait_all_scanner_finished")("scans", GetScansCount());
        Y_ABORT_UNLESS(GetScansCount() == 0);
        IsActiveFlag = false;
    }

    ui32 GetScansCount() const {
        return ShardScanners.size();
    }
    ui32 GetShardsCount() const {
        return Shards.size();
    }
    TShardState::TPtr Put(TShardState&& state);
};

}
