#pragma once

#include <ydb/core/tx/schemeshard/operation_queue_timer.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/schemeshard_types.h>
#include <ydb/core/util/circular_queue.h>

#include <util/generic/ptr.h>

namespace NKikimrConfig {

class TDataErasureConfig;

} // NKikimrConfig

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TTenantShredManager {
private:
using TQueue = NOperationQueue::TOperationQueueWithTimer<
    TShardIdx,
    TFifoQueue<TShardIdx>,
    TEvPrivate::EvRunTenantShred,
    NKikimrServices::FLAT_TX_SCHEMESHARD,
    NKikimrServices::TActivity::SCHEMESHARD_TENANT_SHRED>;

    class TStarter : public TQueue::IStarter {
    public:
        TStarter(TTenantShredManager* const manager);

        NOperationQueue::EStartStatus StartOperation(const TShardIdx& shardIdx) override;
        void OnTimeout(const TShardIdx& shardIdx) override;

    private:
        TTenantShredManager* const Manager;
    };

private:
    TSchemeShard* const SchemeShard = nullptr;
    EShredStatus Status = EShredStatus::COMPLETED;
    ui64 Generation = 0;
    ui64 CompletedGeneration = 0;
    TStarter Starter;
    TQueue* Queue = nullptr;
    THashMap<TShardIdx, EShredStatus> WaitingShredShards;
    THashMap<TShardIdx, TActorId> ActivePipes;

    ::NMonitoring::TDynamicCounters::TCounterPtr WaitingShardsCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr RunnigShardsCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompletedShardsCounter;

public:
    TTenantShredManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config);

    void UpdateConfig(const NKikimrConfig::TDataErasureConfig& config);
    void Start();
    void Stop();
    void Clear() {
        Queue->Clear();
        WaitingShredShards.clear();
        ActivePipes.clear();
    }
    void StartShred(NIceDb::TNiceDb& db, ui64 newGen);
    void StartShredForNewShards(NIceDb::TNiceDb& db, const std::vector<TShardIdx>& shredShards);
    void HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx);
    void FinishShred(NIceDb::TNiceDb& db, const TTabletId& tabletId);
    void RetryShred(const TTabletId& tabletId);
    bool Restore(NIceDb::TNiceDb& db);
    bool StopWaitingShred(const TShardIdx& shardIdx);
    ui64 GetGeneration() const {return Generation;}
    void SetGeneration(ui64 newGeneration) {Generation = newGeneration;}
    ui64 GetCompletedGeneration() {return CompletedGeneration;}
    EShredStatus GetStatus() const {return Status;}
    void SetStatus(const EShredStatus& status) {Status = status;}

private:
    static TQueue::TConfig ConvertConfig(const NKikimrConfig::TDataErasureConfig& config);

    NOperationQueue::EStartStatus StartShredOperation(const TShardIdx& shardIdx);
};

} // NKikimr::NSchemeShard
