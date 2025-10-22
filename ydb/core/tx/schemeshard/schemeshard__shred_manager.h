#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/operation_queue_timer.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/util/circular_queue.h>

#include <util/generic/ptr.h>

namespace NKikimrConfig {

class TDataErasureConfig;

} // NKikimrConfig

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TShredManager {
protected:
    TSchemeShard* const SchemeShard;
    EShredStatus Status = EShredStatus::UNSPECIFIED;
    ui64 Generation = 0;
    bool Running = false;

    ui64 CounterShredOk = 0;
    ui64 CounterShredTimeout = 0;

public:
    TShredManager(TSchemeShard* const schemeShard);

    virtual ~TShredManager() = default;

    virtual void UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) = 0;
    virtual void Start();
    virtual void Stop();
    virtual void ClearOperationQueue() = 0;
    virtual void ClearWaitingShredRequests(NIceDb::TNiceDb& db) = 0;
    virtual void ClearWaitingShredRequests() = 0;
    virtual void WakeupToRunShred(TEvSchemeShard::TEvWakeupToRunShred::TPtr& ev, const NActors::TActorContext& ctx) = 0;
    virtual void Run(NIceDb::TNiceDb& db) = 0;
    virtual void Continue() = 0;
    virtual void HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) = 0;
    virtual void OnDone(const TPathId& pathId, NIceDb::TNiceDb& db) = 0;
    virtual void OnDone(const TTabletId& tabletId, NIceDb::TNiceDb& db) = 0;
    virtual void ScheduleRequestToBSC() = 0;
    virtual void SendRequestToBSC() = 0;
    virtual void Complete() = 0;
    virtual bool Restore(NIceDb::TNiceDb& db) = 0;
    virtual bool Remove(const TPathId& pathId) = 0;
    virtual bool Remove(const TShardIdx& shardIdx) = 0;
    virtual void HandleNewPartitioning(const std::vector<TShardIdx>& shredShards, NIceDb::TNiceDb& db) = 0;
    virtual void SyncBscGeneration(NIceDb::TNiceDb& db, ui64 currentBscGeneration) = 0;

    void Clear();

    EShredStatus GetStatus() const;
    void SetStatus(const EShredStatus& status);

    void IncGeneration();
    void SetGeneration(ui64 generation);
    ui64 GetGeneration() const;

    bool IsRunning() const;
};

//////////////////// TRootShredManager ////////////////////

class TRootShredManager : public TShredManager {
private:
using TQueue = NOperationQueue::TOperationQueueWithTimer<
    TPathId,
    TFifoQueue<TPathId>,
    TEvPrivate::EvRunShred,
    NKikimrServices::FLAT_TX_SCHEMESHARD,
    NKikimrServices::TActivity::SCHEMESHARD_SHRED>;

    class TStarter : public TQueue::IStarter {
    public:
        TStarter(TRootShredManager* const manager);

        NOperationQueue::EStartStatus StartOperation(const TPathId&) override;
        void OnTimeout(const TPathId&) override;

    private:
        TRootShredManager* const Manager;
    };

private:
    TStarter Starter;
    TQueue* Queue = nullptr;
    THashMap<TPathId, EShredStatus> WaitingShredTenants;
    THashMap<TPathId, TActorId> ActivePipes;

    TDuration ShredInterval;
    TDuration ShredBSCInterval;
    TDuration CurrentWakeupInterval;
    bool IsShredWakeupScheduled = false;
    bool IsRequestToBSCScheduled = false;
    TInstant StartTime;
    TInstant FinishTime;

    TTabletId BSC;
    bool IsManualStartup = false;

public:
    TRootShredManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config);

    void UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) override;
    void Start() override;
    void Stop() override;
    void ClearOperationQueue() override;
    void ClearWaitingShredRequests(NIceDb::TNiceDb& db) override;
    void ClearWaitingShredRequests() override;
    void WakeupToRunShred(TEvSchemeShard::TEvWakeupToRunShred::TPtr& ev, const NActors::TActorContext& ctx) override;
    void Run(NIceDb::TNiceDb& db) override;
    void Continue() override;
    void HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) override;
    void OnDone(const TPathId& pathId, NIceDb::TNiceDb& db) override;
    void OnDone(const TTabletId& tabletId, NIceDb::TNiceDb& db) override;
    void ScheduleRequestToBSC() override;
    void SendRequestToBSC() override;
    void Complete() override;
    bool Restore(NIceDb::TNiceDb& db) override;
    bool Remove(const TPathId& pathId) override;
    bool Remove(const TShardIdx& shardIdx) override;
    void HandleNewPartitioning(const std::vector<TShardIdx>& shredShards, NIceDb::TNiceDb& db) override;
    void SyncBscGeneration(NIceDb::TNiceDb& db, ui64 currentBscGeneration) override;

private:
    static TQueue::TConfig ConvertConfig(const NKikimrConfig::TDataErasureConfig& config);

    void ScheduleShredWakeup();
    NOperationQueue::EStartStatus StartShred(const TPathId& pathId);
    void OnTimeout(const TPathId& pathId);
    void Enqueue(const TPathId& pathId);
    void UpdateMetrics();
};

//////////////////// TTenantShredManager ////////////////////

class TTenantShredManager : public TShredManager {
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
    TStarter Starter;
    TQueue* Queue = nullptr;
    THashMap<TShardIdx, EShredStatus> WaitingShredShards;
    THashMap<TShardIdx, TActorId> ActivePipes;

public:
    TTenantShredManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config);

    void UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) override;
    void Start() override;
    void Stop() override;
    void ClearOperationQueue() override;
    void ClearWaitingShredRequests(NIceDb::TNiceDb& db) override;
    void ClearWaitingShredRequests() override;
    void WakeupToRunShred(TEvSchemeShard::TEvWakeupToRunShred::TPtr& ev, const NActors::TActorContext& ctx) override;
    void Run(NIceDb::TNiceDb& db) override;
    void Continue() override;
    void HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) override;
    void OnDone(const TPathId& pathId, NIceDb::TNiceDb& db) override;
    void OnDone(const TTabletId& tabletId, NIceDb::TNiceDb& db) override;
    void ScheduleRequestToBSC() override;
    void SendRequestToBSC() override;
    void Complete() override;
    bool Restore(NIceDb::TNiceDb& db) override;
    bool Remove(const TPathId& pathId) override;
    bool Remove(const TShardIdx& shardIdx) override;
    void HandleNewPartitioning(const std::vector<TShardIdx>& shredShards, NIceDb::TNiceDb& db) override;
    void SyncBscGeneration(NIceDb::TNiceDb& db, ui64 currentBscGeneration) override;

private:
    static TQueue::TConfig ConvertConfig(const NKikimrConfig::TDataErasureConfig& config);

    NOperationQueue::EStartStatus StartShred(const TShardIdx& shardIdx);
    void OnTimeout(const TShardIdx& shardIdx);
    void Enqueue(const TShardIdx& shardIdx);
    void UpdateMetrics();
    void SendResponseToRootSchemeShard();
};

} // NKikimr::NSchemeShard
