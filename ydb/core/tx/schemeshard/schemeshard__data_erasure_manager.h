#pragma once

#include <util/generic/ptr.h>

#include <ydb/core/util/circular_queue.h>

#include <ydb/core/tx/schemeshard/operation_queue_timer.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>

#include <ydb/core/scheme/scheme_pathid.h>

namespace NKikimrConfig {

class TDataErasureConfig;

} // NKikimrConfig

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TDataErasureManager {
protected:
    TSchemeShard* const SchemeShard;
    EDataErasureStatus Status = EDataErasureStatus::UNSPECIFIED;
    ui64 Generation = 0;
    bool Running = false;

    ui64 CounterDataErasureOk = 0;
    ui64 CounterDataErasureTimeout = 0;

public:
    TDataErasureManager(TSchemeShard* const schemeShard);

    virtual ~TDataErasureManager() = default;

    virtual void UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) = 0;
    virtual void Start();
    virtual void Stop();
    virtual void ClearOperationQueue() = 0;
    virtual void ClearWaitingDataErasureRequests(NIceDb::TNiceDb& db) = 0;
    virtual void ClearWaitingDataErasureRequests() = 0;
    virtual void WakeupToRunDataErasure(TEvSchemeShard::TEvWakeupToRunDataErasure::TPtr& ev, const NActors::TActorContext& ctx) = 0;
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
    virtual void HandleNewPartitioning(const std::vector<TShardIdx>& dataErasureShards, NIceDb::TNiceDb& db) = 0;
    virtual void SyncBscGeneration(NIceDb::TNiceDb& db, ui64 currentBscGeneration) = 0;

    void Clear();

    EDataErasureStatus GetStatus() const;
    void SetStatus(const EDataErasureStatus& status);

    void IncGeneration();
    void SetGeneration(ui64 generation);
    ui64 GetGeneration() const;

    bool IsRunning() const;
};

//////////////////// TRootDataErasureManager ////////////////////

class TRootDataErasureManager : public TDataErasureManager {
private:
using TQueue = NOperationQueue::TOperationQueueWithTimer<
    TPathId,
    TFifoQueue<TPathId>,
    TEvPrivate::EvRunDataErasure,
    NKikimrServices::FLAT_TX_SCHEMESHARD,
    NKikimrServices::TActivity::DATA_ERASURE>;

    class TStarter : public TQueue::IStarter {
    public:
        TStarter(TRootDataErasureManager* const manager);

        NOperationQueue::EStartStatus StartOperation(const TPathId&) override;
        void OnTimeout(const TPathId&) override;

    private:
        TRootDataErasureManager* const Manager;
    };

private:
    TStarter Starter;
    TQueue* Queue = nullptr;
    THashMap<TPathId, EDataErasureStatus> WaitingDataErasureTenants;
    THashMap<TPathId, TActorId> ActivePipes;

    TDuration DataErasureInterval;
    TDuration DataErasureBSCInterval;
    TDuration CurrentWakeupInterval;
    bool IsDataErasureWakeupScheduled = false;
    bool IsRequestToBSCScheduled = false;
    TInstant StartTime;
    TInstant FinishTime;

    TTabletId BSC;
    bool IsManualStartup = false;

public:
    TRootDataErasureManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config);

    void UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) override;
    void Start() override;
    void Stop() override;
    void ClearOperationQueue() override;
    void ClearWaitingDataErasureRequests(NIceDb::TNiceDb& db) override;
    void ClearWaitingDataErasureRequests() override;
    void WakeupToRunDataErasure(TEvSchemeShard::TEvWakeupToRunDataErasure::TPtr& ev, const NActors::TActorContext& ctx) override;
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
    void HandleNewPartitioning(const std::vector<TShardIdx>& dataErasureShards, NIceDb::TNiceDb& db) override;
    void SyncBscGeneration(NIceDb::TNiceDb& db, ui64 currentBscGeneration) override;

private:
    static TQueue::TConfig ConvertConfig(const NKikimrConfig::TDataErasureConfig& config);

    void ScheduleDataErasureWakeup();
    NOperationQueue::EStartStatus StartDataErasure(const TPathId& pathId);
    void OnTimeout(const TPathId& pathId);
    void Enqueue(const TPathId& pathId);
    void UpdateMetrics();
};

//////////////////// TTenantDataErasureManager ////////////////////

class TTenantDataErasureManager : public TDataErasureManager {
private:
using TQueue = NOperationQueue::TOperationQueueWithTimer<
        TShardIdx,
        TFifoQueue<TShardIdx>,
        TEvPrivate::EvRunTenantDataErasure,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::TENANT_DATA_ERASURE>;

    class TStarter : public TQueue::IStarter {
    public:
        TStarter(TTenantDataErasureManager* const manager);

        NOperationQueue::EStartStatus StartOperation(const TShardIdx& shardIdx) override;
        void OnTimeout(const TShardIdx& shardIdx) override;

    private:
        TTenantDataErasureManager* const Manager;
    };

private:
    TStarter Starter;
    TQueue* Queue = nullptr;
    THashMap<TShardIdx, EDataErasureStatus> WaitingDataErasureShards;
    THashMap<TShardIdx, TActorId> ActivePipes;

public:
    TTenantDataErasureManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config);

    void UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) override;
    void Start() override;
    void Stop() override;
    void ClearOperationQueue() override;
    void ClearWaitingDataErasureRequests(NIceDb::TNiceDb& db) override;
    void ClearWaitingDataErasureRequests() override;
    void WakeupToRunDataErasure(TEvSchemeShard::TEvWakeupToRunDataErasure::TPtr& ev, const NActors::TActorContext& ctx) override;
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
    void HandleNewPartitioning(const std::vector<TShardIdx>& dataErasureShards, NIceDb::TNiceDb& db) override;
    void SyncBscGeneration(NIceDb::TNiceDb& db, ui64 currentBscGeneration) override;

private:
    static TQueue::TConfig ConvertConfig(const NKikimrConfig::TDataErasureConfig& config);

    NOperationQueue::EStartStatus StartDataErasure(const TShardIdx& shardIdx);
    void OnTimeout(const TShardIdx& shardIdx);
    void Enqueue(const TShardIdx& shardIdx);
    void UpdateMetrics();
    void SendResponseToRootSchemeShard();
};

} // NKikimr::NSchemeShard
