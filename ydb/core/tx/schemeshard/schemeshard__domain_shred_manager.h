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

class TDomainShredManager {
private:
using TQueue = NOperationQueue::TOperationQueueWithTimer<
    TPathId,
    TFifoQueue<TPathId>,
    TEvPrivate::EvRunShred,
    NKikimrServices::FLAT_TX_SCHEMESHARD,
    NKikimrServices::TActivity::SCHEMESHARD_SHRED>;

    class TStarter : public TQueue::IStarter {
    public:
        TStarter(TDomainShredManager* const manager);

        NOperationQueue::EStartStatus StartOperation(const TPathId&) override;
        void OnTimeout(const TPathId&) override;

    private:
        TDomainShredManager* const Manager;
    };

private:
    TSchemeShard* const SchemeShard;
    EShredStatus Status = EShredStatus::UNSPECIFIED;
    ui64 Generation = 0;
    ui64 BscGeneration = 0;
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
    ::NMonitoring::TDynamicCounters::TCounterPtr WaitingTenantsCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr RunungTenantsCounter;
    ::NMonitoring::TDynamicCounters::TCounterPtr CompletedTenantsCounter;

public:
    TDomainShredManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config);

    void UpdateConfig(const NKikimrConfig::TDataErasureConfig& config);
    void Start();
    void Stop();
    void Clear() {
        Queue->Clear();
        WaitingShredTenants.clear();
        ActivePipes.clear();
    }
    void WakeupToRunShred(TEvSchemeShard::TEvWakeupToRunShred::TPtr& ev, const NActors::TActorContext& ctx);
    void WakeupSendRequestToBSC();
    void StartShred(NIceDb::TNiceDb& db);
    void Continue();
    void HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx);
    void FinishShred(NIceDb::TNiceDb& db, const TPathId& pathId);
    void ScheduleRequestToBSC();
    void SendRequestToBSC();
    void Complete();
    bool Restore(NIceDb::TNiceDb& db);
    bool Remove(const TPathId& pathId);
    void SyncBscGeneration(NIceDb::TNiceDb& db, ui64 currentBscGeneration);

    EShredStatus GetStatus() const {return Status;}
    void SetStatus(const EShredStatus& status) {Status = status;}
    ui64 GetGeneration() const {return Generation;}
    void SetGeneration(ui64 generation) {Generation = generation;}
    ui64 GetBscGeneration() const {return BscGeneration;}
    void SetBscGeneration(ui64 generation) {BscGeneration = generation;}

private:
    static TQueue::TConfig ConvertConfig(const NKikimrConfig::TDataErasureConfig& config);

    void ScheduleShredWakeup();
    NOperationQueue::EStartStatus StartShredOperation(const TPathId& pathId);
};

} // NKikimr::NSchemeShard
