#pragma once

#include "logging.h"
#include "nodes_manager.h"
#include "private_events.h"
#include "public_events.h"
#include "replication.h"
#include "schema.h"
#include "session_info.h"
#include "sys_params.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/counters_replication.pb.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>

#include <optional>

namespace NKikimr::NReplication::NController {

class TController
    : public TActor<TController>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    class TTxBase: public NTabletFlatExecutor::TTransactionBase<TController> {
    public:
        TTxBase(const TString& name, TController* self)
            : TTransactionBase(self)
            , TxLogPrefix(CreateTabletLogPrefix(self, name))
        {
        }

    protected:
        const NActors::NStructuredLog::TStructuredMessage TxLogPrefix;
    };

private:
    using Schema = TControllerSchema;

    static THolder<TEvTxUserProxy::TEvProposeTransaction> MakeCommitProposal(
        ui64 writeTxId, const TVector<TString>& tables);

    enum class ESchemaBarrierPhase: ui8 {
        Collecting = 1,
        FlushingTarget = 2,
        Altering = 3,
        Verifying = 4,
        Applied = 5,
        Error = 6,
    };

    struct TSchemaBarrier {
        ESchemaBarrierPhase Phase = ESchemaBarrierPhase::Collecting;
        NKikimrReplication::TSchemaChange Schema;
        THashSet<TWorkerId> ExpectedWorkers;
        THashSet<TWorkerId> ReportedWorkers;
        THashSet<TWorkerId> AppliedWorkers;
        THashSet<TWorkerId> CompletedWorkers;
        THashMap<TWorkerId, ui64> WorkerOffsets;
        ui64 DstAlterTxId = 0;

        // Global consistency temporarily commits the already assigned write
        // ids to just this target before its DDL is executed. The entries in
        // AssignedTxIds deliberately remain intact: the next common
        // heartbeat commits them normally to every target.
        TVector<ui64> TargetFlushTxIds;
        size_t NextTargetFlushTxId = 0;
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_ACTOR;
    }

    explicit TController(const TActorId& tablet, TTabletStorageInfo* info);

private:
    // tablet overrides
    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;

    // state functions
    STFUNC(StateInit);
    STFUNC(StateDatabaseResolve);
    STFUNC(StateWork);

    void Cleanup(const TActorContext& ctx);
    void SwitchToDatabaseResolve(const TActorContext& ctx);
    void SwitchToWork(const TActorContext& ctx);
    void Reset();

    // handlers
    void Handle(TEvController::TEvCreateReplication::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvController::TEvAlterReplication::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvController::TEvDropReplication::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvController::TEvDescribeReplication::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvDropReplication::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvDiscoveryTargetsResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvAssignStreamName::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvCreateStreamResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvDropStreamResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvCreateDstResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvAlterDstResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvDropDstResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvResolveSecretResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvResolveResourceIdResult::TPtr& ev, const TActorContext& ctx);
    void HandleDatabaseResolve(TEvPrivate::TEvResolveTenantResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvResolveTenantResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvUpdateTenantNodes::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvProcessQueues::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRemoveWorker::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvWorkersRegistered::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvResumeDeferredAlter::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvDescribeTargetsResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRequestCreateStream::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRequestDropStream::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDiscovery::TEvDiscoveryData::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDiscovery::TEvError::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvWorkerStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvRunWorker::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvWorkerDataEnd::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvGetTxId::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvHeartbeat::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvSchemaChangeReport::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext& ctx);

    void CreateSession(ui32 nodeId, const TActorContext& ctx);
    void DeleteSession(ui32 nodeId, const TActorContext& ctx);
    void CloseSession(ui32 nodeId, const TActorContext& ctx);
    void ScheduleProcessQueues();
    void ProcessBootQueue(const TActorContext& ctx);
    void ProcessStopQueue(const TActorContext& ctx);
    bool IsValidWorker(const TWorkerId& id) const;
    TWorkerInfo* GetOrCreateWorker(const TWorkerId& id, NKikimrReplication::TRunWorkerCommand* cmd = nullptr);
    void BootWorker(ui32 nodeId, const TWorkerId& id, const NKikimrReplication::TRunWorkerCommand& cmd);
    void ReplaySchemaChangeRecovery(ui32 nodeId, const TWorkerId& id);
    void StopWorker(ui32 nodeId, const TWorkerId& id);
    void RemoveWorker(const TWorkerId& id, const TActorContext& ctx);
    bool MaybeRemoveWorker(const TWorkerId& id, const TActorContext& ctx);
    TReplication::ITarget* FindTarget(const TWorkerId& id) const;
    void UpdateLag(const TWorkerId& id, TDuration lag);
    void UpdateStats(const TWorkerId& id, const NKikimrReplication::TWorkerStats& stats);
    void UpdateStats(const TWorkerId& id, NKikimrReplication::TEvWorkerStatus::EStatus status);
    void ProcessCreateStreamQueue(const TActorContext& ctx);
    void ProcessDropStreamQueue(const TActorContext& ctx);

    // local transactions
    class TTxInitSchema;
    class TTxInit;
    class TTxCreateReplication;
    class TTxAlterReplication;
    class TTxDropReplication;
    class TTxDescribeReplication;
    class TTxDiscoveryTargetsResult;
    class TTxAssignStreamName;
    class TTxCreateStreamResult;
    class TTxDropStreamResult;
    class TTxCreateDstResult;
    class TTxAlterDstResult;
    class TTxDropDstResult;
    class TTxResolveDatabaseResult;
    class TTxResolveSecretResult;
    class TTxResolveResourceIdResult;
    class TTxWorkerError;
    class TTxAssignTxId;
    class TTxHeartbeat;
    class TTxCommitChanges;
    class TTxRunWorker;
    class TTxRemoveWorker;
    class TTxWorkersRegistered;
    class TTxSchemaChangeReport;
    class TTxSchemaChangeDstAlterTxId;
    class TTxSchemaChangeDstAlterResult;
    class TTxResumeDeferredAlter;

    // tx runners
    void RunTxInitSchema(const TActorContext& ctx);
    void RunTxInit(const TActorContext& ctx);
    void RunTxCreateReplication(TEvController::TEvCreateReplication::TPtr& ev, const TActorContext& ctx);
    void RunTxAlterReplication(TEvController::TEvAlterReplication::TPtr& ev, const TActorContext& ctx);
    void RunTxDropReplication(TEvController::TEvDropReplication::TPtr& ev, const TActorContext& ctx);
    void RunTxDropReplication(TEvPrivate::TEvDropReplication::TPtr& ev, const TActorContext& ctx);
    void RunTxDescribeReplication(TEvController::TEvDescribeReplication::TPtr& ev, const TActorContext& ctx);
    void RunTxDescribeReplication(TEvPrivate::TEvDescribeTargetsResult::TPtr& ev, const TActorContext& ctx);
    void RunTxDiscoveryTargetsResult(TEvPrivate::TEvDiscoveryTargetsResult::TPtr& ev, const TActorContext& ctx);
    void RunTxAssignStreamName(TEvPrivate::TEvAssignStreamName::TPtr& ev, const TActorContext& ctx);
    void RunTxCreateStreamResult(TEvPrivate::TEvCreateStreamResult::TPtr& ev, const TActorContext& ctx);
    void RunTxDropStreamResult(TEvPrivate::TEvDropStreamResult::TPtr& ev, const TActorContext& ctx);
    void RunTxCreateDstResult(TEvPrivate::TEvCreateDstResult::TPtr& ev, const TActorContext& ctx);
    void RunTxAlterDstResult(TEvPrivate::TEvAlterDstResult::TPtr& ev, const TActorContext& ctx);
    void RunTxDropDstResult(TEvPrivate::TEvDropDstResult::TPtr& ev, const TActorContext& ctx);
    void RunTxResolveDatabaseResult(TEvPrivate::TEvResolveTenantResult::TPtr& ev, const TActorContext& ctx);
    void RunTxResolveSecretResult(TEvPrivate::TEvResolveSecretResult::TPtr& ev, const TActorContext& ctx);
    void RunTxResolveResourceIdResult(TEvPrivate::TEvResolveResourceIdResult::TPtr& ev, const TActorContext& ctx);
    void RunTxWorkerError(const TWorkerId& id, const TString& error, const TActorContext& ctx);
    void RunTxAssignTxId(const TActorContext& ctx);
    void RunTxHeartbeat(const TActorContext& ctx);
    void RunTxRunWorker(TEvService::TEvRunWorker::TPtr& ev, const TActorContext& ctx);
    void RunTxRemoveWorker(const TWorkerId& id, const TActorContext& ctx);
    void RunTxWorkersRegistered(TEvPrivate::TEvWorkersRegistered::TPtr& ev, const TActorContext& ctx);
    void RunTxSchemaChangeReport(TEvService::TEvSchemaChangeReport::TPtr& ev, const TActorContext& ctx);
    void RunTxSchemaChangeDstAlterTxId(TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr& ev, const TActorContext& ctx);
    void RunTxSchemaChangeDstAlterResult(TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr& ev, const TActorContext& ctx);
    void RunTxResumeDeferredAlter(TEvPrivate::TEvResumeDeferredAlter::TPtr& ev, const TActorContext& ctx);

    void StartSchemaChangeDstAlter(const std::pair<ui64, ui64>& key, const TActorContext& ctx);
    void StopSchemaChangeDstAlter(const std::pair<ui64, ui64>& key, const TActorContext& ctx);
    void StartSchemaChangeTargetFlush(const std::pair<ui64, ui64>& key, const TActorContext& ctx);
    bool HasActiveSchemaBarrier(ui64 replicationId) const;

    // other
    template <typename T>
    TReplication::TPtr Add(ui64 id, const TPathId& pathId, T&& config, const TString& database) {
        auto replication = MakeIntrusive<TReplication>(id, pathId, std::forward<T>(config), database);
        {
            const auto res = Replications.emplace(id, replication);
            Y_VERIFY_S(res.second, "Duplication replication: " << id);
        }
        {
            const auto res = ReplicationsByPathId.emplace(pathId, replication);
            Y_VERIFY_S(res.second, "Duplication replication: " << pathId);
        }

        return replication;
    }

    TReplication::TPtr Find(ui64 id) const;
    TReplication::TPtr Find(const TPathId& pathId) const;
    TReplication::TPtr GetSingle() const;
    void Remove(ui64 id);

private:
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;
    THolder<TTabletCountersBase> TabletCountersPtr;
    TTabletCountersBase* TabletCounters;

    TSysParams SysParams;
    THashMap<ui64, TReplication::TPtr> Replications;
    THashMap<TPathId, TReplication::TPtr> ReplicationsByPathId;
    THashMap<ui64, ui8> UnresolvedDatabaseReplications;
    static constexpr ui8 ResolveDatabaseAttemptsLimit = 5;

    TActorId DiscoveryCache;
    TNodesManager NodesManager;
    THashMap<ui32, TSessionInfo> Sessions;
    THashMap<TWorkerId, TWorkerInfo> Workers;
    THashSet<TWorkerId> BootQueue;
    THashSet<std::pair<TWorkerId, ui32>> StopQueue;
    THashSet<TWorkerId> RemoveQueue;

    bool ProcessQueuesScheduled = false;
    static constexpr ui32 ProcessBatchLimit = 100;

    // create stream limiter
    TDeque<TActorId> RequestedCreateStream;
    THashSet<TActorId> InflightCreateStream;
    // drop stream limiter
    TDeque<TActorId> RequestedDropStream;
    THashSet<TActorId> InflightDropStream;

    TActorId TxAllocatorClient;
    TDeque<ui64> AllocatedTxIds; // got from tx allocator
    bool AllocateTxIdInFlight = false;
    TMap<TRowVersion, ui64> AssignedTxIds; // tx ids assigned to version
    TMap<TRowVersion, THashSet<ui32>> PendingTxId;
    bool AssignTxIdInFlight = false;

    THashSet<TWorkerId> WorkersWithHeartbeat;
    TMap<TRowVersion, THashSet<TWorkerId>> WorkersByHeartbeat;
    THashMap<TWorkerId, TRowVersion> PendingHeartbeats;
    bool ProcessHeartbeatsInFlight = false;
    ui64 CommittingTxId = 0;

    // Keyed by (replication id, target id). DDL execution advances the phase
    // in subsequent transactions; keeping the collector explicit prevents a
    // fast worker from completing a barrier against a partial report set.
    TMap<std::pair<ui64, ui64>, TSchemaBarrier> SchemaBarriers;
    THashMap<std::pair<ui64, ui64>, TActorId> SchemaChangeDstAlterers;
    THashSet<std::pair<ui64, ui64>> WorkerSnapshots;
    THashSet<ui64> DeferredAlters;
    THashMap<ui64, std::pair<ui64, ui64>> SchemaTargetFlushes;
    std::optional<std::pair<ui64, ui64>> ActiveSchemaTargetFlush;

}; // TController

}
