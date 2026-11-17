#pragma once

#include "logging.h"
#include "nodes_manager.h"
#include "private_events.h"
#include "public_events.h"
#include "replication.h"
#include "schema.h"
#include "session_info.h"
#include "sys_params.h"
#include "worker_lag_expiry_queue.h"
#include <ydb/core/tx/replication/checksum/checksum.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>

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
#include <util/stream/fwd.h>

#include <optional>

namespace NKikimr::NReplication::NController {

THolder<TEvTxUserProxy::TEvProposeTransaction> MakeCommitProposal(
    ui64 writeTxId, const TVector<TString>& tables, bool reportPlanned = false);

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

    // TODO(ilnaz): configurable
    static constexpr ui64 MaxOpenTxIds = 5;
    static constexpr ui64 MinAllocatedTxIds = 3;

    enum class ESchemaBarrierPhase: ui8 {
        Collecting = 1,
        Altering = 2,
        Applied = 3,
        Error = 4,
        FlushingTarget = 5,
        Verifying = 6,
    };

    struct TSchemaBarrier {
        ESchemaBarrierPhase Phase = ESchemaBarrierPhase::Collecting;
        NKikimrReplication::TSchemaChange Schema;
        THashSet<TWorkerId> ExpectedWorkers;
        THashSet<TWorkerId> ReportedWorkers;
        THashSet<TWorkerId> IndexMetadataWorkers;
        THashSet<TWorkerId> AppliedWorkers;
        THashSet<TWorkerId> CompletedWorkers;
        THashMap<TWorkerId, ui64> WorkerOffsets;
        ui64 DstAlterTxId = 0;

        // These IDs remain in AssignedTxIds after their target-only commits.
        // The next ordinary global commit retires them across all targets.
        TVector<ui64> TargetFlushTxIds;
        size_t NextTargetFlushTxId = 0;

        bool IsDestinationSchemaReady() const {
            return Phase == ESchemaBarrierPhase::Verifying || Phase == ESchemaBarrierPhase::Applied;
        }

        bool AreAllWorkersCompleted() const {
            return CompletedWorkers.size() == ExpectedWorkers.size();
        }

        // Applied and Error are terminal phases; earlier phases may still
        // need a global heartbeat quorum, even before DDL starts.
        bool IsInProgress() const {
            return Phase != ESchemaBarrierPhase::Applied && Phase != ESchemaBarrierPhase::Error;
        }

        // Lifecycle changes also wait for the worker handshake after Applied.
        bool IsActive() const {
            return IsInProgress()
                || (Phase == ESchemaBarrierPhase::Applied && !AreAllWorkersCompleted());
        }
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
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override;
    bool RenderMonitoring(TStringStream& out, ui64 replicationId, ui32 page) const;
    TStringBuf GetMonitoringBootStatus(const TWorkerId& id) const;

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
    void Handle(TEvController::TEvCheckTable::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvController::TEvCancelCheckTable::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvController::TEvCheckTableResult::TPtr& ev, const TActorContext& ctx);
    void Handle(NSchemeShard::TEvSchemeShard::TEvChecksumAdmissionResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvChecksumSchedule::TPtr& ev, const TActorContext& ctx);
    void Handle(NChecksum::TEvResult::TPtr& ev, const TActorContext& ctx);
    void Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev, const TActorContext& ctx);
    void HandleChecksumWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);
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
    void Handle(TEvPrivate::TEvRestoreSession::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvBootPersistedWorkers::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRemoveWorker::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvCompleteWorkerSet::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvResumeDeferredAlter::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvDescribeTargetsResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRequestCreateStream::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRequestDropStream::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvCancelCreateStream::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvCancelDropStream::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDiscovery::TEvDiscoveryData::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDiscovery::TEvError::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvWorkerStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvRunWorker::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvWorkerDataEnd::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvGetTxId::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvHeartbeat::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvIndexBuildProgress::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvSchemaChangeReport::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvService::TEvSourceOffsetReport::TPtr& ev, const TActorContext& ctx);
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
    bool HasWorkerSession(const TWorkerId& id, ui32 nodeId) const;
    bool TracksSourceOffsets(const TWorkerId& id) const;
    void SetReplicationError(TReplication& replication, const TReplication::ITarget& target) const;
    void DeleteProcessedSourceOffsets(ui64 replicationId, ui64 targetId, NIceDb::TNiceDb&);
    void StopWorkersForMismatch(ui64 replicationId, const TActorContext& ctx);
    TWorkerInfo* GetOrCreateWorker(const TWorkerId& id);
    void BootWorker(ui32 nodeId, const TWorkerId& id, const NKikimrReplication::TRunWorkerCommand& cmd);
    void ReplaySchemaChangeRecovery(ui32 nodeId, const TWorkerId& id);
    void SendSchemaChangeResult(const TWorkerId& id, const NKikimrReplication::TSchemaChange& schema,
        ui64 offset, bool applied, bool completed, const TActorContext& ctx);
    void StopWorker(ui32 nodeId, const TWorkerId& id);
    void RemoveWorker(const TWorkerId& id, const TActorContext& ctx);
    bool MaybeRemoveWorker(const TWorkerId& id, const TActorContext& ctx);
    TReplication::ITarget* FindTarget(const TWorkerId& id) const;
    bool IsHeartbeatParticipant(const TWorkerId& id) const;
    size_t CountHeartbeatParticipants() const;
    void AllocateTxIds(const TActorContext& ctx);
    TVector<TString> GetCommitTablePaths(const TRowVersion& version) const;
    bool CanCommitIndexBuilds(const TRowVersion& version) const;
    void UpdateLag(const TWorkerId& id, TDuration lag);
    void UpdateLagCounters();
    void InvalidateLag(const TWorkerId& id);
    void ScheduleLagExpiry(const TActorContext& ctx);
    void Handle(TEvPrivate::TEvExpireWorkerLag::TPtr& ev, const TActorContext& ctx);
    void UpdateStats(const TWorkerId& id, const NKikimrReplication::TWorkerStats& stats);
    void UpdateStats(const TWorkerId& id, NKikimrReplication::TEvWorkerStatus::EStatus status);
    void ProcessCreateStreamQueue(const TActorContext& ctx);
    void ProcessDropStreamQueue(const TActorContext& ctx);
    void RemoveTarget(NIceDb::TNiceDb& db, TReplication& replication, ui64 targetId);

    // local transactions
    class TTxInitSchema;
    class TTxInit;
    class TTxMonitoring;
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
    class TTxSourceOffsetReport;
    class TTxAssignTxId;
    class TTxHeartbeat;
    class TTxIndexBuild;
    class TTxIndexReady;
    class TTxCommitChanges;
    class TTxRunWorker;
    class TTxRemoveWorker;
    class TTxCompleteWorkerSet;
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
    void RunTxWorkerError(const TWorkerId& id, const TString& error, const TActorContext& ctx, bool sourceOffsetMismatch = false, TMaybe<ui64> sourceCommittedOffset = Nothing());
    void RunTxAssignTxId(const TActorContext& ctx);
    using TChecksumKey = std::pair<ui64, ui64>;
    using TChecksumStatus = Ydb::Replication::DescribeReplicationResult::Item::ChecksumStatus;
    class TTxChecksum;
    void RunChecksumTx(std::function<void(NIceDb::TNiceDb&)> execute, std::function<void(const TActorContext&)> complete);
    void SaveChecksum(NIceDb::TNiceDb& db, TChecksumKey key);
    void ProgressChecksums(const TActorContext& ctx);
    void StopChecksum(TChecksumKey key, const TActorContext& ctx);
    void FinishChecksum(TChecksumKey key, Ydb::StatusIds::StatusCode status, TString issue,
        bool deferred, const TActorContext& ctx);
    void StartChecksumSource(TChecksumKey key, bool probe, const TActorContext& ctx);
    void StartChecksumAdmission(TChecksumKey key, const TActorContext& ctx);
    void ProgressChecksumSchedule(const TActorContext& ctx);
    void RebuildChecksumTargets();
    void RemoveReplicationChecksums(ui64 replicationId, const TActorContext& ctx);
    TString ValidateChecksum(const TReplication& replication, bool requireSessions) const;
    bool ChecksumAssignmentPaused() const;
    bool IsChecksumBoundaryTxId(ui64 writeTxId) const;
    TRowVersion AdjustTxIdVersion(TRowVersion version) const;
    void RebuildPendingTxIds();
    TRowVersion GetChecksumBoundary() const;
    TRowVersion GetTxIdBegin(TRowVersion end) const;
    void OnChecksumCommitPlanned(ui64 writeTxId, TRowVersion version, const TActorContext& ctx);
    void OnChecksumCommitComplete(ui64 writeTxId, TRowVersion version, NIceDb::TNiceDb& db);
    void RunTxHeartbeat(const TActorContext& ctx);
    void RunTxIndexBuild(const TActorContext& ctx);
    void RunTxIndexReadyTxId(TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr& ev, const TActorContext& ctx);
    void RunTxIndexReadyResult(TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr& ev, const TActorContext& ctx);
    void RunTxRunWorker(TEvService::TEvRunWorker::TPtr& ev, const TActorContext& ctx);
    void RunTxRemoveWorker(const TWorkerId& id, const TActorContext& ctx);
    void RunTxCompleteWorkerSet(TEvPrivate::TEvCompleteWorkerSet::TPtr& ev, const TActorContext& ctx);
    void RunTxSchemaChangeReport(TEvService::TEvSchemaChangeReport::TPtr& ev, const TActorContext& ctx);
    void RunTxSchemaChangeDstAlterTxId(TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr& ev, const TActorContext& ctx);
    void RunTxSchemaChangeDstAlterResult(TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr& ev, const TActorContext& ctx);
    void RunTxResumeDeferredAlter(TEvPrivate::TEvResumeDeferredAlter::TPtr& ev, const TActorContext& ctx);

    void StartSchemaChangeDstAlter(const std::pair<ui64, ui64>& key, const TActorContext& ctx);
    void StopSchemaChangeDstAlter(const std::pair<ui64, ui64>& key, const TActorContext& ctx);
    void StartSchemaChangeTargetFlush(const std::pair<ui64, ui64>& key, const TActorContext& ctx);
    void StopSchemaChangeTargetFlush(const std::pair<ui64, ui64>& key);
    bool HasActiveSchemaBarrier(ui64 replicationId) const;
    bool HasPendingAlter(ui64 replicationId) const;
    bool HasFreshHeartbeatQuorum(const TSchemaBarrier& barrier) const;
    bool HasPendingTargetFlushTxId(const TSchemaBarrier& barrier) const;
    bool BlocksGlobalCommit(const TSchemaBarrier& barrier) const;
    void AdvanceVerifyingSchemaBarriers(NIceDb::TNiceDb& db);

    void StartIndexReady(const std::pair<ui64, ui64>& key, const TActorContext& ctx);
    static bool IsCancelledIndexBuild(const NKikimrReplication::TIndexBuildState& build);
    static bool IsJoinedIndexBuild(const NKikimrReplication::TIndexBuildState& build, const TRowVersion& version);
    static void SaveIndexBuild(NIceDb::TNiceDb& db, const std::pair<ui64, ui64>& key,
        const NKikimrReplication::TIndexBuildState& build);

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

        replication->SetLagChangedCallback([this] { UpdateLagCounters(); });
        UpdateLagCounters();
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
    struct TProcessedSourceOffset {
        ui64 NextOffset = 0;
        TMaybe<ui64> PersistedNextOffset;
    };
    THashMap<TWorkerId, TProcessedSourceOffset> ProcessedSourceOffsets;
    THashSet<ui64> ReplicationsWithSourceOffsetMismatch;
    TWorkerLagExpiryQueue LagExpiryQueue;
    bool LagExpiryScheduled = false;
    THashSet<TWorkerId> StoppingWorkers;
    static constexpr TDuration WorkerLagTimeout = TDuration::Seconds(60);
    THashSet<std::pair<ui64, ui64>> CompleteWorkerSets;
    TMap<std::pair<ui64, ui64>, TSchemaBarrier> SchemaBarriers;
    THashMap<std::pair<ui64, ui64>, TActorId> SchemaChangeDstAlterers;
    THashSet<ui64> DeferredAlters;
    THashSet<TWorkerId> BootQueue;
    THashSet<std::pair<TWorkerId, ui32>> StopQueue;
    THashSet<TWorkerId> RemoveQueue;

    bool ProcessQueuesScheduled = false;
    static constexpr ui32 ProcessBatchLimit = 100;
    static constexpr TDuration RestoreSessionInterval = TDuration::Seconds(1);
    static constexpr TDuration BootPersistedWorkersDelay = TDuration::Seconds(10);

    // create stream limiter
    TDeque<TActorId> RequestedCreateStream;
    THashSet<TActorId> InflightCreateStream;
    // drop stream limiter
    TDeque<TActorId> RequestedDropStream;
    THashSet<TActorId> InflightDropStream;

    TActorId TxAllocatorClient;
    TDeque<ui64> AllocatedTxIds; // got from tx allocator
    bool AllocateTxIdInFlight = false;
    TMap<std::pair<ui64, ui64>, NKikimrReplication::TIndexBuildState> IndexBuilds;
    THashMap<TWorkerId, NKikimrReplication::TIndexBuildProgress> IndexBuildProgress;
    TDeque<TEvService::TEvIndexBuildProgress::TPtr> PendingIndexBuildProgress;
    TDeque<TEvService::TEvGetTxId::TPtr> PendingIndexTxIds;
    THashMap<ui64, std::pair<ui64, ui64>> IndexCommits;
    THashSet<ui64> CompletedIndexCommits;
    THashMap<std::pair<ui64, ui64>, TActorId> IndexReadyActors;
    bool IndexBuildTxInFlight = false;
    TMap<ui64, TRowVersion> CommittedVersions; // Completed global frontier per replication.
    TMap<TRowVersion, ui64> AssignedTxIds; // tx ids assigned to version
    TMap<TRowVersion, TRowVersion> AssignedTxIdBegins;
    TMap<TRowVersion, THashSet<ui32>> RawPendingTxId;
    TMap<TRowVersion, THashSet<ui32>> PendingTxId;
    bool AssignTxIdInFlight = false;

    THashSet<TWorkerId> WorkersWithHeartbeat;
    TMap<TRowVersion, THashSet<TWorkerId>> WorkersByHeartbeat;
    THashMap<TWorkerId, TRowVersion> PendingHeartbeats;
    bool ProcessHeartbeatsInFlight = false;
    ui64 CommittingTxId = 0;
    THashMap<ui64, std::pair<ui64, ui64>> SchemaTargetFlushes;
    std::optional<std::pair<ui64, ui64>> ActiveSchemaTargetFlush;

    TMap<TChecksumKey, NKikimrReplication::TTableChecksumExecution> ChecksumExecutions;
    TMap<TChecksumKey, TChecksumStatus> ChecksumResults;
    TMap<ui64, TRowVersion> ChecksumBoundaries;
    TMap<TPathId, TChecksumKey> ChecksumTargets;
    TMap<TChecksumKey, NKikimrReplication::TTableChecksumSchedule> ChecksumSchedules;
    bool ChecksumScheduleWakeupScheduled = false;
    bool ChecksumScheduleTxInFlight = false;

    struct TChecksumRuntime {
        TActorId Source;
        TActorId Destination;
        TActorId Admission;
        TVector<std::pair<TActorId, ui64>> Waiters;
        TVector<THolder<IEventHandle>> PendingEvents;
        bool SourceDone = false;
        bool DestinationDone = false;
        bool TransitionInFlight = false;
        bool Retaining = false;
        bool Retained = false;
        bool Recovery = false;
        TRowVersion DestinationVersion = TRowVersion::Min();
        NKqp::TSnapshotHandle DestinationHold;
        TString SourceSchema;
        TString DestinationSchema;
    };
    TMap<TChecksumKey, TChecksumRuntime> ChecksumRuntime;
    ui64 NextChecksumCookie = 0;
    struct TChecksumRetainCookie {
        TChecksumKey Key;
        TString RequestId;
        TRowVersion Version;
    };
    THashMap<ui64, TChecksumRetainCookie> ChecksumRetainCookies;
    bool ChecksumWakeupScheduled = false;

}; // TController

}
