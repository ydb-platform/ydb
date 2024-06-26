#pragma once

#include "datashard.h"
#include "datashard_trans_queue.h"
#include "datashard_active_transaction.h"
#include "datashard_write_operation.h"
#include "datashard_dep_tracker.h"
#include "datashard_user_table.h"
#include "execution_unit.h"
#include "read_iterator.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/flat_exec_seat.h>

namespace NKikimr {
namespace NDataShard {

using NTabletFlatExecutor::TTransactionContext;

class TDataShard;
class TOperation;

///
class TPipeline : TNonCopyable {
public:
    struct TConfig {
        enum EFlags : ui64 {
            EFlagsOutOfOrder = 0x01,
            EFlagsForceOnline = 0x02,
            EFlagsForceOnlineRW = 0x04,
            EFlagsDirtyOnline = 0x08,
            EFlagsDirtyImmediate = 0x10,
        };

        ui64 Flags;
        ui64 LimitActiveTx;
        ui64 LimitDataTxCache;
        ui64 LimitDoneDataTx;

        TConfig()
            : Flags(0)
            , LimitActiveTx(DefaultLimitActiveTx())
            , LimitDataTxCache(DefaultLimitDataTxCache())
            , LimitDoneDataTx(DefaultLimitDoneDataTx())
        {}

        static constexpr ui64 DefaultLimitActiveTx() { return 1; }
        static constexpr ui64 DefaultLimitDataTxCache() { return 20; }
        static constexpr ui64 DefaultLimitDoneDataTx() { return 20; }

        bool OutOfOrder() const { return (Flags & EFlagsOutOfOrder) && (LimitActiveTx > 1); }
        bool NoImmediate() const { return Flags & EFlagsForceOnline; }
        bool ForceOnlineRW() const { return Flags & EFlagsForceOnlineRW; }
        bool DirtyOnline() const { return Flags & EFlagsDirtyOnline; }
        bool DirtyImmediate() const { return Flags & EFlagsDirtyImmediate; }
        bool SoftUpdates() const { return Flags & (EFlagsForceOnlineRW|EFlagsDirtyOnline|EFlagsDirtyImmediate); }

        void Update(const NKikimrSchemeOp::TPipelineConfig& cfg) {
            if (cfg.GetEnableOutOfOrder()) {
                Flags |= EFlagsOutOfOrder;
            } else {
                Flags &= ~EFlagsOutOfOrder;
            }
            if (cfg.GetDisableImmediate()) {
                Flags |= EFlagsForceOnline;
            } else {
                Flags &= ~EFlagsForceOnline;
            }
            if (cfg.GetEnableSoftUpdates()) {
                Flags |= EFlagsForceOnlineRW & EFlagsDirtyOnline & EFlagsDirtyImmediate;
            } else {
                Flags &= ~(EFlagsForceOnlineRW | EFlagsDirtyOnline | EFlagsDirtyImmediate);
            }

            // has [default = 8] in TPipelineConfig proto
            LimitActiveTx = cfg.GetNumActiveTx();

            // does not have default in TPipelineConfig proto
            if (cfg.HasDataTxCacheSize()) {
                LimitDataTxCache = cfg.GetDataTxCacheSize();
            }
        }
    };

    TPipeline(TDataShard * self);
    ~TPipeline();

    bool Load(NIceDb::TNiceDb& db);
    void UpdateConfig(NIceDb::TNiceDb& db, const NKikimrSchemeOp::TPipelineConfig& cfg);

    bool OutOfOrderLimits() const;
    bool CanRunAnotherOp();

    bool CanRunOp(const TOperation &op) const;

    ui64 ImmediateInFly() const { return ImmediateOps.size(); }
    const THashMap<ui64, TOperation::TPtr> &GetImmediateOps() const { return ImmediateOps; }

    TDependencyTracker &GetDepTracker() { return DepTracker; }

    const TConfig &GetConfig() const { return Config; }

    // tx propose

    bool SaveForPropose(TValidatedTx::TPtr tx);
    void SetProposed(ui64 txId, const TActorId& actorId);

    void ForgetUnproposedTx(ui64 txId);
    void ForgetTx(ui64 txId);

    // tx activity

    TOperation::TPtr GetNextPlannedOp(ui64 step, ui64 txId) const;
    TOperation::TPtr GetNextActiveOp(bool dryRun);
    bool IsReadyOp(TOperation::TPtr op);

    bool LoadTxDetails(TTransactionContext &txc, const TActorContext &ctx, TActiveTransaction::TPtr tx);
    bool LoadWriteDetails(TTransactionContext& txc, const TActorContext& ctx, TWriteOperation::TPtr tx);

    void DeactivateOp(TOperation::TPtr op, TTransactionContext& txc, const TActorContext &ctx);
    void RemoveTx(TStepOrder stepTxId);
    const TSchemaOperation* FindSchemaTx(ui64 txId) const;
    void CompleteSchemaTx(NIceDb::TNiceDb& db, ui64 txId);
    void MarkOpAsUsingSnapshot(TOperation::TPtr op);

    bool HasPredictedPlan() const { return !PredictedPlan.empty(); }
    ui64 NextPredictedPlanStep() const { return PredictedPlan.begin()->Step; }

    bool PlanTxs(ui64 step, TVector<ui64> &txIds, TTransactionContext &txc, const TActorContext &ctx);
    bool PlanPredictedTxs(ui64 step, TTransactionContext &txc, const TActorContext &ctx);
    void PlanTxImpl(ui64 step, ui64 txId, TTransactionContext &txc, const TActorContext &ctx);
    void AddPredictedPlan(ui64 step, ui64 txId, const TActorContext &ctx);
    void PreserveSchema(NIceDb::TNiceDb& db, ui64 step);
    TDuration CleanupTimeout() const;
    ECleanupStatus Cleanup(NIceDb::TNiceDb& db, const TActorContext& ctx,
        std::vector<std::unique_ptr<IEventHandle>>& replies);

    // times

    bool AssignPlanInterval(TOperation::TPtr op);
    ui64 OutdatedReadSetStep() const;
    ui64 OutdatedCleanupStep() const;
    ui64 AllowedDataStep() const { return Max(LastPlannedTx.Step + 1, TAppData::TimeProvider->Now().MilliSeconds()); }
    ui64 AllowedSchemaStep() const { return LastPlannedTx.Step + 1; }
    ui64 VacantSchemaStep() const { return KeepSchemaStep + 1; }

    TStepOrder GetLastPlannedTx() const { return LastPlannedTx; }
    TStepOrder GetLastCompleteTx() const { return LastCompleteTx; }
    TStepOrder GetUtmostCompleteTx() const { return UtmostCompleteTx; }

    ui64 GetTxCompleteLag(EOperationKind kind, ui64 timecastStep) const;
    ui64 GetDataTxCompleteLag(ui64 timecastStep) const;
    ui64 GetScanTxCompleteLag(ui64 timecastStep) const;

    // schema ops

    bool HasSchemaOperation() const { return CurrentSchemaTxId(); }
    bool HasCreate() const { return SchemaTx && SchemaTx->IsCreate(); }
    bool HasAlter() const { return SchemaTx && SchemaTx->IsAlter(); }
    bool HasDrop() const { return SchemaTx && SchemaTx->IsDrop(); }
    bool HasBackup() const { return SchemaTx && SchemaTx->IsBackup(); }
    bool HasRestore() const { return SchemaTx && SchemaTx->IsRestore(); }
    bool HasCopy() const { return SchemaTx && SchemaTx->IsCopy(); }
    bool HasCreatePersistentSnapshot() const { return SchemaTx && SchemaTx->IsCreatePersistentSnapshot(); }
    bool HasDropPersistentSnapshot() const { return SchemaTx && SchemaTx->IsDropPersistentSnapshot(); }
    bool HasInitiateBuilIndex() const { return SchemaTx && SchemaTx->IsInitiateBuildIndex(); }
    bool HasFinalizeBuilIndex() const { return SchemaTx && SchemaTx->IsFinalizeBuildIndex(); }
    bool HasDropIndexNotice() const { return SchemaTx && SchemaTx->IsDropIndexNotice(); }
    bool HasMove() const { return SchemaTx && SchemaTx->IsMove(); }
    bool HasMoveIndex() const { return SchemaTx && SchemaTx->IsMoveIndex(); }
    bool HasCreateCdcStream() const { return SchemaTx && SchemaTx->IsCreateCdcStream(); }
    bool HasAlterCdcStream() const { return SchemaTx && SchemaTx->IsAlterCdcStream(); }
    bool HasDropCdcStream() const { return SchemaTx && SchemaTx->IsDropCdcStream(); }

    ui64 CurrentSchemaTxId() const {
        if (SchemaTx)
            return SchemaTx->TxId;
        return 0;
    }

    const TSchemaOperation* GetSchemaOp() const {
        return SchemaTx;
    }

    void SetSchemaOp(TSchemaOperation * op) {
        Y_ABORT_UNLESS(!SchemaTx || SchemaTx->TxId == op->TxId);
        SchemaTx = op;
    }

    // TTransQueue wrappers

    void ProposeTx(TOperation::TPtr op, const TStringBuf &txBody, TTransactionContext &txc, const TActorContext &ctx);
    void ProposeComplete(const TOperation::TPtr &op, const TActorContext &ctx);
    void PersistTxFlags(TOperation::TPtr op, TTransactionContext &txc);
    void UpdateSchemeTxBody(ui64 txId, const TStringBuf &txBody, TTransactionContext &txc);
    void ProposeSchemeTx(const TSchemaOperation &op, TTransactionContext &txc);
    bool CancelPropose(NIceDb::TNiceDb& db, const TActorContext& ctx, ui64 txId,
        std::vector<std::unique_ptr<IEventHandle>>& replies);
    ECleanupStatus CleanupOutdated(NIceDb::TNiceDb& db, const TActorContext& ctx, ui64 outdatedStep,
        std::vector<std::unique_ptr<IEventHandle>>& replies);
    bool CleanupVolatile(ui64 txId, const TActorContext& ctx,
        std::vector<std::unique_ptr<IEventHandle>>& replies);
    size_t CleanupWaitingVolatile(const TActorContext& ctx, std::vector<std::unique_ptr<IEventHandle>>& replies);
    ui64 PlannedTxInFly() const;
    const TSet<TStepOrder> &GetPlan() const;
    bool HasProposeDelayers() const;
    bool RemoveProposeDelayer(ui64 txId);

    void ProcessDisconnected(ui32 nodeId);

    ui64 GetInactiveTxSize() const;

    const TMap<TStepOrder, TOperation::TPtr> &GetActivePlannedOps() const { return ActivePlannedOps; }

    ui64 GetLastCompletedTxStep() const { return LastCompleteTx.Step; }
    ui64 GetLastActivePlannedOpStep() const
    {
        return ActivePlannedOps
            ? ActivePlannedOps.rbegin()->second->GetStep()
            : LastCompleteTx.Step;
    }
    ui64 GetLastActivePlannedOpId() const
    {
        return ActivePlannedOps
            ? ActivePlannedOps.rbegin()->second->GetTxId()
            : LastCompleteTx.TxId;
    }
    // Read set iface.
    bool SaveInReadSet(const TEvTxProcessing::TEvReadSet &rs,
                       THolder<IEventHandle> &ack,
                       TTransactionContext &txc,
                       const TActorContext &ctx);
    bool LoadInReadSets(TOperation::TPtr op,
                        TTransactionContext &txc,
                        const TActorContext &ctx);
    void RemoveInReadSets(TOperation::TPtr op,
                          NIceDb::TNiceDb &db);

    TOperation::TPtr FindOp(ui64 txId);

    TOperation::TPtr GetActiveOp(ui64 txId);
    TOperation::TPtr GetVolatileOp(ui64 txId);
    const TMap<TStepOrder, TOperation::TPtr> &GetActiveOps() const { return ActiveOps; }

    void AddActiveOp(TOperation::TPtr op);
    void RemoveActiveOp(TOperation::TPtr op);

    void UnblockNormalDependencies(const TOperation::TPtr &op);
    void UnblockSpecialDependencies(const TOperation::TPtr &op);

    const THashSet<TOperation::TPtr> &GetExecuteBlockers() const { return ExecuteBlockers; }
    void AddExecuteBlocker(TOperation::TPtr op)
    {
        ExecuteBlockers.insert(op);
    }
    void RemoveExecuteBlocker(TOperation::TPtr op)
    {
        ExecuteBlockers.erase(op);
    }

    // Operation builders
    TOperation::TPtr BuildOperation(TEvDataShard::TEvProposeTransaction::TPtr &ev,
                                    TInstant receivedAt, ui64 tieBreakerIndex,
                                    NTabletFlatExecutor::TTransactionContext &txc,
                                    const TActorContext &ctx, NWilson::TSpan &&operationSpan);
    TOperation::TPtr BuildOperation(NEvents::TDataEvents::TEvWrite::TPtr&& ev,
                                    TInstant receivedAt, ui64 tieBreakerIndex,
                                    NTabletFlatExecutor::TTransactionContext &txc,
                                    NWilson::TSpan &&operationSpan);
    void BuildDataTx(TActiveTransaction *tx,
                     TTransactionContext &txc,
                     const TActorContext &ctx);
    ERestoreDataStatus RestoreDataTx(
            TActiveTransaction *tx,
            TTransactionContext &txc,
            const TActorContext &ctx)
    {
        return tx->RestoreTxData(Self, txc, ctx);
    }

    ERestoreDataStatus RestoreWriteTx(
        TWriteOperation* writeOp,
        TTransactionContext& txc
    )
    {
        return writeOp->RestoreTxData(Self, txc.DB);
    }

    void RegisterDistributedWrites(const TOperation::TPtr& op, NTable::TDatabase& db);

    // Execution units
    TExecutionUnit &GetExecutionUnit(EExecutionUnitKind kind)
    {
        return *ExecutionUnits[static_cast<ui32>(kind)].Get();
    }
    const TExecutionUnit &GetExecutionUnit(EExecutionUnitKind kind) const
    {
        return *ExecutionUnits[static_cast<ui32>(kind)].Get();
    }
    EExecutionStatus RunExecutionUnit(TOperation::TPtr op,
                                      TTransactionContext &txc,
                                      const TActorContext &ctx);
    EExecutionStatus RunExecutionPlan(TOperation::TPtr op,
                                      TVector<EExecutionUnitKind> &completeList,
                                      TTransactionContext &txc,
                                      const TActorContext &ctx);
    void RunCompleteList(TOperation::TPtr op,
                         TVector<EExecutionUnitKind> &completeList,
                         const TActorContext &ctx);

    void AddCandidateOp(TOperation::TPtr op)
    {
        if (!op->IsInProgress()
            && !op->IsExecutionPlanFinished()
            && NextActiveOp != op) {
            CandidateOps[op->GetStepOrder()] = op;
        }
    }
    void AddCandidateUnit(EExecutionUnitKind kind)
    {
        CandidateUnits.insert(kind);
    }

    ui64 GetDataTxCacheSize() const { return DataTxCache.size(); }
    const TMap<TStepOrder, TStackVec<THolder<IEventHandle>, 1>> &GetDelayedAcks() const
    {
        return DelayedAcks;
    }

    void HoldExecutionProfile(TOperation::TPtr op);
    void FillStoredExecutionProfiles(NKikimrTxDataShard::TEvGetSlowOpProfilesResponse &rec) const;

    void StartStreamingTx(ui64 txId, ui32 count) {
        ActiveStreamingTxs[txId] += count;
    }

    bool FinishStreamingTx(ui64 txId) {
        if (auto it = ActiveStreamingTxs.find(txId); it != ActiveStreamingTxs.end()) {
            it->second--;
            if (it->second == 0) {
                ActiveStreamingTxs.erase(it);
            }
            return true;
        }
        return false;
    }

    bool HasWaitingSchemeOps() const { return !WaitingSchemeOps.empty(); }

    bool AddWaitingSchemeOp(const TOperation::TPtr& op);
    bool RemoveWaitingSchemeOp(const TOperation::TPtr& op);
    void ActivateWaitingSchemeOps(const TActorContext& ctx) const;
    void MaybeActivateWaitingSchemeOps(const TActorContext& ctx) const;

    ui64 WaitingTxs() const { return WaitingDataTxOps.size(); } // note that without iterators
    bool CheckInflightLimit() const;
    bool AddWaitingTxOp(TEvDataShard::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx);
    bool AddWaitingTxOp(NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx);
    void ActivateWaitingTxOps(TRowVersion edge, const TActorContext& ctx);
    void ActivateWaitingTxOps(const TActorContext& ctx);

    ui64 WaitingReadIterators() const { return WaitingDataReadIterators.size(); }
    void AddWaitingReadIterator(
        const TRowVersion& version,
        TEvDataShard::TEvRead::TPtr ev,
        const TActorContext& ctx);
    bool HasWaitingReadIterator(const TReadIteratorId& readId);
    bool CancelWaitingReadIterator(const TReadIteratorId& readId);
    void RegisterWaitingReadIterator(const TReadIteratorId& readId, TEvDataShard::TEvRead* event);
    bool HandleWaitingReadIterator(const TReadIteratorId& readId, TEvDataShard::TEvRead* event);

    TRowVersion GetReadEdge() const;
    TRowVersion GetUnreadableEdge() const;

    void AddCompletingOp(const TOperation::TPtr& op);
    void RemoveCompletingOp(const TOperation::TPtr& op);
    TOperation::TPtr FindCompletingOp(ui64 txId) const;

    void AddCommittingOp(const TRowVersion& version);
    void AddCommittingOp(const TOperation::TPtr& op);
    void RemoveCommittingOp(const TRowVersion& version);
    void RemoveCommittingOp(const TOperation::TPtr& op);
    bool WaitCompletion(const TOperation::TPtr& op) const;
    bool HasCommittingOpsBelow(TRowVersion upperBound) const;

    /**
     * Promotes the mvcc complete edge to the last distributed transaction less than version
     */
    bool PromoteCompleteEdgeUpTo(const TRowVersion& version, TTransactionContext& txc);

    /**
     * Marks all active planned transactions before this version as logically "complete"
     */
    bool MarkPlannedLogicallyCompleteUpTo(const TRowVersion& version, TTransactionContext& txc);

    /**
     * Marks all active planned transactions before and including this version as logically "incomplete"
     */
    bool MarkPlannedLogicallyIncompleteUpTo(const TRowVersion& version, TTransactionContext& txc);

    /**
     * Adds new runtime dependencies to op based on its buffered lock updates.
     *
     * Returns true when new dependencies were added and op must be rescheduled.
     */
    bool AddLockDependencies(const TOperation::TPtr& op, TLocksUpdate& guardLocks);

    /**
     * Provides a global txId for the waiting operation
     *
     * The operation must have a WaitingForGlobalTxId flag.
     */
    void ProvideGlobalTxId(const TOperation::TPtr& op, ui64 globalTxId);

private:
    struct TStoredExecutionProfile {
        TBasicOpInfo OpInfo;
        TVector<std::pair<EExecutionUnitKind, TExecutionProfile::TUnitProfile>> UnitProfiles;
    };

    class TCommittingDataTxOps {
    private:
        struct TItem {
            ui64 Step;
            ui64 TxId;
            mutable ui32 Counter;

            TItem(const TRowVersion& from)
                : Step(from.Step)
                , TxId(from.TxId)
                , Counter(1u)
            {}

            friend constexpr bool operator<(const TItem& a, const TItem& b) {
                return a.Step < b.Step || (a.Step == b.Step && a.TxId < b.TxId);
            }

            friend constexpr bool operator<=(const TItem& a, const TRowVersion& b) {
                return a.Step < b.Step || (a.Step == b.Step && a.TxId <= b.TxId);
            }
        };

        using TItemsSet = TSet<TItem>;
        using TTxIdMap = THashMap<ui64, TItemsSet::iterator>;
    public:
        inline void Add(ui64 txId, TRowVersion version) {
            auto res = ItemsSet.emplace(version);
            if (!res.second)
                res.first->Counter += 1;
            auto res2 = TxIdMap.emplace(txId, res.first);
            Y_VERIFY_S(res2.second, "Unexpected duplicate immediate tx " << txId
                    << " committing at " << version);
        }

        inline void Add(TRowVersion version) {
            auto res = ItemsSet.emplace(version);
            if (!res.second)
                res.first->Counter += 1;
        }

        inline void Remove(ui64 txId) {
            if (auto it = TxIdMap.find(txId); it != TxIdMap.end()) {
                if (--it->second->Counter == 0)
                    ItemsSet.erase(it->second);
                TxIdMap.erase(it);
            }
        }

        inline void Remove(TRowVersion version) {
            if (auto it = ItemsSet.find(version); it != ItemsSet.end() && --it->Counter == 0)
                ItemsSet.erase(it);
        }

        inline bool HasOpsBelow(TRowVersion upperBound) const {
            return bool(ItemsSet) && *ItemsSet.begin() <= upperBound;
        }

    private:
        TTxIdMap TxIdMap;
        TItemsSet ItemsSet;
    };

    using TSortedOps = TMap<TStepOrder, TOperation::TPtr>;

    ///
    TDataShard * const Self;
    TDependencyTracker DepTracker;
    TConfig Config;
    THashMap<ui64, TOperation::TPtr> ImmediateOps;
    TSortedOps ActiveOps;
    TSortedOps ActivePlannedOps;
    TSortedOps::iterator ActivePlannedOpsLogicallyCompleteEnd;
    TSortedOps::iterator ActivePlannedOpsLogicallyIncompleteEnd;
    THashMap<ui64, TValidatedTx::TPtr> DataTxCache;
    TMap<TStepOrder, TStackVec<THolder<IEventHandle>, 1>> DelayedAcks;
    TStepOrder LastPlannedTx;
    TStepOrder LastCompleteTx;
    TStepOrder UtmostCompleteTx;
    ui64 KeepSchemaStep;
    ui64 LastCleanupTime;
    TSchemaOperation * SchemaTx;
    std::array<THolder<TExecutionUnit>, (ui32)EExecutionUnitKind::Count> ExecutionUnits;
    THashSet<TOperation::TPtr> ExecuteBlockers;
    // Candidates for execution.
    TMap<TStepOrder, TOperation::TPtr> CandidateOps;
    // Candidates for ready operation source (checked
    // via FindReadyOperation).
    TSet<EExecutionUnitKind> CandidateUnits;
    // Next active op found during GetNextActiveOp dry run.
    TOperation::TPtr NextActiveOp;
    // Slow operation profiles.
    TList<TStoredExecutionProfile> SlowOpProfiles;
    TMap<ui64, ui32> ActiveStreamingTxs;
    TSet<TStepOrder> PredictedPlan;

    typedef TList<TOperation::TPtr> TWaitingSchemeOpsOrder;
    typedef THashMap<TOperation::TPtr, TWaitingSchemeOpsOrder::iterator> TWaitingSchemeOps;
    TWaitingSchemeOpsOrder WaitingSchemeOpsOrder;
    TWaitingSchemeOps WaitingSchemeOps;

    struct TWaitingDataTxOp {
        TAutoPtr<IEventHandle> Event;
        NWilson::TSpan Span;

        TWaitingDataTxOp(TAutoPtr<IEventHandle>&& ev);
    };

    TMultiMap<TRowVersion, TWaitingDataTxOp> WaitingDataTxOps;
    TCommittingDataTxOps CommittingOps;

    THashMap<ui64, TOperation::TPtr> CompletingOps;

    struct TWaitingReadIterator {
        TEvDataShard::TEvRead::TPtr Event;
        NWilson::TSpan Span;

        TWaitingReadIterator(TEvDataShard::TEvRead::TPtr&& ev);
    };

    TMultiMap<TRowVersion, TWaitingReadIterator> WaitingDataReadIterators;
    THashMap<TReadIteratorId, TEvDataShard::TEvRead*, TReadIteratorId::THash> WaitingReadIteratorsById;

    bool GetPlannedTx(NIceDb::TNiceDb& db, ui64& step, ui64& txId);
    void SaveLastPlannedTx(NIceDb::TNiceDb& db, TStepOrder stepTxId);
    void CompleteTx(TOperation::TPtr op, TTransactionContext &txc, const TActorContext &ctx);
    void PersistConfig(NIceDb::TNiceDb& db);

    void MoveToNextUnit(TOperation::TPtr op);

    bool AddImmediateOp(TOperation::TPtr op);
    void RemoveImmediateOp(TOperation::TPtr op);

    void SaveInReadSet(const TEvTxProcessing::TEvReadSet &rs,
                       TTransactionContext &txc);
};

}}
