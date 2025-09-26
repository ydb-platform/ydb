#include "schemeshard__operation_incremental_restore_finalize.h"

#include "schemeshard__operation_base.h"
#include "schemeshard__operation_common.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_W(stream) LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

class TIncrementalRestoreFinalizeOp: public TSubOperationWithContext {
    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch(state) {
        case TTxState::Waiting:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return MakeHolder<TFinalizationPropose>(OperationId, Transaction);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    class TFinalizationPropose: public TSubOperationState {
    private:
        TOperationId OperationId;
        TTxTransaction Transaction;

        TString DebugHint() const override {
            return TStringBuilder()
                << "TIncrementalRestoreFinalize TPropose"
                << " operationId: " << OperationId;
        }

    public:
        TFinalizationPropose(TOperationId id, const TTxTransaction& tx) 
            : OperationId(id), Transaction(tx) {}

        bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
            return TSubOperationState::HandleReply(ev, context);
        }

        bool ProgressState(TOperationContext& context) override {
            LOG_I(DebugHint() << " ProgressState");

            TTxState* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);

            const auto& finalize = Transaction.GetIncrementalRestoreFinalize();

            // Release all affected path states to EPathStateNoChanges
            TVector<TPathId> pathsToNormalize;
            CollectPathsToNormalize(finalize, context, pathsToNormalize);

            for (const auto& pathId : pathsToNormalize) {
                context.OnComplete.ReleasePathState(OperationId, pathId, 
                    TPathElement::EPathState::EPathStateNoChanges);
            }

            PerformFinalCleanup(finalize, context);

            {
                ui64 originalOpId = finalize.GetOriginalOperationId();
                NIceDb::TNiceDb db(context.GetDB());
                
                auto stateIt = context.SS->IncrementalRestoreStates.find(originalOpId);
                if (stateIt != context.SS->IncrementalRestoreStates.end()) {
                    const auto& involvedShards = stateIt->second.InvolvedShards;

                    LOG_I("Cleaning up " << involvedShards.size() << " shard progress entries for operation " << originalOpId);

                    for (const auto& shardIdx : involvedShards) {
                        db.Table<Schema::IncrementalRestoreShardProgress>()
                            .Key(originalOpId, ui64(shardIdx.GetLocalId()))
                            .Delete();
                    }
                }
                
                db.Table<Schema::IncrementalRestoreState>().Key(originalOpId).Delete();
            }

            context.OnComplete.DoneOperation(OperationId);
            return true;
        }

    private:
        void CollectPathsToNormalize(const NKikimrSchemeOp::TIncrementalRestoreFinalize& finalize, 
                                   TOperationContext& context,
                                   TVector<TPathId>& pathsToNormalize) {
            
            // Collect target table paths
            for (const auto& tablePath : finalize.GetTargetTablePaths()) {
                TPath path = TPath::Resolve(tablePath, context.SS);
                if (path.IsResolved()) {
                    TPathId pathId = path.Base()->PathId;
                    if (auto* pathInfo = context.SS->PathsById.FindPtr(pathId)) {
                        if ((*pathInfo)->PathState == NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore) {
                            pathsToNormalize.push_back(pathId);
                            LOG_I("Adding target table path to normalize: " << tablePath);
                        }
                    }
                }
            }
            
            // Collect backup table paths
            for (const auto& backupTablePath : finalize.GetBackupTablePaths()) {
                TPath path = TPath::Resolve(backupTablePath, context.SS);
                if (path.IsResolved()) {
                    TPathId pathId = path.Base()->PathId;
                    if (auto* pathInfo = context.SS->PathsById.FindPtr(pathId)) {
                        if ((*pathInfo)->PathState == NKikimrSchemeOp::EPathState::EPathStateOutgoingIncrementalRestore ||
                            (*pathInfo)->PathState == NKikimrSchemeOp::EPathState::EPathStateAwaitingOutgoingIncrementalRestore) {
                            pathsToNormalize.push_back(pathId);
                            LOG_I("Adding backup table path to normalize: " << backupTablePath);
                        }
                    }
                }
            }
        }

        void PerformFinalCleanup(const NKikimrSchemeOp::TIncrementalRestoreFinalize& finalize, 
                                TOperationContext& context) {
            ui64 originalOpId = finalize.GetOriginalOperationId();
            
            NIceDb::TNiceDb db(context.GetDB());
            
            auto stateIt = context.SS->IncrementalRestoreStates.find(originalOpId);
            if (stateIt != context.SS->IncrementalRestoreStates.end()) {
                auto& state = stateIt->second;
                state.State = TIncrementalRestoreState::EState::Completed;
                
                LOG_I("Marked incremental restore state as completed for operation: " << originalOpId);
            }
            
            LOG_I("Keeping IncrementalRestoreOperations entry for operation: " << originalOpId << " - will be cleaned up on FORGET");
            
            context.SS->LongIncrementalRestoreOps.erase(TOperationId(originalOpId, 0));
            LOG_I("Cleaned up long incremental restore ops for operation: " << originalOpId);
            
            CleanupMappings(context.SS, originalOpId, context);
        }

        void CleanupMappings(TSchemeShard* ss, ui64 operationId, TOperationContext& context) {
            auto txIt = ss->TxIdToIncrementalRestore.begin();
            while (txIt != ss->TxIdToIncrementalRestore.end()) {
                if (txIt->second == operationId) {
                    auto toErase = txIt++;
                    ss->TxIdToIncrementalRestore.erase(toErase);
                } else {
                    ++txIt;
                }
            }
            
            auto opIt = ss->IncrementalRestoreOperationToState.begin();
            while (opIt != ss->IncrementalRestoreOperationToState.end()) {
                if (opIt->second == operationId) {
                    auto toErase = opIt++;
                    ss->IncrementalRestoreOperationToState.erase(toErase);
                } else {
                    ++opIt;
                }
            }
            
            LOG_I("Cleaned up mappings for operation: " << operationId);
        }
    };

    class TDone: public TSubOperationState {
    private:
        TOperationId OperationId;

        TString DebugHint() const override {
            return TStringBuilder()
                << "TIncrementalRestoreFinalize TDone"
                << " operationId: " << OperationId;
        }

    public:
        TDone(TOperationId id) : OperationId(id) {}

        bool ProgressState(TOperationContext& context) override {
            LOG_I(DebugHint() << " ProgressState");
            // Operation is already complete, nothing to do
            return true;
        }
    };

public:
    using TSubOperationWithContext::TSubOperationWithContext;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& tx = Transaction;
        const TTabletId schemeshardTabletId = context.SS->SelfTabletId();
        
        LOG_I("TIncrementalRestoreFinalizeOp Propose"
            << ", opId: " << OperationId
        );

        const auto& finalize = tx.GetIncrementalRestoreFinalize();
        ui64 originalOpId = finalize.GetOriginalOperationId();

        // Validate that we have the restore state
        auto stateIt = context.SS->IncrementalRestoreStates.find(originalOpId);
        if (stateIt == context.SS->IncrementalRestoreStates.end()) {
            return MakeHolder<TProposeResponse>(NKikimrScheme::StatusPreconditionFailed, 
                ui64(OperationId.GetTxId()), ui64(schemeshardTabletId),
                "Incremental restore state not found for operation: " + ToString(originalOpId));
        }

        Y_VERIFY_S(!context.SS->FindTx(OperationId), 
            "TIncrementalRestoreFinalizeOp Propose: operation already exists"
            << ", opId: " << OperationId);

        // Use backup collection path as domain path
        TPathId backupCollectionPathId(context.SS->TabletID(), finalize.GetBackupCollectionPathId());
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxIncrementalRestoreFinalize, backupCollectionPathId);
        
        txState.TargetPathId = backupCollectionPathId;
        
        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(schemeshardTabletId));

        txState.State = TTxState::Waiting;
        context.DbChanges.PersistTxState(OperationId);
        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState(TTxState::Waiting), context);
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TIncrementalRestoreFinalizeOp AbortPropose"
            << ", opId: " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TIncrementalRestoreFinalizeOp AbortUnsafe"
            << ", opId: " << OperationId
            << ", forceDropId: " << forceDropTxId);
        
        // TODO: Handle abort if needed
    }
};

ISubOperation::TPtr CreateIncrementalRestoreFinalize(TOperationId opId, const TTxTransaction& tx) {
    return MakeSubOperation<TIncrementalRestoreFinalizeOp>(opId, tx);
}

ISubOperation::TPtr CreateIncrementalRestoreFinalize(TOperationId opId, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TIncrementalRestoreFinalizeOp>(opId, state);
}

}
