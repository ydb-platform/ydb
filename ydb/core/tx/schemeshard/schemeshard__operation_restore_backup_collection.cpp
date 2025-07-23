#include "schemeshard__operation_restore_backup_collection.h"

#include "schemeshard__backup_collection_common.h"
#include "schemeshard__op_traits.h"
#include "schemeshard__operation.h"
#include "schemeshard__operation_base.h"
#include "schemeshard__operation_change_path_state.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_states.h"

#include <ydb/core/base/test_failure_injection.h>

#include <util/generic/guid.h>

#define LOG_D(stream) LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_E(stream) LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpRestoreBackupCollection>;

namespace NOperation {

template <>
std::optional<THashMap<TString, THashSet<TString>>> GetRequiredPaths<TTag>(
    TTag,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    const auto& restoreOp = tx.GetRestoreBackupCollection();
    return NBackup::GetRestoreRequiredPaths(tx, restoreOp.GetName(), context);
}

} // namespace NOperation

// Forward declarations
bool CreateLongIncrementalRestoreOp(
    TOperationId opId,
    const TPath& bcPath,
    TVector<ISubOperation::TPtr>& result);

class TDoneWithIncrementalRestore: public TDone {
public:
    explicit TDoneWithIncrementalRestore(const TOperationId& id)
        : TDone(id)
    {
        auto events = AllIncomingEvents();
        events.erase(TEvPrivate::TEvCompleteBarrier::EventType);
        IgnoreMessages(DebugHint(), events);
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        context.OnComplete.Barrier(OperationId, "DoneBarrier");
        return false;
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr&, TOperationContext& context) override {
        LOG_I(DebugHint() << "HandleReply TEvCompleteBarrier");

        if (!TDone::Process(context)) {
            return false;
        }

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateLongIncrementalRestoreOp);
        const auto& targetPathId = txState->TargetPathId;

        Y_ABORT_UNLESS(context.SS->PathsById.contains(targetPathId));
        auto path = context.SS->PathsById.at(targetPathId);

        // Find the backup collection path from the long incremental restore operation
        auto itOp = context.SS->LongIncrementalRestoreOps.find(OperationId);
        if (itOp == context.SS->LongIncrementalRestoreOps.end()) {
            LOG_E(DebugHint() << "Failed to find long incremental restore operation");
            return false;
        }

        const auto& op = itOp->second;
        TPathId backupCollectionPathId;
        backupCollectionPathId.OwnerId = op.GetBackupCollectionPathId().GetOwnerId();
        backupCollectionPathId.LocalPathId = op.GetBackupCollectionPathId().GetLocalId();

        if (AppData()->HasInjectedFailure(static_cast<ui64>(EInjectedFailureType::DisableIncrementalRestoreAutoSwitchingToReadyStateForTests))) {
            return true;
        }

        // Extract incremental backup names from the operation
        TVector<TString> incrementalBackupNames;
        for (const auto& name : op.GetIncrementalBackupTrimmedNames()) {
            incrementalBackupNames.push_back(name);
        }

        LOG_I(DebugHint() << " Found " << incrementalBackupNames.size() << " incremental backups to restore");

        context.OnComplete.Send(context.SS->SelfId(), new TEvPrivate::TEvRunIncrementalRestore(backupCollectionPathId, OperationId, incrementalBackupNames));

        return true;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder()
            << "TDoneWithIncrementalRestore"
            << ", operationId: " << OperationId;
    }

}; // TDoneWithIncrementalRestore

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateRestoreOpControlPlane::TPropose"
            << ", operationId: " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(
        TEvPrivate::TEvOperationPlan::TPtr& ev,
        TOperationContext& context) override
    {
        const auto step = TStepId(ev->Get()->StepId);
        const auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            DebugHint() << " HandleReply TEvOperationPlan"
            << ", step: " << step
            << ", at schemeshard: " << ssId);

        auto* txState = context.SS->FindTx(OperationId);
        if (!txState) {
            return false;
        }

        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateLongIncrementalRestoreOp);
 
        // NIceDb::TNiceDb db(context.GetDB());
        // TODO

        context.OnComplete.DoneOperation(OperationId);

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        const auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            DebugHint() << " ProgressState"
            << ", at schemeshard: " << ssId);

        auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateLongIncrementalRestoreOp);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TCreateRestoreOpControlPlane: public TSubOperationWithContext {
    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch(state) {
        case TTxState::Waiting:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::CopyTableBarrier;
        case TTxState::CopyTableBarrier:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return MakeHolder<TEmptyPropose>(OperationId);
        case TTxState::CopyTableBarrier:
            return MakeHolder<TWaitCopyTableBarrier>(OperationId, "TCreateRestoreOpControlPlane");
        case TTxState::Done:
            return MakeHolder<TDoneWithIncrementalRestore>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    TCreateRestoreOpControlPlane(TOperationId id, const TTxTransaction& tx)
        : TSubOperationWithContext(id, tx)
    {
    }

    TCreateRestoreOpControlPlane(TOperationId id, TTxState::ETxState state)
        : TSubOperationWithContext(id, state)
    {
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        if (AppData()->HasInjectedFailure(static_cast<ui64>(EInjectedFailureType::LateBackupCollectionNotFound))) {
            return MakeHolder<TProposeResponse>(NKikimrScheme::StatusPathDoesNotExist, ui64(OperationId.GetTxId()), ui64(context.SS->SelfTabletId()));
        }

        const auto& tx = Transaction;
        const TTabletId schemeshardTabletId = context.SS->SelfTabletId();
        LOG_I("TCreateRestoreOpControlPlane Propose"
            << ", opId: " << OperationId
        );

        TString bcPathStr = JoinPath({tx.GetWorkingDir(), tx.GetRestoreBackupCollection().GetName()});

        const TPath& bcPath = TPath::Resolve(bcPathStr, context.SS);

        if (!bcPath.IsResolved()) {
            return MakeHolder<TProposeResponse>(NKikimrScheme::StatusPathDoesNotExist, ui64(OperationId.GetTxId()), ui64(schemeshardTabletId));
        }

        const auto& bc = context.SS->BackupCollections[bcPath->PathId];

        // Create in-flight operation object
        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateLongIncrementalRestoreOp, bcPath.GetPathIdForDomain()); // Fix PathId to backup collection PathId

        txState.TargetPathTargetState = static_cast<NKikimrSchemeOp::EPathState>(NKikimrSchemeOp::EPathStateOutgoingIncrementalRestore);

        // Set the target path ID for coordinator communication
        txState.TargetPathId = bcPath.Base()->PathId;
        bcPath.Base()->PathState = *txState.TargetPathTargetState;

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(schemeshardTabletId));

        txState.State = TTxState::Waiting;

        // Add source tables from backup collection to transaction paths for proper state tracking
        TString lastFullBackupName;
        TVector<TString> incrBackupNames;

        for (auto& [child, _] : bcPath.Base()->GetChildren()) {
            if (child.EndsWith("_full")) {
                lastFullBackupName = child;
                incrBackupNames.clear();
            } else if (child.EndsWith("_incremental")) {
                incrBackupNames.push_back(child);
            }
        }

        context.DbChanges.PersistTxState(OperationId);
        context.OnComplete.ActivateTx(OperationId);

        NKikimrSchemeOp::TLongIncrementalRestoreOp op;

        op.SetTxId(ui64(OperationId.GetTxId()));

        // Create deterministic UUID for test reproducibility
        // Using parts from OperationId to ensure uniqueness within the same SchemeShard
        const ui64 txId = ui64(OperationId.GetTxId());
        // Create deterministic GUID from txId for test reproducibility
        TGUID uuid;
        uuid.dw[0] = static_cast<ui32>(txId);
        uuid.dw[1] = static_cast<ui32>(txId >> 32);
        uuid.dw[2] = static_cast<ui32>(txId ^ 0xDEADBEEF);
        uuid.dw[3] = static_cast<ui32>((txId ^ 0xCAFEBABE) >> 32);
        op.SetId(uuid.AsGuidString());

        bcPath->PathId.ToProto(op.MutableBackupCollectionPathId());

        for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
            if (item.GetType() == ::NKikimrSchemeOp::TBackupCollectionDescription_TBackupEntry_EType_ETypeTable) {
                op.AddTablePathList(item.GetPath());
            }
        }

        TStringBuf fullBackupName = lastFullBackupName;
        fullBackupName.ChopSuffix("_full"_sb);

        op.SetFullBackupTrimmedName(TString(fullBackupName));

        for (const auto& backupName : incrBackupNames) {
            TStringBuf incrBackupName = backupName;
            incrBackupName.ChopSuffix("_incremental"_sb);

            op.AddIncrementalBackupTrimmedNames(TString(incrBackupName));
        }

        context.MemChanges.GrabNewLongIncrementalRestoreOp(context.SS, OperationId);
        context.SS->LongIncrementalRestoreOps[OperationId] = op;
        context.DbChanges.PersistLongIncrementalRestoreOp(op);

        // Set initial operation state
        SetState(NextState(TTxState::Waiting), context);

        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateRestoreOpControlPlane AbortPropose"
            << ", opId: " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateRestoreOpControlPlane AbortUnsafe"
            << ", opId: " << OperationId
            << ", forceDropId: " << forceDropTxId
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

ISubOperation::TPtr CreateLongIncrementalRestoreOpControlPlane(TOperationId opId, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateRestoreOpControlPlane>(opId, tx);
}

ISubOperation::TPtr CreateLongIncrementalRestoreOpControlPlane(TOperationId opId, TTxState::ETxState state) {
    return MakeSubOperation<TCreateRestoreOpControlPlane>(opId, state);
}

TVector<ISubOperation::TPtr> CreateRestoreBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    TString bcPathStr = JoinPath({tx.GetWorkingDir(), tx.GetRestoreBackupCollection().GetName()});

    if (AppData()->HasInjectedFailure(static_cast<ui64>(EInjectedFailureType::BackupCollectionNotFound))) {
        result = {CreateReject(opId, NKikimrScheme::StatusPathDoesNotExist, "Backup collection not found")};
        return result;
    }

    if (AppData()->HasInjectedFailure(static_cast<ui64>(EInjectedFailureType::BackupChildrenEmpty))) {
        result = {CreateReject(opId, NKikimrScheme::StatusSchemeError, "Backup collection children empty")};
        return result;
    }

    if (AppData()->HasInjectedFailure(static_cast<ui64>(EInjectedFailureType::PathSplitFailure))) {
        result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, "Path split failure")};
        return result;
    }

    if (AppData()->HasInjectedFailure(static_cast<ui64>(EInjectedFailureType::IncrementalBackupPathNotResolved))) {
        result = {CreateReject(opId, NKikimrScheme::StatusPathDoesNotExist, "Incremental backup path not resolved")};
        return result;
    }

    if (AppData()->HasInjectedFailure(static_cast<ui64>(EInjectedFailureType::CreateChangePathStateFailed))) {
        result = {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, "Create change path state failed")};
        return result;
    }

    const TPath& bcPath = TPath::Resolve(bcPathStr, context.SS);
    {
        auto checks = bcPath.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsBackupCollection();

        if (!checks) {
            result = {CreateReject(opId, checks.GetStatus(), checks.GetError())};
            return result;
        }
    }

    Y_ABORT_UNLESS(context.SS->BackupCollections.contains(bcPath->PathId));
    const auto& bc = context.SS->BackupCollections[bcPath->PathId];

    TString lastFullBackupName;
    TVector<TString> incrBackupNames;

    if (!bcPath.Base()->GetChildren().size()) {
        return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, TStringBuilder() << "Nothing to restore")};
    } else {
        static_assert(
            std::is_same_v<
                TMap<TString, TPathId>,
                std::decay_t<decltype(bcPath.Base()->GetChildren())>> == true,
            "Assume path children list is lexicographically sorted");

        for (auto& [child, _] : bcPath.Base()->GetChildren()) {
            if (child.EndsWith("_full")) {
                lastFullBackupName = child;
                incrBackupNames.clear();
            } else if (child.EndsWith("_incremental")) {
                incrBackupNames.push_back(child);
            }
        }
    }

    NKikimrSchemeOp::TModifyScheme consistentCopyTables;
    consistentCopyTables.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables);
    consistentCopyTables.SetInternal(true);
    consistentCopyTables.SetWorkingDir(tx.GetWorkingDir());

    auto& cct = *consistentCopyTables.MutableCreateConsistentCopyTables();
    auto& copyTables = *cct.MutableCopyTableDescriptions();
    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);

    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
        std::pair<TString, TString> paths;
        TString err;
        if (!TrySplitPathByDb(item.GetPath(), bcPath.GetDomainPathString(), paths, err)) {
            result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, err)};
            return {};
        }
        auto& relativeItemPath = paths.second;

        auto& desc = *copyTables.Add();
        desc.SetSrcPath(JoinPath({tx.GetWorkingDir(), tx.GetRestoreBackupCollection().GetName(), lastFullBackupName, relativeItemPath}));
        desc.SetDstPath(item.GetPath());
        desc.SetAllowUnderSameOperation(true);
        if (incrBackupNames) {
            desc.SetTargetPathTargetState(NKikimrSchemeOp::EPathStateIncomingIncrementalRestore);
        }
    }

    CreateConsistentCopyTables(opId, consistentCopyTables, context, result);

    if (incrBackupNames) {
        // op id increased internally
        if(!CreateIncrementalBackupPathStateOps(opId, tx, bc, bcPath, incrBackupNames, context, result)) {
            return result;
        }

        // we don't need long op when we don't have incremental backups
        CreateLongIncrementalRestoreOp(opId, bcPath, result);
    }

    return result;
}

bool CreateLongIncrementalRestoreOp(
    TOperationId opId,
    const TPath& bcPath,
    TVector<ISubOperation::TPtr>& result)
{
    TTxTransaction tx;
    tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateLongIncrementalRestoreOp);
    tx.SetInternal(true);

    tx.SetWorkingDir(bcPath.PathString());

    result.push_back(CreateLongIncrementalRestoreOpControlPlane(NextPartId(opId, result), tx));

    return true;
}

bool CreateIncrementalBackupPathStateOps(
    TOperationId opId,
    const TTxTransaction& tx,
    const TBackupCollectionInfo::TPtr& bc,
    const TPath& bcPath,
    const TVector<TString>& incrBackupNames,
    TOperationContext& context,
    TVector<ISubOperation::TPtr>& result)
{
    for (const auto& incrBackupName : incrBackupNames) {
        // Create path state change operations for each table in each incremental backup
        for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
            std::pair<TString, TString> paths;
            TString err;
            if (!TrySplitPathByDb(item.GetPath(), bcPath.GetDomainPathString(), paths, err)) {
                result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, err)};
                return false;
            }
            auto& relativeItemPath = paths.second;

            // Check if the incremental backup path exists
            TString incrBackupPathStr = JoinPath({tx.GetWorkingDir(), tx.GetRestoreBackupCollection().GetName(), incrBackupName, relativeItemPath});
            const TPath& incrBackupPath = TPath::Resolve(incrBackupPathStr, context.SS);
            
            // Only create path state change operation if the path exists
            if (incrBackupPath.IsResolved()) {
                // Create transaction for path state change
                TTxTransaction pathStateChangeTx;
                pathStateChangeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpChangePathState);
                pathStateChangeTx.SetInternal(true);
                pathStateChangeTx.SetWorkingDir(tx.GetWorkingDir());

                auto& changePathState = *pathStateChangeTx.MutableChangePathState();
                changePathState.SetPath(JoinPath({tx.GetRestoreBackupCollection().GetName(), incrBackupName, relativeItemPath}));
                changePathState.SetTargetState(NKikimrSchemeOp::EPathStateAwaitingOutgoingIncrementalRestore);

                // Create the operation immediately after calling NextPartId to maintain proper sequencing
                if (!CreateChangePathState(opId, pathStateChangeTx, context, result)) {
                    return false;
                }
            }
        }
    }
    return true;
}

} // namespace NKikimr::NSchemeShard
