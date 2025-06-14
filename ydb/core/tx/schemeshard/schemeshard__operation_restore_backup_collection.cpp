#include "schemeshard__backup_collection_common.h"
#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation.h"

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

class TWaitCopyTableBarrier: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateRestoreOpControlPlane::TWaitCopyTableBarrier"
                << " operationId: " << OperationId;
    }

public:
    TWaitCopyTableBarrier(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            { TEvHive::TEvCreateTabletReply::EventType
            , TEvDataShard::TEvProposeTransactionResult::EventType
            , TEvPrivate::TEvOperationPlan::EventType
            , TEvDataShard::TEvSchemaChanged::EventType }
        );
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvCompleteBarrier"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet# " << ssId);

        NIceDb::TNiceDb db(context.GetDB());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << "ProgressState, operation type "
                            << TTxState::TypeName(txState->TxType));

        context.OnComplete.Barrier(OperationId, "CopyTableBarrier");
        return false;
    }
};

class TCreateRestoreOpControlPlane: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Waiting;
    }

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
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::CopyTableBarrier:
            return MakeHolder<TWaitCopyTableBarrier>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& tx = Transaction;
        const TTabletId schemeshardTabletId = context.SS->SelfTabletId();
        LOG_I("TCreateRestoreOpControlPlane Propose"
            << ", opId: " << OperationId
        );

        TString bcPathStr = JoinPath({tx.GetWorkingDir(), tx.GetRestoreBackupCollection().GetName()});

        const TPath& bcPath = TPath::Resolve(bcPathStr, context.SS);

        const auto& bc = context.SS->BackupCollections[bcPath->PathId];

        // Create in-flight operation object
        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateLongIncrementalRestoreOp, bcPath.GetPathIdForDomain()); // Fix PathId to backup collection PathId

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(schemeshardTabletId));

        // Persist alter
        // context.DbChanges.PersistSubDomainAlter(basenameId);
        txState.State = TTxState::Waiting;

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

        TStringBuf fullBackupName = lastFullBackupName;
        fullBackupName.ChopSuffix("_full"_sb);

        op.SetFullBackupTrimmedName(TString(fullBackupName));

        for (const auto& backupName : incrBackupNames) {
            TStringBuf incrBackupName = backupName;
            incrBackupName.ChopSuffix("_incremental"_sb);

            op.SetFullBackupTrimmedName(TString(incrBackupName));
        }

        context.DbChanges.PersistLongIncrementalRestoreOp(op);

        // Set initial operation state
        SetState(NextState());

        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TCreateRestoreOpControlPlane");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateRestoreOpControlPlane AbortUnsafe"
            << ", opId: " << OperationId
            << ", forceDropId: " << forceDropTxId
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

bool CreateLongIncrementalRestoreOp(
    TOperationId opId,
    const TPath& bcPath,
    TVector<ISubOperation::TPtr>& result)
{
    // Create a transaction for the long incremental restore operation
    TTxTransaction tx;
    tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateLongIncrementalRestoreOp);
    tx.SetInternal(true);
    
    // Set the backup collection path as the working directory for this operation
    tx.SetWorkingDir(bcPath.PathString());
    
    // Use the factory function to create the control plane sub-operation
    result.push_back(CreateLongIncrementalRestoreOpControlPlane(opId, tx));
    
    return true;
}

ISubOperation::TPtr CreateLongIncrementalRestoreOpControlPlane(TOperationId opId, const TTxTransaction& tx) {
    // Create the control plane sub-operation directly for operation factory dispatch
    return new TCreateRestoreOpControlPlane(opId, tx);
}

TVector<ISubOperation::TPtr> CreateRestoreBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    TString bcPathStr = JoinPath({tx.GetWorkingDir(), tx.GetRestoreBackupCollection().GetName()});

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

    CreateConsistentCopyTables(NextPartId(opId, result), consistentCopyTables, context, result);

    CreateLongIncrementalRestoreOp(NextPartId(opId, result), bcPath, result);

    return result;
}

} // namespace NKikimr::NSchemeShard
