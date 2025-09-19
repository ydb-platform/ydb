#include "schemeshard__backup_collection_common.h"
#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_states.h"
#include "schemeshard_impl.h"

#define LOG_D(stream) LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_E(stream) LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpBackupIncrementalBackupCollection>;

namespace NOperation {

template <>
std::optional<THashMap<TString, THashSet<TString>>> GetRequiredPaths<TTag>(
    TTag,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    const auto& backupOp = tx.GetBackupIncrementalBackupCollection();
    return NBackup::GetBackupRequiredPaths(tx, backupOp.GetTargetDir(), backupOp.GetName(), context);
}

template <>
bool Rewrite(TTag, TTxTransaction& tx) {
    auto now = NBackup::ToX509String(TlsActivationContext->AsActorContext().Now());
    tx.MutableBackupIncrementalBackupCollection()->SetTargetDir(now + "_incremental");
    return true;
}

} // namespace NOperation

class TCreateLongIncrementalBackupOp : public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Propose:
            return MakeHolder<TEmptyPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId, TPathElement::EPathState::EPathStateNoChanges);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& workingDir = Transaction.GetWorkingDir();
        const auto& streamPathIds = Transaction.GetCreateLongIncrementalBackupOp().GetStreamPathIds();

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());

        const ui64 id = static_cast<ui64>(OperationId.GetTxId());
        if (context.SS->IncrementalBackups.contains(id)) {
            result->SetError(NKikimrScheme::StatusAlreadyExists, TStringBuilder() << "Incremental backup with id " << id << " is already exist");
            return result;
        }

        const TPath workingDirPath = TPath::Resolve(workingDir, context.SS);
        {
            TPath::TChecker checks = workingDirPath.Check();
            checks
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsLikeDirectory();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabNewLongIncrementalBackupOp(context.SS, id);

        context.DbChanges.PersistTxState(OperationId);
        context.DbChanges.PersistLongIncrementalBackupOp(id);

        TIncrementalBackupInfo::TPtr backupInfo = new TIncrementalBackupInfo(id, workingDirPath->DomainPathId);
        backupInfo->State = TIncrementalBackupInfo::EState::Transferring;

        for (const auto& streamPathId : streamPathIds) {
            TPathId pathId = TPathId::FromProto(streamPathId);

            auto& item = backupInfo->Items[pathId];
            item.PathId = pathId;
            item.State = TIncrementalBackupInfo::TItem::EState::Transferring;
        }
        backupInfo->StartTime = TAppData::TimeProvider->Now();

        if (context.UserToken) {
            backupInfo->UserSID = context.UserToken->GetUserSID();
        }
        context.SS->IncrementalBackups[id] = backupInfo;

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        auto& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateLongIncrementalBackupOp, workingDirPath->PathId);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateLongIncrementalBackupOp AbortPropose"
            << ": opId# " << OperationId);
    }

    void AbortUnsafe(TTxId txId, TOperationContext& context) override {
        LOG_N("TCreateLongIncrementalBackupOp AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << txId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

bool CreateLongIncrementalBackupOp(TOperationId opId, const TPath& bcPath, TVector<ISubOperation::TPtr>& result, const TVector<TPathId>& streams) {
    TTxTransaction tx;
    tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateLongIncrementalBackupOp);
    tx.SetInternal(true);
    tx.SetWorkingDir(bcPath.PathString());
    for (const auto& s : streams) {
        s.ToProto(tx.MutableCreateLongIncrementalBackupOp()->AddStreamPathIds());
    }

    result.push_back(CreateLongIncrementalBackupOp(NextPartId(opId, result), tx));

    return true;
}

TVector<ISubOperation::TPtr> CreateBackupIncrementalBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    TString bcPathStr = JoinPath({tx.GetWorkingDir(), tx.GetBackupIncrementalBackupCollection().GetName()});

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
    bool incrBackupEnabled = bc->Description.HasIncrementalBackupConfig();

    if (!incrBackupEnabled) {
        return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, "Incremental backup is disabled on this collection")};
    }

    TVector<TPathId> streams;
    for (const auto& item : bc->Description.GetExplicitEntryList().GetEntries()) {
        std::pair<TString, TString> paths;
        TString err;
        if (!TrySplitPathByDb(item.GetPath(), bcPath.GetDomainPathString(), paths, err)) {
            result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, err)};
            return {};
        }
        auto& relativeItemPath = paths.second;

        NKikimrSchemeOp::TModifyScheme modifyScheme;
        modifyScheme.SetWorkingDir(tx.GetWorkingDir());
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterContinuousBackup);
        modifyScheme.SetInternal(true);
        auto& cb = *modifyScheme.MutableAlterContinuousBackup();
        cb.SetTableName(relativeItemPath);
        auto& ib = *cb.MutableTakeIncrementalBackup();
        ib.SetDstPath(JoinPath({tx.GetBackupIncrementalBackupCollection().GetName(), tx.GetBackupIncrementalBackupCollection().GetTargetDir(), relativeItemPath}));

        TPathId stream;
        if (!CreateAlterContinuousBackup(opId, modifyScheme, context, result, stream)) {
            return result;
        }
        streams.push_back(stream);
    }

    CreateLongIncrementalBackupOp(opId, bcPath, result, streams);

    return result;
}

ISubOperation::TPtr CreateLongIncrementalBackupOp(TOperationId opId, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateLongIncrementalBackupOp>(opId, tx);
}

ISubOperation::TPtr CreateLongIncrementalBackupOp(TOperationId opId, TTxState::ETxState state) {
    return MakeSubOperation<TCreateLongIncrementalBackupOp>(opId, state);
}

} // namespace NKikimr::NSchemeShard
