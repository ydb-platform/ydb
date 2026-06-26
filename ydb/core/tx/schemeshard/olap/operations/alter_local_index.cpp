#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TAlterLocalIndex TPropose"
            << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);

        YDB_LOG_INFO_CTX(context.Ctx, "HandleReply TEvOperationPlan",
            {"debugHint", DebugHint()},
            {"step", step},
            {"atSchemeshard", context.SS->TabletID()});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterLocalIndex);
        Y_ABORT_UNLESS(txState->State == TTxState::Propose);

        NIceDb::TNiceDb db(context.GetDB());

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(context.SS->Indexes.contains(path->PathId));
        TTableIndexInfo::TPtr indexData = context.SS->Indexes.at(path->PathId);
        Y_ABORT_UNLESS(indexData->AlterData, "AlterData must be valid after TTableIndexInfo::Create");
        context.SS->PersistTableIndex(db, path->PathId);
        context.SS->Indexes[path->PathId] = indexData->AlterData;

        auto parentPath = TPath::Init(path->PathId, context.SS).Parent();
        ++parentPath->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath.Base());

        context.SS->ClearDescribePathCaches(path);
        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, path->PathId);
        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentPath->PathId);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "ProgressState",
            {"debugHint", DebugHint()},
            {"atSchemeshard", context.SS->TabletID()});

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterLocalIndex);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TAlterLocalIndex: public TSubOperation {
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
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        if (!Transaction.HasAlterTableIndex()) {
            auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusInvalidParameter, ui64(OperationId.GetTxId()), ui64(ssId));
            result->SetError(NKikimrScheme::StatusInvalidParameter, "AlterTableIndex is not present");
            return result;
        }

        const auto& tableIndexAlter = Transaction.GetAlterTableIndex();
        const TString& parentPathStr = Transaction.GetWorkingDir();

        if (!tableIndexAlter.HasName()) {
            auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusInvalidParameter, ui64(OperationId.GetTxId()), ui64(ssId));
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Name is not present in AlterTableIndex");
            return result;
        }

        const TString& name = tableIndexAlter.GetName();

        YDB_LOG_NOTICE_CTX(context.Ctx, "TAlterLocalIndex Propose /",
            {"path", parentPathStr},
            {"name", name},
            {"operationId", OperationId},
            {"atSchemeshard", ssId});

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        NSchemeShard::TPath indexPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            NSchemeShard::TPath::TChecker checks = indexPath.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsTableIndex();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        // Validate parent path
        NSchemeShard::TPath parentPath = indexPath.Parent();
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsUnderOperation()
                .IsUnderTheSameOperation(OperationId.GetTxId())
                .IsColumnTable();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        auto indexIt = context.SS->Indexes.find(indexPath.Base()->PathId);
        if (indexIt == context.SS->Indexes.end() || indexIt->second->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Index is already being altered");
            return result;
        }

        TString errStr;
        TTableIndexInfo::TPtr newIndexData = TTableIndexInfo::Create(tableIndexAlter, errStr);
        if (!newIndexData) {
            result->SetError(TEvSchemeShard::EStatus::StatusInvalidParameter, errStr);
            return result;
        }

        // Validate that the new index type matches the existing index type
        if (newIndexData->AlterData->Type != indexIt->second->Type) {
            result->SetError(NKikimrScheme::StatusInvalidParameter,
                TStringBuilder() << "Cannot alter index type from "
                    << NKikimrSchemeOp::EIndexType_Name(indexIt->second->Type)
                    << " to "
                    << NKikimrSchemeOp::EIndexType_Name(newIndexData->AlterData->Type));
            return result;
        }

        // Single source of truth for which local-index types are supported and
        // which variant alternative each one requires. Adding a new type means
        // adding one case here. Used to validate both the existing index and the
        // requested alter; the variant copy below relies on this invariant.
        auto checkLocalIndex = [](const TTableIndexInfo& info, TStringBuf what) -> std::optional<TString> {
            switch (info.Type) {
                case NKikimrSchemeOp::EIndexTypeLocalBloomFilter:
                    if (!std::holds_alternative<NKikimrSchemeOp::TBloomFilter>(info.SpecializedIndexDescription)) {
                        return TStringBuilder() << what << " SpecializedIndexDescription does not hold TBloomFilter for index type LocalBloomFilter";
                    }
                    return std::nullopt;
                case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter:
                    if (!std::holds_alternative<NKikimrSchemeOp::TBloomNGrammFilter>(info.SpecializedIndexDescription)) {
                        return TStringBuilder() << what << " SpecializedIndexDescription does not hold TBloomNGrammFilter for index type LocalBloomNgramFilter";
                    }
                    return std::nullopt;
                case NKikimrSchemeOp::EIndexTypeLocalMinMax:
                    if (!std::holds_alternative<std::monostate>(info.SpecializedIndexDescription)) {
                        return TStringBuilder() << what << " SpecializedIndexDescription is not empty for index type LocalMinMax";
                    }
                    return std::nullopt;
                default:
                    return TStringBuilder() << "Unexpected index type " << static_cast<int>(info.Type)
                        << " in TAlterLocalIndex::Propose. Only local bloom filter and min_max types are supported.";
            }
        };

        // Existing index and requested alter must both be supported types whose
        // variant alternatives match the declared Type. CreateNextVersion()
        // preserves the alternative from indexIt->second, so this also covers
        // alterData's variant after DbGuard.
        if (auto err = checkLocalIndex(*indexIt->second, "existing")) {
            result->SetError(NKikimrScheme::StatusSchemeError, *err);
            return result;
        }
        if (auto err = checkLocalIndex(*newIndexData->AlterData, "requested")) {
            result->SetError(NKikimrScheme::StatusSchemeError, *err);
            return result;
        }

        auto guard = context.DbGuard();
        context.MemChanges.GrabPath(context.SS, indexPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabIndex(context.SS, indexPath.Base()->PathId);

        context.DbChanges.PersistPath(indexPath.Base()->PathId);
        context.DbChanges.PersistAlterIndex(indexPath.Base()->PathId);
        context.DbChanges.PersistTxState(OperationId);

        TTableIndexInfo::TPtr alterData = indexIt->second->CreateNextVersion();
        if (!newIndexData->AlterData->IndexKeys.empty()) {
            alterData->IndexKeys = newIndexData->AlterData->IndexKeys;
        }
        alterData->State = NKikimrSchemeOp::EIndexStateReady;
        alterData->SpecializedIndexDescription = newIndexData->AlterData->SpecializedIndexDescription;

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterLocalIndex, indexPath.Base()->PathId);
        txState.State = TTxState::Propose;

        indexPath.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
        indexPath.Base()->LastTxId = OperationId.GetTxId();

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TAlterLocalIndex AbortPropose",
            {"opId", OperationId},
            {"atSchemeshard", context.SS->TabletID()});
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TAlterLocalIndex AbortUnsafe",
            {"opId", OperationId},
            {"forceDropId", forceDropTxId},
            {"atSchemeshard", context.SS->TabletID()});

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterColumnTableLocalIndex(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterLocalIndex>(id, tx);
}

ISubOperation::TPtr CreateAlterColumnTableLocalIndex(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TAlterLocalIndex>(id, state);
}

} // namespace NKikimr::NSchemeShard
