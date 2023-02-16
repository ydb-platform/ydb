#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)


namespace {

using namespace NKikimr;
using namespace NSchemeShard;

bool IsAllowedType(ui32 typeId) {
    if (!NScheme::NTypeIds::IsYqlType(typeId)) {
        return false;
    }

    switch (typeId) {
        case NYql::NProto::Bool:
        case NYql::NProto::Interval:
        case NYql::NProto::Decimal:
        case NYql::NProto::DyNumber:
            return false;
        default:
            break;
    }
    return true;
}

TExternalTableInfo::TPtr CreateExternalTable(const NKikimrSchemeOp::TExternalTableDescription& desc, TString& errStr) {
    TExternalTableInfo::TPtr externalTableInfo = new TExternalTableInfo;
    const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;

    externalTableInfo->DataSourcePath = desc.GetDataSourcePath();
    externalTableInfo->Location = desc.GetLocation();
    externalTableInfo->AlterVersion = 1;
    externalTableInfo->Content = desc.GetContent();

    uint64_t nextColumnId = 1;
    for (const auto& col : desc.GetColumns()) {
        TString colName = col.GetName();

        if (!colName) {
            errStr = "Columns cannot have an empty name";
            return nullptr;
        }

        if (col.HasTypeId()) {
            errStr = TStringBuilder() << "Cannot set TypeId for column '" << colName << "', use Type";
            return nullptr;
        }

        if (!col.HasType()) {
            errStr = TStringBuilder() << "Missing Type for column '" << colName << "'";
            return nullptr;
        }

        auto typeName = NMiniKQL::AdaptLegacyYqlType(col.GetType());
        const NScheme::IType* type = typeRegistry->GetType(typeName);

        if (!type || !IsAllowedType(type->GetTypeId())) {
            errStr = TStringBuilder()
                << "Type '" << col.GetType() << "' specified for column '" << colName << "' is not supported";
            return nullptr;
        }

        NScheme::TTypeInfo typeInfo;
        if (type) {
            // Only allow YQL types
            if (!NScheme::NTypeIds::IsYqlType(type->GetTypeId())) {
                errStr = Sprintf("Type '%s' specified for column '%s' is no longer supported", col.GetType().data(), colName.data());
                return nullptr;
            }
            typeInfo = NScheme::TTypeInfo(type->GetTypeId());
        } else {
            auto* typeDesc = NPg::TypeDescFromPgTypeName(typeName);
            if (!typeDesc) {
                errStr = Sprintf("Type '%s' specified for column '%s' is not supported by storage", col.GetType().data(), colName.data());
                return nullptr;
            }
            typeInfo = NScheme::TTypeInfo(NScheme::NTypeIds::Pg, typeDesc);
        }

        ui32 colId = col.HasId() ? col.GetId() : nextColumnId;
        if (externalTableInfo->Columns.contains(colId)) {
            errStr = Sprintf("Duplicate column id: %" PRIu32, colId);
            return nullptr;
        }

        nextColumnId = colId + 1 > nextColumnId ? colId + 1 : nextColumnId;

        TTableInfo::TColumn& column = externalTableInfo->Columns[colId];
        column = TTableInfo::TColumn(colName, colId, typeInfo, ""); // TODO: do we need typeMod here?
        column.NotNull = col.GetNotNull();
    }

    return externalTableInfo;
}

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateExternalTable TPropose"
            << ", operationId: " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);

        LOG_I(DebugHint() << "HandleReply TEvOperationPlan"
            << ": step# " << step);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateExternalTable);

        auto pathId = txState->TargetPathId;
        auto path = TPath::Init(pathId, context.SS);
        TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);

        context.SS->TabletCounters->Simple()[COUNTER_EXTERNAL_TABLE_COUNT].Add(1);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        context.SS->ClearDescribePathCaches(pathPtr);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateExternalTable);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};


class TCreateExternalTable: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
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

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const auto ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const auto& externalTableDescription = Transaction.GetCreateExternalTable();
        const TString& name = externalTableDescription.GetName();


        LOG_N("TCreateExternalTable Propose"
            << ": opId# " << OperationId
            << ", path# " << parentPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
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

        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        NSchemeShard::TPath dstPath = parentPath.Child(name);
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeExternalTable, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .IsValidLeafName()
                    .DepthLimit()
                    .PathsLimit()
                    .DirChildrenLimit()
                    .IsValidACL(acl);
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TExternalTableInfo::TPtr externalTableInfo = CreateExternalTable(externalTableDescription, errStr);
        if (!externalTableInfo) {
            result->SetError(NKikimrScheme::StatusSchemeError, errStr);
            return result;
        }

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        TPathElement::TPtr externalTable = dstPath.Base();
        externalTable->CreateTxId = OperationId.GetTxId();
        externalTable->LastTxId = OperationId.GetTxId();
        externalTable->PathState = TPathElement::EPathState::EPathStateCreate;
        externalTable->PathType = TPathElement::EPathType::EPathTypeExternalTable;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateExternalTable, externalTable->PathId);
        txState.Shards.clear();

        NIceDb::TNiceDb db(context.GetDB());

        if (parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->ExternalTables[externalTable->PathId] = externalTableInfo;
        context.SS->IncrementPathDbRefCount(externalTable->PathId);

        context.SS->PersistPath(db, externalTable->PathId);

        if (!acl.empty()) {
            externalTable->ApplyACL(acl);
            context.SS->PersistACL(db, externalTable);
        }

        context.SS->PersistExternalTable(db, externalTable->PathId, externalTableInfo);
        context.SS->PersistTxState(db, OperationId);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TCreateExternalTable AbortPropose"
            << ": opId# " << OperationId);
        Y_FAIL("no AbortPropose for TCreateExternalTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TCreateExternalTable AbortUnsafe"
            << ": opId# " << OperationId
            << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateNewExternalTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateExternalTable>(id, tx);
}

ISubOperation::TPtr CreateNewExternalTable(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return MakeSubOperation<TCreateExternalTable>(id, state);
}

}
