#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;


class TCreateLockForIndexBuild: public TSubOperation {
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State = TTxState::Invalid;

    TTxState::ETxState NextState() {
        return TTxState::Done;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Waiting:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::Done:
            return THolder(new TDone(OperationId));
        default:
            return nullptr;
        }
    }

    void StateDone(TOperationContext& context) override {
        State = NextState(State);

        if (State != TTxState::Invalid) {
            SetState(SelectStateFunc(State));
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    TCreateLockForIndexBuild(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
          , Transaction(tx)
    {
    }

    TCreateLockForIndexBuild(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
          , State(state)
    {
        SetState(SelectStateFunc(state));
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        auto lockSchema = Transaction.GetLockConfig();

        const TString& parentPathStr = Transaction.GetWorkingDir();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateLockForIndexBuild Propose"
                         << ", path: " << parentPathStr << "/" << lockSchema.GetName()
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!Transaction.HasLockConfig()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "no locking config present");
            return result;
        }

        if (!Transaction.HasInitiateIndexBuild()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "no build index config present");
            return result;
        }

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
                TString explain = TStringBuilder() << "parent path fail checks"
                                                   << ", path: " << parentPath.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        NSchemeShard::TPath tablePath = parentPath.Child(lockSchema.GetName());
        {
            NSchemeShard::TPath::TChecker checks = tablePath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .NotUnderOperation()
                .IsTable()
                .IsCommonSensePath();

            if (!checks) {
                TString explain = TStringBuilder() << "table path fail checks"
                                                   << ", path: " << tablePath.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                if (tablePath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(tablePath.Base()->CreateTxId));
                    result->SetPathId(tablePath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        auto buildIndexSchema = Transaction.GetInitiateIndexBuild();

        if (buildIndexSchema.GetTable() != tablePath.PathString()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "table path in build index mismatch with locking path");
            return result;
        }

        auto indexSchema = buildIndexSchema.GetIndex();

        NSchemeShard::TPath indexPath = tablePath.Child(indexSchema.GetName());
        {
            NSchemeShard::TPath::TChecker checks = indexPath.Check();
            checks
                .IsAtLocalSchemeShard();

            if (indexPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist(TPathElement::EPathType::EPathTypeTableIndex, false);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            checks
                .IsValidLeafName()
                .PathsLimit(2) // we are creating 2 pathes at next stages: index and table
                .DirChildrenLimit()
                .ShardsLimit(1) // we are creating 1 shard at next stages for index table
                ;

            if (!checks) {
                TString explain = TStringBuilder() << "index path fail checks"
                                                   << ", path: " << indexPath.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                if (indexPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(indexPath.Base()->CreateTxId));
                    result->SetPathId(indexPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TPathElement::TPtr pathEl = tablePath.Base();
        TPathId pathId = pathEl->PathId;
        result->SetPathId(pathId.LocalPathId);

        TTableInfo::TPtr tableInfo = context.SS->Tables.at(tablePath.Base()->PathId);

        if (tableInfo->IsTTLEnabled()) {
            if (indexSchema.GetType() == NKikimrSchemeOp::EIndexTypeGlobalAsync && !AppData()->FeatureFlags.GetEnableTtlOnAsyncIndexedTables()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "Async indexes are not currently supported on tables with TTL");
                return result;
            }
        }

        const ui64 aliveIndices = context.SS->GetAliveChildren(tablePath.Base(), NKikimrSchemeOp::EPathTypeTableIndex);
        if (aliveIndices + 1 > tablePath.DomainInfo()->GetSchemeLimits().MaxTableIndices) {
            auto msg = TStringBuilder() << "indexes count has reached maximum value in the table"
                                        << ", children limit for dir in domain: " << tablePath.DomainInfo()->GetSchemeLimits().MaxTableIndices
                                        << ", intention to create new children: " << aliveIndices + 1;
            result->SetError(NKikimrScheme::StatusPreconditionFailed, msg);
            return result;
        }

        {
            NTableIndex::TIndexColumns indexKeys = NTableIndex::ExtractInfo(indexSchema);
            if (indexKeys.KeyColumns.empty()) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "no key colums in index creation config");
                return result;
            }

            NTableIndex::TTableColumns baseTableColumns = NTableIndex::ExtractInfo(tableInfo);

            TString explainErr;
            if (!NTableIndex::IsCompatibleIndex(baseTableColumns, indexKeys, explainErr)) {
                TString msg = TStringBuilder() << "IsCompatibleIndex fail with explain: " << explainErr;
                result->SetError(NKikimrScheme::StatusInvalidParameter, msg);
                return result;
            }

            NTableIndex::TTableColumns impTableColumns = NTableIndex::CalcTableImplDescription(baseTableColumns, indexKeys);

            if (!NTableIndex::IsCompatibleKeyTypes(NTableIndex::ExtractTypes(tableInfo), impTableColumns, false, explainErr)) {
                TString msg = TStringBuilder() << "IsCompatibleKeyTypes fail with explain: " << explainErr;
                result->SetError(NKikimrScheme::StatusInvalidParameter, msg);
                return result;
            }

            if (impTableColumns.Keys.size() > tablePath.DomainInfo()->GetSchemeLimits().MaxTableKeyColumns) {
                TString msg = TStringBuilder()
                    << "Too many key indexed, index table reaches the limit of the maximum keys colums count"
                    << ": indexing colums: " << indexKeys.KeyColumns.size()
                    << ": requested keys colums for index table: " << impTableColumns.Keys.size()
                    << ". Limit: " << tablePath.DomainInfo()->GetSchemeLimits().MaxTableKeyColumns;
                result->SetError(NKikimrScheme::StatusSchemeError, msg);
                return result;
            }
        }

        if (tablePath.LockedBy() == OperationId.GetTxId()) {
            TString explain = TStringBuilder()
                << "dst path fail checks"
                << ", path already locked by this operation"
                << ", path: " << tablePath.PathString();
            result->SetError(TEvSchemeShard::EStatus::StatusAlreadyExists, explain);
            return result;
        }

        TString errStr;
        if (!context.SS->CheckLocks(pathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }
        if (!context.SS->CheckInFlightLimit(TTxState::TxCreateLockForIndexBuild, errStr)) {
            result->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());

        pathEl->LastTxId = OperationId.GetTxId();
        pathEl->PathState = NKikimrSchemeOp::EPathState::EPathStateAlter;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateLockForIndexBuild, pathId);
        txState.State = TTxState::Done;

        context.SS->PersistTxState(db, OperationId);

        if (pathEl->IsTable()) {
            // wait all the splits
            TTableInfo::TPtr table = context.SS->Tables.at(pathId);
            for (auto splitTx: table->GetSplitOpsInFlight()) {
                context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
            }
        }

        context.SS->LockedPaths[pathId] = OperationId.GetTxId();
        context.SS->PersistLongLock(db, OperationId.GetTxId(), pathId);
        context.SS->TabletCounters->Simple()[COUNTER_LOCKS_COUNT].Add(1);

        context.OnComplete.ActivateTx(OperationId);

        State = NextState();
        SetState(SelectStateFunc(State));
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TCreateLock");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateLock AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateLockForIndexBuild(TOperationId id, const TTxTransaction& tx) {
    return new TCreateLockForIndexBuild(id, tx);
}

ISubOperationBase::TPtr CreateLockForIndexBuild(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TCreateLockForIndexBuild(id, state);
}

}
}
