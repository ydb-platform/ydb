#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/filestore/core/filestore.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

////////////////////////////////////////////////////////////////////////////////

class TPropose: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDropFileStore::TPropose"
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

        YDB_LOG_CTX_INFO(context.Ctx, "HandleReply TEvOperationPlan",
            {"#_DebugHint()", DebugHint()},
            {"step", step},
            {"at_schemeshard", ssId});

        auto* txState = context.SS->FindTx(OperationId);
        if (!txState) {
            return false;
        }

        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropFileStore);
        TPathId pathId = txState->TargetPathId;
        auto path = context.SS->PathsById.at(pathId);
        auto parentDir = context.SS->PathsById.at(path->ParentPathId);

        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(!path->Dropped());
        path->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);
        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside(context.SS);
        DecAliveChildrenDirect(OperationId, parentDir, context); // for correct discard of ChildrenExist prop

        // KIKIMR-13173
        // Repeat it here for a while, delete it from TDeleteParts after
        // Initiate asynchronous deletion of all shards
        for (auto shard : txState->Shards) {
            context.OnComplete.DeleteShard(shard.Idx);
        }

        TFileStoreInfo::TPtr fs = context.SS->FileStoreInfos.at(pathId);

        const auto oldFileStoreSpace = fs->GetFileStoreSpace();
        auto domainDir = context.SS->PathsById.at(context.SS->ResolvePathIdForDomain(path));
        domainDir->ChangeFileStoreSpaceCommit({ }, oldFileStoreSpace);

        if (!AppData()->DisableSchemeShardCleanupOnDropForTest) {
            context.SS->PersistRemoveFileStoreInfo(db, pathId);
        }

        context.SS->TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(path->UserAttrs->Size());
        context.SS->PersistUserAttributes(db, path->PathId, path->UserAttrs, nullptr);

        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.SS->ClearDescribePathCaches(path);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        }

        context.OnComplete.DoneOperation(OperationId);

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        const auto ssId = context.SS->SelfTabletId();

        YDB_LOG_CTX_INFO(context.Ctx, "ProgressState",
            {"#_DebugHint()", DebugHint()},
            {"at_schemeshard", ssId});

        auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropFileStore);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDropFileStore: public TSubOperation {
public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(
        const TString& owner,
        TOperationContext& context) override;

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TDropFileStore");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        AbortUnsafeDropOperation(OperationId, forceDropTxId, context);
    }

private:
    static TTxState::ETxState NextState() {
        return TTxState::DeleteParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::DeleteParts:
            return TTxState::Propose;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::DeleteParts:
            return MakeHolder<TDeleteParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        default:
            return nullptr;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

THolder<TProposeResponse> TDropFileStore::Propose(
    const TString& owner,
    TOperationContext& context)
{
    Y_UNUSED(owner);

    const auto ssId = context.SS->SelfTabletId();

    const auto& operation = Transaction.GetDrop();
    const TString& parentPathStr = Transaction.GetWorkingDir();
    const TString& name = operation.GetName();

    YDB_LOG_CTX_NOTICE(context.Ctx, "TDropFileStore Propose /",
        {"path", parentPathStr},
        {"name", name},
        {"pathId", operation.GetId()},
        {"opId", OperationId},
        {"at_schemeshard", ssId});

    auto result = MakeHolder<TProposeResponse>(
        NKikimrScheme::StatusAccepted,
        ui64(OperationId.GetTxId()),
        ui64(ssId));

    TPath path = operation.HasId()
        ? TPath::Init(context.SS->MakeLocalId(operation.GetId()), context.SS)
        : TPath::Resolve(parentPathStr, context.SS).Dive(name);

    {
        auto checks = path.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsFileStore()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            if (path.IsResolved() && path.Base()->IsFileStore() && path.Base()->PlannedToDrop()) {
                result->SetPathDropTxId(ui64(path.Base()->DropTxId));
                result->SetPathId(path.Base()->PathId.LocalPathId);
            }
            return result;
        }
    }

    TString errStr;
    if (!context.SS->CheckApplyIf(Transaction, errStr)) {
        result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
        return result;
    }

    TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropFileStore, path.Base()->PathId);
    // Dirty hack: operation step must not be zero because 0 is treated as "hasn't been in operation"
    txState.MinStep = TStepId(1);
    txState.State = TTxState::DeleteParts;

    NIceDb::TNiceDb db(context.GetDB());

    auto fs = context.SS->FileStoreInfos.at(path.Base()->PathId);
    Y_VERIFY_S(fs, "FileStore info is null. PathId: " << path.Base()->PathId);

    {
        auto shardIdx = fs->IndexShardIdx;
        Y_VERIFY_S(context.SS->ShardInfos.count(shardIdx), "invalid schemeshard idx " << shardIdx << " at " << path.Base()->PathId);

        txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos.at(shardIdx).TabletType, txState.State);

        context.SS->ShardInfos.at(shardIdx).CurrentTxId = OperationId.GetTxId();
        context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
    }

    // Trying to abort Alter/Create. Wait if can't.
    context.OnComplete.ActivateTx(OperationId);
    context.SS->PersistTxState(db, OperationId);

    path.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
    path.Base()->DropTxId = OperationId.GetTxId();
    path.Base()->LastTxId = OperationId.GetTxId();

    context.SS->TabletCounters->Simple()[COUNTER_FILESTORE_COUNT].Sub(1);

    auto parentDir = path.Parent();
    ++parentDir.Base()->DirAlterVersion;
    context.SS->PersistPathDirAlterVersion(db, parentDir.Base());
    context.SS->ClearDescribePathCaches(parentDir.Base());
    context.SS->ClearDescribePathCaches(path.Base());

    if (!context.SS->DisablePublicationsOfDropping) {
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir.Base()->PathId);
        context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);
    }

    SetState(NextState());
    return result;
}

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateDropFileStore(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropFileStore>(id, tx);
}

ISubOperation::TPtr CreateDropFileStore(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropFileStore>(id, state);
}

}
