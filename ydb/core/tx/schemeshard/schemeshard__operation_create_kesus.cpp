#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/persqueue/config/config.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

bool ValidateConfig(const Ydb::Coordination::Config& config, TEvSchemeShard::EStatus& status, TString& errStr) {
    if (!config.path().empty()) {
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = "Setting path is not allowed";
        return false;
    }
    return true;
}

TTxState& PrepareChanges(TOperationId operationId, TPathElement::TPtr parentDir,
                    TPathElement::TPtr item, TKesusInfo::TPtr kesus, const TString& acl,
                    const TChannelsBindings& tabletChannels,
                    TOperationContext& context)
{
    NIceDb::TNiceDb db(context.GetDB());

    item->CreateTxId = operationId.GetTxId();
    item->LastTxId = operationId.GetTxId();
    item->PathState = TPathElement::EPathState::EPathStateCreate;
    item->PathType = TPathElement::EPathType::EPathTypeKesus;
    TPathId pathId = item->PathId;

    TTxState& txState = context.SS->CreateTx(operationId, TTxState::TxCreateKesus, pathId);

    auto shardIdx = context.SS->RegisterShardInfo(
        TShardInfo::KesusInfo(operationId.GetTxId(), pathId)
            .WithBindedChannels(tabletChannels));
    context.SS->TabletCounters->Simple()[COUNTER_KESUS_SHARD_COUNT].Add(1);
    txState.Shards.emplace_back(shardIdx, ETabletType::Kesus, TTxState::CreateParts);
    kesus->KesusShardIdx = shardIdx;

    if (parentDir->HasActiveChanges()) {
        TTxId parentTxId =  parentDir->PlannedToCreate() ? parentDir->CreateTxId : parentDir->LastTxId;
        context.OnComplete.Dependence(parentTxId, operationId.GetTxId());
    }

    context.SS->ChangeTxState(db, operationId, TTxState::CreateParts);
    context.OnComplete.ActivateTx(operationId);

    if (!acl.empty()) {
        item->ApplyACL(acl);
    }
    context.SS->PersistPath(db, item->PathId);
    context.SS->KesusInfos[pathId] = kesus;
    context.SS->PersistKesusInfo(db, pathId, kesus);
    context.SS->IncrementPathDbRefCount(pathId);

    context.SS->PersistTxState(db, operationId);
    context.SS->PersistUpdateNextPathId(db);
    context.SS->PersistUpdateNextShardIdx(db);
    for (auto shard : txState.Shards) {
        Y_ABORT_UNLESS(shard.Operation == TTxState::CreateParts);
        context.SS->PersistChannelsBinding(db, shard.Idx, context.SS->ShardInfos[shard.Idx].BindedChannels);
        context.SS->PersistShardMapping(db, shard.Idx, InvalidTabletId, pathId, operationId.GetTxId(), shard.TabletType);
    }

    return txState;
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateKesus TConfigureParts"
                << " operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(NKesus::TEvKesus::TEvSetConfigResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCreateKesus TConfigureParts HandleReply TEvSetConfigResult"
                    << " operationId#" << OperationId
                    << " at tablet" << ssId);

        auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
        auto status = ev->Get()->Record.GetError().GetStatus();

        // SetConfig may fail if schemeshard tries to downgrade configuration
        // That likely means this is a very outdated version
        Y_VERIFY_S(status == Ydb::StatusIds::SUCCESS,
                   "Unexpected error in SetConfigResul:"
                       << " status " << Ydb::StatusIds::StatusCode_Name(status) << " Tx " << OperationId << " tablet " << tabletId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateKesus);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        auto idx = context.SS->MustGetShardIdx(tabletId);
        txState->ShardsInProgress.erase(idx);

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCreateKesus TConfigureParts ProgressState"
                    << " operationId#" << OperationId
                    << " at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateKesus);
        Y_ABORT_UNLESS(!txState->Shards.empty());

        txState->ClearShardsInProgress();

        TKesusInfo::TPtr kesus = context.SS->KesusInfos[txState->TargetPathId];
        Y_VERIFY_S(kesus, "kesus is null. PathId: " << txState->TargetPathId);


        auto kesusPath = TPath::Init(txState->TargetPathId, context.SS);
        Y_ABORT_UNLESS(kesusPath.IsResolved());

        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        for (auto shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos[shardIdx].TabletID;
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::Kesus);

            kesus->KesusShardIdx = shardIdx;
            kesus->KesusTabletId = tabletId;

            auto event = MakeHolder<NKesus::TEvKesus::TEvSetConfig>(ui64(OperationId.GetTxId()), kesus->Config, kesus->Version);
            event->Record.MutableConfig()->set_path(kesusPath.PathString()); // TODO: remove legacy field eventually
            event->Record.SetPath(kesusPath.PathString());

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, shardIdx, event.Release());

            // Wait for results from this shard
            txState->ShardsInProgress.insert(shardIdx);
        }

        return false;
    }
};


class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateKesus TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        auto step = TStepId(ev->Get()->StepId);
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if (!txState) {
            return false;
        }
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateKesus);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        Y_VERIFY_S(context.SS->KesusInfos.contains(pathId), "kesus has not found. PathId: " << pathId);
        TKesusInfo::TPtr kesus = context.SS->KesusInfos.at(pathId);
        Y_VERIFY_S(kesus, "kesus is null. PathId: " << pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        // KIKIMR-9036
        // usually we make creation over alter, alter as null -> first version
        // but now kesus already has been persisted as first version at propose stage
        // so we bump version to be sure that actual description pass over caches correctly
        ++kesus->Version;
        context.SS->PersistKesusVersion(db, pathId, kesus);

        auto parentDir = context.SS->PathsById.at(path->ParentPathId);
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateKesus);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};


class TCreateKesus: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
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
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const auto& config = Transaction.GetKesus().GetConfig();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = Transaction.GetKesus().GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateKesus Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        TEvSchemeShard::EStatus status = NKikimrScheme::StatusAccepted;
        auto result = MakeHolder<TProposeResponse>(status, ui64(OperationId.GetTxId()), ui64(ssId));

        TString errStr;
        if (!ValidateConfig(config, status, errStr)) {
            result->SetError(status, errStr);
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
                .IsLikeDirectory()
                .FailOnRestrictedCreateInTempZone();

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
                    .FailOnExist(TPathElement::EPathType::EPathTypeKesus, acceptExisted);
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
                    .ShardsLimit()
                    .PathShardsLimit()
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

        const ui32 kesusProfileId = 0;
        TChannelsBindings kesusChannelsBindings;
        if (!context.SS->ResolveTabletChannels(kesusProfileId, dstPath.GetPathIdForDomain(), kesusChannelsBindings)) {
            result->SetError(NKikimrScheme::StatusInvalidParameter,
                        "Unable to construct channel binding for coordination node with the storage pool");
            return result;
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        context.SS->TabletCounters->Simple()[COUNTER_KESUS_COUNT].Add(1);
        TKesusInfo::TPtr kesus = new TKesusInfo();
        kesus->Config.CopyFrom(config);
        kesus->Version = 1;

        const TTxState& txState = PrepareChanges(OperationId, parentPath.Base(), dstPath.Base(), kesus, acl, kesusChannelsBindings, context);

        NIceDb::TNiceDb db(context.GetDB());
        ++parentPath.Base()->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath.Base());
        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath.Base()->PathId);

        context.SS->ClearDescribePathCaches(dstPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, dstPath.Base()->PathId);

        dstPath.DomainInfo()->IncPathsInside();
        dstPath.DomainInfo()->AddInternalShards(txState);

        dstPath.Base()->IncShardsInside();
        parentPath.Base()->IncAliveChildren();

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TCreateKesus");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateKesus AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateNewKesus(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateKesus>(id, tx);
}

ISubOperation::TPtr CreateNewKesus(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateKesus>(id, state);
}

}
