#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/sequenceshard/public/events.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/base/subdomain.h>


namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TCopyTableBarrier: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCopySequence TCopyTableBarrier"
                << " operationId: " << OperationId;
    }

public:
    TCopyTableBarrier(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvCompleteBarrier"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet" << ssId);

        NIceDb::TNiceDb db(context.GetDB());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
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

class TConfigureParts : public TSubOperationState {
private:
    TOperationId OperationId;
    NKikimrTxSequenceShard::TEvGetSequenceResult GetSequenceResult;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCopySequence TConfigureParts"
                << " operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType,
        });
    }

    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TConfigureParts HandleReply TEvRestoreSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        switch (status) {
            case NKikimrTxSequenceShard::TEvRestoreSequenceResult::SUCCESS: break;
            default:
                // Treat all other replies as unexpected and spurious
                LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TConfigureParts HandleReply ignoring unexpected TEvRestoreSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCopySequence);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TCopySequence TConfigureParts HandleReply ignoring duplicate TEvRestoreSequenceResult"
                << " shardId# " << tabletId
                << " status# " << status
                << " operationId# " << OperationId
                << " at tablet " << ssId);
            return false;
        }

        if (!txState->ShardsInProgress.empty()) {
            return false;
        }

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        context.OnComplete.ActivateTx(OperationId);
        return true;

        return false;
    }

    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvGetSequenceResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TConfigureParts HandleReply TEvGetSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        switch (status) {
            case NKikimrTxSequenceShard::TEvGetSequenceResult::SUCCESS: break;
            default:
                // Treat all other replies as unexpected and spurious
                LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TConfigureParts HandleReply ignoring unexpected TEvGetSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCopySequence);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TCopySequence TConfigureParts HandleReply ignoring duplicate TEvGetSequenceResult"
                << " shardId# " << tabletId
                << " status# " << status
                << " operationId# " << OperationId
                << " at tablet " << ssId);
            return false;
        }

        if (!txState->ShardsInProgress.empty()) {
            return false;
        }

        GetSequenceResult = ev->Get()->Record;

        for (auto shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto currentTabletId = context.SS->ShardInfos.at(shardIdx).TabletID;
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::SequenceShard);

            if (currentTabletId == InvalidTabletId) {
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "TCopySequence TConfigureParts ProgressState"
                            << " shard " << shardIdx << " is not created yet, waiting"
                            << " operationId# " << OperationId
                            << " at tablet " << ssId);
                context.OnComplete.WaitShardCreated(shardIdx, OperationId);
                txState->ShardsInProgress.insert(shardIdx);
                return false;
            }

            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvRestoreSequence>(
                txState->TargetPathId, GetSequenceResult);
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TCopySequence TConfigureParts ProgressState"
                        << " sending TEvRestoreSequence to tablet " << currentTabletId
                        << " operationId# " << OperationId
                        << " at tablet " << ssId);

            context.OnComplete.BindMsgToPipe(OperationId, currentTabletId, txState->TargetPathId, event.Release());

            // Wait for results from this shard
            txState->ShardsInProgress.insert(shardIdx);
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TConfigureParts ProgressState"
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCopySequence);
        Y_ABORT_UNLESS(!txState->Shards.empty());
        Y_ABORT_UNLESS(txState->SourcePathId != InvalidPathId);

        TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(txState->SourcePathId);
        Y_ABORT_UNLESS(sequenceInfo);
        Y_ABORT_UNLESS(!sequenceInfo->AlterData);

        for (const auto& shardIdxProto : sequenceInfo->Sharding.GetSequenceShards()) {
            TShardIdx shardIdx = FromProto(shardIdxProto);
            auto tabletId = context.SS->ShardInfos.at(shardIdx).TabletID;

            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvGetSequence>(txState->SourcePathId);
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TCopySequence TConfigureParts ProgressState"
                        << " sending TEvGetSequence to tablet " << tabletId
                        << " operationId# " << OperationId
                        << " at tablet " << ssId);

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, txState->SourcePathId, event.Release());

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
            << "TCopySequence TPropose"
            << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType,
            NSequenceShard::TEvSequenceShard::TEvFreezeSequenceResult::EventType,
            NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult::EventType,
        });
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCopySequence);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        Y_VERIFY_S(context.SS->Sequences.contains(pathId), "Sequence not found. PathId: " << pathId);
        TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(pathId);
        Y_ABORT_UNLESS(sequenceInfo);
        TSequenceInfo::TPtr alterData = sequenceInfo->AlterData;
        Y_ABORT_UNLESS(alterData);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        context.SS->Sequences[pathId] = alterData;
        context.SS->PersistSequenceAlterRemove(db, pathId);
        context.SS->PersistSequence(db, pathId, *alterData);

        auto parentDir = context.SS->PathsById.at(path->ParentPathId);
        if (parentDir->IsLikeDirectory()) {
            ++parentDir->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentDir);
        }
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCopySequence);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TCopySequence: public TSubOperation {

    static TTxState::ETxState NextState() {
        return TTxState::CopyTableBarrier;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::CopyTableBarrier:
            return TTxState::CreateParts;
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        using TPtr = TSubOperationState::TPtr;

        switch (state) {
        case TTxState::CopyTableBarrier:
            return TPtr(new TCopyTableBarrier(OperationId));
        case TTxState::CreateParts:
            return TPtr(new TCreateParts(OperationId));
        case TTxState::ConfigureParts:
            return TPtr(new TConfigureParts(OperationId));
        case TTxState::Propose:
            return TPtr(new TPropose(OperationId));
        case TTxState::Done:
            return TPtr(new TDone(OperationId));
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        auto& descr = Transaction.GetSequence();
        const TString& name = descr.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCopySequence Propose"
                        << ", path: " << parentPathStr << "/" << name
                        << ", opId: " << OperationId
                        << ", at schemeshard: " << ssId);

        TEvSchemeShard::EStatus status = NKikimrScheme::StatusAccepted;
        auto result = MakeHolder<TProposeResponse>(status, ui64(OperationId.GetTxId()), ui64(ssId));

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath();

            if (checks) {
                if (parentPath->IsTable()) {
                    // allow immediately inside a normal table
                    if (parentPath.IsUnderOperation()) {
                        checks.IsUnderTheSameOperation(OperationId.GetTxId()); // allowed only as part of consistent operations
                    }
                } else {
                    // otherwise don't allow unexpected object types
                    checks.IsLikeDirectory();
                }
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPath srcPath = TPath::Resolve(descr.GetCopyFromSequence(), context.SS);
        {
            TPath::TChecker checks = srcPath.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsSequence()
                .NotUnderTheSameOperation(OperationId.GetTxId())
                .NotUnderOperation();

            if (checks) {
                if (!parentPath->IsTable()) {
                    // otherwise don't allow unexpected object types
                    checks.IsLikeDirectory();
                }
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        auto domainPathId = parentPath.GetPathIdForDomain();
        auto domainInfo = parentPath.DomainInfo();

        // TODO: maybe select from several shards
        ui64 shardsToCreate = 0;
        TShardIdx sequenceShard;
        if (domainInfo->GetSequenceShards().empty()) {
            ++shardsToCreate;
        } else {
            sequenceShard = *domainInfo->GetSequenceShards().begin();
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
                    .FailOnExist(TPathElement::EPathType::EPathTypeSequence, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks.IsValidLeafName();

                if (!parentPath->IsTable()) {
                    checks.DepthLimit();
                }

                checks
                    .PathsLimit()
                    .DirChildrenLimit()
                    .ShardsLimit(shardsToCreate)
                    .IsTheSameDomain(srcPath)
                    //.PathShardsLimit(shardsToCreate)
                    .IsValidACL(acl);
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath->CreateTxId));
                    result->SetPathId(dstPath->PathId.LocalPathId);
                }
                return result;
            }
        }

        TString errStr;

        if (!TSequenceInfo::ValidateCreate(descr, errStr)) {
            result->SetError(NKikimrScheme::StatusSchemeError, errStr);
            return result;
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        Y_ABORT_UNLESS(context.SS->Sequences.contains(srcPath.Base()->PathId));

        const ui32 profileId = 0;
        TChannelsBindings channelsBindings;
        if (shardsToCreate) {
            if (!context.SS->ResolveTabletChannels(profileId, dstPath.GetPathIdForDomain(), channelsBindings)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                            "Unable to construct channel binding for sequence shard with the storage pool");
                return result;
            }
        }

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath->PathId.LocalPathId);
        context.SS->TabletCounters->Simple()[COUNTER_SEQUENCE_COUNT].Add(1);

        srcPath.Base()->PathState = TPathElement::EPathState::EPathStateCopying;
        srcPath.Base()->LastTxId = OperationId.GetTxId();

        TPathId pathId = dstPath->PathId;
        dstPath->CreateTxId = OperationId.GetTxId();
        dstPath->LastTxId = OperationId.GetTxId();
        dstPath->PathState = TPathElement::EPathState::EPathStateCreate;
        dstPath->PathType = TPathElement::EPathType::EPathTypeSequence;

        if (parentPath->HasActiveChanges()) {
            TTxId parentTxId = parentPath->PlannedToCreate() ? parentPath->CreateTxId : parentPath->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        TTxState& txState =
            context.SS->CreateTx(OperationId, TTxState::TxCopySequence, pathId, srcPath.Base()->PathId);
        txState.State = TTxState::ConfigureParts;

        TSequenceInfo::TPtr sequenceInfo = new TSequenceInfo(0);
        TSequenceInfo::TPtr alterData = sequenceInfo->CreateNextVersion();
        alterData->Description = descr;

        if (shardsToCreate) {
            sequenceShard = context.SS->RegisterShardInfo(
                TShardInfo::SequenceShardInfo(OperationId.GetTxId(), domainPathId)
                    .WithBindedChannels(channelsBindings));
            context.SS->TabletCounters->Simple()[COUNTER_SEQUENCESHARD_COUNT].Add(1);
            txState.Shards.emplace_back(sequenceShard, ETabletType::SequenceShard, TTxState::CreateParts);
            txState.State = TTxState::CreateParts;
            context.SS->PathsById.at(domainPathId)->IncShardsInside();
            domainInfo->AddInternalShard(sequenceShard);
            domainInfo->AddSequenceShard(sequenceShard);
        } else {
            txState.Shards.emplace_back(sequenceShard, ETabletType::SequenceShard, TTxState::ConfigureParts);
            auto& shardInfo = context.SS->ShardInfos.at(sequenceShard);
            if (shardInfo.CurrentTxId != OperationId.GetTxId()) {
                context.OnComplete.Dependence(shardInfo.CurrentTxId, OperationId.GetTxId());
            }
        }

        {
            auto* p = alterData->Sharding.AddSequenceShards();
            p->SetOwnerId(sequenceShard.GetOwnerId());
            p->SetLocalId(ui64(sequenceShard.GetLocalId()));
        }

        NIceDb::TNiceDb db(context.GetDB());

        context.SS->ChangeTxState(db, OperationId, txState.State);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->PersistPath(db, dstPath->PathId);
        if (!acl.empty()) {
            dstPath->ApplyACL(acl);
            context.SS->PersistACL(db, dstPath.Base());
        }

        context.SS->Sequences[pathId] = sequenceInfo;
        context.SS->PersistSequence(db, pathId, *sequenceInfo);
        context.SS->PersistSequenceAlter(db, pathId, *alterData);
        context.SS->IncrementPathDbRefCount(pathId);

        context.SS->PersistTxState(db, OperationId);
        context.SS->PersistUpdateNextPathId(db);
        if (shardsToCreate) {
            context.SS->PersistUpdateNextShardIdx(db);
        }

        for (auto shard : txState.Shards) {
            if (shard.Operation == TTxState::CreateParts) {
                context.SS->PersistChannelsBinding(db, shard.Idx, context.SS->ShardInfos.at(shard.Idx).BindedChannels);
                context.SS->PersistShardMapping(db, shard.Idx, InvalidTabletId, domainPathId, OperationId.GetTxId(), shard.TabletType);
            }
        }

        ++parentPath->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath.Base());
        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath->PathId);

        context.SS->ClearDescribePathCaches(dstPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, dstPath->PathId);

        domainInfo->IncPathsInside();
        parentPath->IncAliveChildren();

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TCopySequence");
    }

    void AbortUnsafe(TTxId, TOperationContext&) override {
        Y_ABORT("no AbortUnsafe for TCopySequence");
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateCopySequence(TOperationId id, const TTxTransaction& tx)
{
    return MakeSubOperation<TCopySequence>(id, tx);
}

ISubOperation::TPtr CreateCopySequence(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TCopySequence>(id, state);
}

}
