#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/sequenceshard/public/events.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/base/subdomain.h>


namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts : public TSubOperationState {
private:
    TOperationId OperationId;

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

    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TConfigureParts HandleReply TEvCreateSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        switch (status) {
            case NKikimrTxSequenceShard::TEvCreateSequenceResult::SUCCESS:
            case NKikimrTxSequenceShard::TEvCreateSequenceResult::SEQUENCE_ALREADY_EXISTS:
                // Treat expected status as success
                break;

            default:
                // Treat all other replies as unexpected and spurious
                LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TConfigureParts HandleReply ignoring unexpected TEvCreateSequenceResult"
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
                "TCreateSequence TConfigureParts HandleReply ignoring duplicate TEvCreateSequenceResult"
                << " shardId# " << tabletId
                << " status# " << status
                << " operationId# " << OperationId
                << " at tablet " << ssId);
            return false;
        }

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, txState->TargetPathId);

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
                    "TCopySequence TConfigureParts ProgressState"
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCopySequence);
        Y_ABORT_UNLESS(!txState->Shards.empty());

        txState->ClearShardsInProgress();

        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        for (auto shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos.at(shardIdx).TabletID;
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::SequenceShard);
            Y_ABORT_UNLESS(tabletId != InvalidTabletId);

            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvCreateSequence>(txState->TargetPathId);
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());
            event->Record.SetFrozen(true);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TCopySequence TConfigureParts ProgressState"
                        << " sending TEvCreateSequence to tablet " << tabletId
                        << " operationId# " << OperationId
                        << " at tablet " << ssId);

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, txState->TargetPathId, event.Release());

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
            << "TCopySequence TPropose"
            << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::EventType,
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

        context.SS->ChangeTxState(db, OperationId, TTxState::CopyTableBarrier);
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
        IgnoreMessages(DebugHint(), {
            TEvPrivate::TEvOperationPlan::EventType,
            NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::EventType,
        });
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

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedCopySequence);
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

class TProposedCopySequence : public TSubOperationState {
private:
    TOperationId OperationId;
    NKikimrTxSequenceShard::TEvGetSequenceResult GetSequenceResult;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCopySequence TProposedCopySequence"
                << " operationId#" << OperationId;
    }

    void UpdateSequenceDescription(NKikimrSchemeOp::TSequenceDescription& descr) {
        descr.SetStartValue(GetSequenceResult.GetStartValue());
        descr.SetMinValue(GetSequenceResult.GetMinValue());
        descr.SetMaxValue(GetSequenceResult.GetMaxValue());
        descr.SetCache(GetSequenceResult.GetCache());
        descr.SetIncrement(GetSequenceResult.GetIncrement());
        descr.SetCycle(GetSequenceResult.GetCycle());
        auto* setValMsg = descr.MutableSetVal();
        setValMsg->SetNextValue(GetSequenceResult.GetNextValue());
        setValMsg->SetNextUsed(GetSequenceResult.GetNextUsed());
    }

public:
    TProposedCopySequence(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvPrivate::TEvOperationPlan::EventType,
            TEvPrivate::TEvCompleteBarrier::EventType,
            NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::EventType,
        });
    }

    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TProposedCopySequence HandleReply TEvRestoreSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        switch (status) {
            case NKikimrTxSequenceShard::TEvRestoreSequenceResult::SUCCESS:
            case NKikimrTxSequenceShard::TEvRestoreSequenceResult::SEQUENCE_ALREADY_ACTIVE: break;
            default:
                // Treat all other replies as unexpected and spurious
                LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TProposedCopySequence HandleReply ignoring unexpected TEvRestoreSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, txState->TargetPathId);

        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCopySequence);
        Y_ABORT_UNLESS(txState->State == TTxState::ProposedCopySequence);

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TCopySequence TProposedCopySequence HandleReply ignoring duplicate TEvRestoreSequenceResult"
                << " shardId# " << tabletId
                << " status# " << status
                << " operationId# " << OperationId
                << " at tablet " << ssId);
            return false;
        }

        if (!txState->ShardsInProgress.empty()) {
            return false;
        }

        TPathId pathId = txState->TargetPathId;

        NIceDb::TNiceDb db(context.GetDB());

        auto sequenceInfo = context.SS->Sequences.at(pathId);
        UpdateSequenceDescription(sequenceInfo->Description);

        context.SS->PersistSequence(db, pathId, *sequenceInfo);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        context.OnComplete.ActivateTx(OperationId);
        return true;
    }

    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvGetSequenceResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TProposedCopySequence HandleReply TEvGetSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        switch (status) {
            case NKikimrTxSequenceShard::TEvGetSequenceResult::SUCCESS: break;
            default:
                // Treat all other replies as unexpected and spurious
                LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TCopySequence TProposedCopySequence HandleReply ignoring unexpected TEvGetSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCopySequence);
        Y_ABORT_UNLESS(txState->State == TTxState::ProposedCopySequence);

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, txState->SourcePathId);

        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TCopySequence TProposedCopySequence HandleReply ignoring duplicate TEvGetSequenceResult"
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

        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        for (auto shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto currentTabletId = context.SS->ShardInfos.at(shardIdx).TabletID;

            Y_ABORT_UNLESS(currentTabletId != InvalidTabletId);

            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvRestoreSequence>(
                txState->TargetPathId, GetSequenceResult);

            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TCopySequence TProposedCopySequence ProgressState"
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
                    "TCopySequence TProposedCopySequence ProgressState"
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCopySequence);
        Y_ABORT_UNLESS(!txState->Shards.empty());
        Y_ABORT_UNLESS(txState->SourcePathId != InvalidPathId);

        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        for (auto shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos.at(shardIdx).TabletID;

            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvGetSequence>(txState->SourcePathId);
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TCopySequence TProposedCopySequence ProgressState"
                        << " sending TEvGetSequence to tablet " << tabletId
                        << " operationId# " << OperationId
                        << " at tablet " << ssId);

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, txState->SourcePathId, event.Release());

            txState->ShardsInProgress.insert(shardIdx);
        }

        return false;
    }
};

class TCopySequence: public TSubOperation {

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
            return TTxState::CopyTableBarrier;
        case TTxState::CopyTableBarrier:
            return TTxState::ProposedCopySequence;
        case TTxState::ProposedCopySequence:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        using TPtr = TSubOperationState::TPtr;

        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TPtr(new TCreateParts(OperationId));
        case TTxState::ConfigureParts:
            return TPtr(new TConfigureParts(OperationId));
        case TTxState::Propose:
            return TPtr(new TPropose(OperationId));
        case TTxState::CopyTableBarrier:
            return TPtr(new TCopyTableBarrier(OperationId));
        case TTxState::ProposedCopySequence:
            return TPtr(new TProposedCopySequence(OperationId));
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
        auto& copySequence = Transaction.GetCopySequence();
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
                .IsCommonSensePath()
                .FailOnRestrictedCreateInTempZone(Transaction.GetAllowCreateInTempDir());

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

        TPath srcPath = TPath::Resolve(copySequence.GetCopyFrom(), context.SS);
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

        Y_ABORT_UNLESS(context.SS->Sequences.contains(srcPath.Base()->PathId));
        TSequenceInfo::TPtr srcSequence = context.SS->Sequences.at(srcPath.Base()->PathId);
        Y_ABORT_UNLESS(!srcSequence->Sharding.GetSequenceShards().empty());

        const auto& protoSequenceShard = *srcSequence->Sharding.GetSequenceShards().rbegin();
        TShardIdx sequenceShard = FromProto(protoSequenceShard);

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
                    .IsTheSameDomain(srcPath)
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
        txState.State = TTxState::Propose;

        TSequenceInfo::TPtr sequenceInfo = new TSequenceInfo(0);
        TSequenceInfo::TPtr alterData = sequenceInfo->CreateNextVersion();
        alterData->Description = descr;

        txState.Shards.emplace_back(sequenceShard, ETabletType::SequenceShard, TTxState::ConfigureParts);
        auto& shardInfo = context.SS->ShardInfos.at(sequenceShard);
        if (shardInfo.CurrentTxId != OperationId.GetTxId()) {
            context.OnComplete.Dependence(shardInfo.CurrentTxId, OperationId.GetTxId());
        }

        {
            auto* p = alterData->Sharding.AddSequenceShards();
            p->SetOwnerId(sequenceShard.GetOwnerId());
            p->SetLocalId(ui64(sequenceShard.GetLocalId()));
        }

        NIceDb::TNiceDb db(context.GetDB());

        context.SS->ChangeTxState(db, OperationId, txState.State);
        context.OnComplete.ActivateTx(OperationId);

        if (!acl.empty()) {
            dstPath->ApplyACL(acl);
        }
        context.SS->PersistPath(db, dstPath->PathId);

        context.SS->Sequences[pathId] = sequenceInfo;
        context.SS->PersistSequence(db, pathId, *sequenceInfo);
        context.SS->PersistSequenceAlter(db, pathId, *alterData);
        context.SS->IncrementPathDbRefCount(pathId);

        context.SS->PersistTxState(db, OperationId);
        context.SS->PersistUpdateNextPathId(db);

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
