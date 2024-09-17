#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/sequenceshard/public/events.h>
#include <ydb/core/mind/hive/hive.h>

namespace NKikimr::NSchemeShard {

namespace {

class TConfigureParts : public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateSequence TConfigureParts"
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
                    "TCreateSequence TConfigureParts HandleReply TEvCreateSequenceResult"
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
                    "TCreateSequence TConfigureParts HandleReply ignoring unexpected TEvCreateSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSequence);
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
                    "TCreateSequence TConfigureParts ProgressState"
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSequence);
        Y_ABORT_UNLESS(!txState->Shards.empty());

        txState->ClearShardsInProgress();

        TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(txState->TargetPathId);
        Y_ABORT_UNLESS(sequenceInfo);
        TSequenceInfo::TPtr alterData = sequenceInfo->AlterData;
        Y_ABORT_UNLESS(alterData);

        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        for (auto shard : txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos.at(shardIdx).TabletID;
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::SequenceShard);

            if (tabletId == InvalidTabletId) {
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "TCreateSequence TConfigureParts ProgressState"
                            << " shard " << shardIdx << " is not created yet, waiting"
                            << " operationId# " << OperationId
                            << " at tablet " << ssId);
                context.OnComplete.WaitShardCreated(shardIdx, OperationId);
                txState->ShardsInProgress.insert(shardIdx);
                return false;
            }

            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvCreateSequence>(txState->TargetPathId);
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetTxPartId(OperationId.GetSubTxId());
            if (alterData->Description.HasMinValue()) {
                event->Record.SetMinValue(alterData->Description.GetMinValue());
            }
            if (alterData->Description.HasMaxValue()) {
                event->Record.SetMaxValue(alterData->Description.GetMaxValue());
            }
            if (alterData->Description.HasStartValue()) {
                event->Record.SetStartValue(alterData->Description.GetStartValue());
            }
            if (alterData->Description.HasCache()) {
                event->Record.SetCache(alterData->Description.GetCache());
            }
            if (alterData->Description.HasIncrement()) {
                event->Record.SetIncrement(alterData->Description.GetIncrement());
            }
            if (alterData->Description.HasCycle()) {
                event->Record.SetCycle(alterData->Description.GetCycle());
            }
            if (alterData->Description.HasSetVal()) {
                event->Record.MutableSetVal()->SetNextValue(alterData->Description.GetSetVal().GetNextValue());
                event->Record.MutableSetVal()->SetNextUsed(alterData->Description.GetSetVal().GetNextUsed());
            }

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TCreateSequence TConfigureParts ProgressState"
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
                << "TCreateSequence TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType,
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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSequence);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSequence);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

// fill sequence description with default values
std::optional<NKikimrSchemeOp::TSequenceDescription> FillSequenceDescription(const NKikimrSchemeOp::TSequenceDescription& sequence,
        const NScheme::TTypeRegistry& typeRegistry, bool pgTypesEnabled,
        TString& errStr) {
    NKikimrSchemeOp::TSequenceDescription result = sequence;

    TString dataType;
    if (!sequence.HasDataType()) {
        dataType = NScheme::TypeName(NScheme::NTypeIds::Int64);
    } else {
        dataType = sequence.GetDataType();
    }

    auto validationResult = ValidateSequenceType(sequence.GetName(), dataType, typeRegistry, pgTypesEnabled, errStr);
    if (!validationResult) {
        return std::nullopt;
    }

    auto [dataTypeMinValue, dataTypeMaxValue] = *validationResult;

    i64 increment = 0;
    if (result.HasIncrement()) {
        increment = result.GetIncrement();
    }
    if (increment == 0) {
        increment = 1;
    }
    result.SetIncrement(increment);

    i64 minValue = 1;
    i64 maxValue = dataTypeMaxValue;
    if (increment < 0) {
        maxValue = -1;
        minValue = dataTypeMinValue;
    }

    if (result.HasMaxValue()) {
        maxValue = result.GetMaxValue();
    }

    if (result.HasMinValue()) {
        minValue = result.GetMinValue();
    }

    result.SetMaxValue(maxValue);
    result.SetMinValue(minValue);

    bool cycle = false;
    if (result.HasCycle()) {
        cycle = result.GetCycle();
    }

    result.SetCycle(cycle);

    i64 startValue = minValue;
    if (increment < 0) {
        startValue = maxValue;
    }
    if (result.HasStartValue()) {
        startValue = result.GetStartValue();
    }

    result.SetStartValue(startValue);

    ui64 cache = 1;
    if (result.HasCache()) {
        cache = result.GetCache();
    }

    result.SetCache(cache);
    result.SetDataType(dataType);

    return result;
}

class TCreateSequence : public TSubOperation {
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
                     "TCreateSequence Propose"
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

        TPathId pathId = dstPath->PathId;
        dstPath->CreateTxId = OperationId.GetTxId();
        dstPath->LastTxId = OperationId.GetTxId();
        dstPath->PathState = TPathElement::EPathState::EPathStateCreate;
        dstPath->PathType = TPathElement::EPathType::EPathTypeSequence;

        if (parentPath->HasActiveChanges()) {
            TTxId parentTxId = parentPath->PlannedToCreate() ? parentPath->CreateTxId : parentPath->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateSequence, pathId);
        txState.State = TTxState::ConfigureParts;

        TSequenceInfo::TPtr sequenceInfo = new TSequenceInfo(0);
        TSequenceInfo::TPtr alterData = sequenceInfo->CreateNextVersion();
        const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;
        auto description = FillSequenceDescription(
            descr, *typeRegistry, context.SS->EnableTablePgTypes, errStr);
        if (!description) {
            status = NKikimrScheme::StatusInvalidParameter;
            result->SetError(status, errStr);
            return result;
        }
        alterData->Description = *description;

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
        Y_ABORT("no AbortPropose for TCreateSequence");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateSequence AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

ISubOperation::TPtr CreateNewSequence(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateSequence>(id ,tx);
}

ISubOperation::TPtr CreateNewSequence(TOperationId id, TTxState::ETxState state) {
    return MakeSubOperation<TCreateSequence>(id, state);
}

}
