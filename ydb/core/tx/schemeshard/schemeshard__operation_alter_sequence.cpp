#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TAlterSequence TConfigureParts"
                << " operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(NSequenceShard::TEvSequenceShard::TEvUpdateSequenceResult::TPtr& ev,
            TOperationContext& context) override {

        auto ssId = context.SS->SelfTabletId();
        auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        auto status = ev->Get()->Record.GetStatus();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TAlterSequence TConfigureParts HandleReply TEvUpdateSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        switch (status) {
            case NKikimrTxSequenceShard::TEvUpdateSequenceResult::SUCCESS:
                // Treat expected status as success
                break;

            default:
                // Treat all other replies as unexpected and spurious
                LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TAlterSequence TConfigureParts HandleReply ignoring unexpected TEvUpdateSequenceResult"
                    << " shardId# " << tabletId
                    << " status# " << status
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);
                return false;
        }

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSequence);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        if (!txState->ShardsInProgress.erase(shardIdx)) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TAlterSequence TConfigureParts HandleReply ignoring duplicate TEvUpdateSequenceResult"
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
            return true;
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSequence);
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
            Y_ABORT_UNLESS(tabletId != InvalidTabletId);

            auto event = MakeHolder<NSequenceShard::TEvSequenceShard::TEvUpdateSequence>(txState->TargetPathId);
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

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TAlterSequence TConfigureParts ProgressState"
                        << " sending TEvUpdateSequence to tablet " << tabletId
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
                << "TAlterSequence TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {NSequenceShard::TEvSequenceShard::TEvUpdateSequenceResult::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        auto step = TStepId(ev->Get()->StepId);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        if (!txState) {
            return false;
        }
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSequence);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        Y_VERIFY_S(context.SS->Sequences.contains(pathId), "Sequence not found. PathId: " << pathId);
        TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(pathId);
        Y_ABORT_UNLESS(sequenceInfo);
        TSequenceInfo::TPtr alterData = sequenceInfo->AlterData;
        Y_ABORT_UNLESS(alterData);

        NIceDb::TNiceDb db(context.GetDB());

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSequence);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

std::optional<NKikimrSchemeOp::TSequenceDescription> GetAlterSequenceDescription(
        const NKikimrSchemeOp::TSequenceDescription& sequence, const NKikimrSchemeOp::TSequenceDescription& alter,
        const NScheme::TTypeRegistry& typeRegistry, bool pgTypesEnabled,
        TString& errStr) {

    NKikimrSchemeOp::TSequenceDescription result = sequence;

    i64 minValue = result.GetMinValue();
    i64 maxValue = result.GetMaxValue();

    auto dataType = result.GetDataType();
    if (alter.HasDataType()) {
        dataType = alter.GetDataType();
    }

    auto validationResult = ValidateSequenceType(sequence.GetName(), dataType, typeRegistry, pgTypesEnabled, errStr);
    if (!validationResult) {
        return std::nullopt;
    }

    auto [dataTypeMinValue, dataTypeMaxValue] = *validationResult;

    if (maxValue != Max<i16>() && maxValue != Max<i32>() && maxValue != Max<i64>()) {
        if (maxValue > dataTypeMaxValue) {
            errStr = Sprintf("MAXVALUE (%ld) is out of range for sequence data type %s", maxValue, dataType.c_str());
            return std::nullopt;
        }
    } else {
        maxValue = dataTypeMaxValue;
    }

    if (minValue != Min<i16>() && minValue != Min<i32>() && minValue != Min<i64>()) {
        if (minValue < dataTypeMinValue) {
            errStr = Sprintf("MINVALUE (%ld) is out of range for sequence data type %s", minValue, dataType.c_str());
            return std::nullopt;
        }
    } else {
        minValue = dataTypeMinValue;
    }

    if (alter.HasMinValue()) {
        minValue = alter.GetMinValue();
    }
    if (alter.HasMaxValue()) {
        maxValue = alter.GetMaxValue();
    }

    if (maxValue > dataTypeMaxValue) {
        errStr = Sprintf("MAXVALUE (%ld) is out of range for sequence", maxValue);
        return std::nullopt;
    }

    if (minValue < dataTypeMinValue) {
        errStr = Sprintf("MINVALUE (%ld) is out of range for sequence", minValue);
        return std::nullopt;
    }

    if (minValue >= maxValue) {
        errStr = Sprintf("MINVALUE (%ld) must be less than MAXVALUE (%ld)", minValue, maxValue);
        return std::nullopt;
    }

    i64 startValue = result.GetStartValue();
    if (alter.HasStartValue()) {
        startValue = alter.GetStartValue();
    }

    if (startValue > maxValue) {
        errStr = Sprintf("START value (%ld) cannot be greater than MAXVALUE (%ld)", startValue, maxValue);
        return std::nullopt;
    }
    if (startValue < minValue) {
        errStr = Sprintf("START value (%ld) cannot be less than MINVALUE (%ld)",  startValue, minValue);
        return std::nullopt;
    }

    i64 increment = result.GetIncrement();
    if (alter.HasIncrement()) {
        increment = alter.GetIncrement();
    }
    ui64 cache = result.GetCache();
    if (alter.HasCache()) {
        cache = alter.GetCache();
    }
    bool cycle = result.GetCycle();
    if (alter.HasCycle()) {
        cycle = alter.GetCycle();
    }

    result.SetMinValue(minValue);
    result.SetMaxValue(maxValue);
    result.SetIncrement(increment);
    result.SetCycle(cycle);
    result.SetCache(cache);
    result.SetStartValue(startValue);
    result.SetDataType(dataType);

    return result;
}

class TAlterSequence: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
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
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::ProposedWaitParts:
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
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

        const auto& sequenceAlter = Transaction.GetSequence();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = sequenceAlter.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterSequence Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", operationId: " << OperationId
                         << ", transaction: " << Transaction.ShortDebugString()
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

        NSchemeShard::TPath dstPath = parentPath.Child(name);

        {
            TPath::TChecker checks = dstPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsSequence()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        Y_ABORT_UNLESS(context.SS->Sequences.contains(dstPath->PathId));
        TSequenceInfo::TPtr sequenceInfo = context.SS->Sequences.at(dstPath->PathId);
        Y_ABORT_UNLESS(!sequenceInfo->AlterData);

        if (sequenceAlter.HasSetVal()) {
            errStr = "Set value by alter sequence is not supported";
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;
        auto description = GetAlterSequenceDescription(
                sequenceInfo->Description, sequenceAlter, *typeRegistry, context.SS->EnableTablePgTypes, errStr);
        if (!description) {
            status = NKikimrScheme::StatusInvalidParameter;
            result->SetError(status, errStr);
            return result;
        }

        TSequenceInfo::TPtr alterData = sequenceInfo->CreateNextVersion();
        Y_ABORT_UNLESS(alterData);
        alterData->Description = *description;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterSequence, dstPath->PathId);
        txState.State = TTxState::ConfigureParts;

        const auto& protoSequenceShard = *sequenceInfo->Sharding.GetSequenceShards().rbegin();
        TShardIdx sequenceShard = FromProto(protoSequenceShard);

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

        context.OnComplete.ActivateTx(OperationId);

        NIceDb::TNiceDb db(context.GetDB());
        auto sequencePath = dstPath.Base();

        sequencePath->PathState = TPathElement::EPathState::EPathStateAlter;
        sequencePath->LastTxId = OperationId.GetTxId();
        context.SS->PersistLastTxId(db, sequencePath);
        context.SS->PersistTxState(db, OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterSequence");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterSequence AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterSequence(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterSequence>(id, tx);
}

ISubOperation::TPtr CreateAlterSequence(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterSequence>(id, state);
}

}
