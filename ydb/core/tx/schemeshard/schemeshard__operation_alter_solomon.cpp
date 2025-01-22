#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/persqueue/config/config.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TAlterSolomon TConfigureParts"
            << ", operationId: " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
                       {TEvHive::TEvCreateTabletReply::EventType, TEvHive::TEvAdoptTabletReply::EventType});
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSolomonVolume);

        auto solomon = context.SS->SolomonVolumes[txState->TargetPathId];
        Y_VERIFY_S(solomon, "solomon volume is null. PathId: " << txState->TargetPathId);
        Y_VERIFY_S(solomon->AlterData, "solomon volume alter data is null. PathId: " << txState->TargetPathId);

        for (const auto& shard: txState->Shards) {
            auto solomonPartition = solomon->AlterData->Partitions[shard.Idx];
            Y_VERIFY_S(solomonPartition, "rtmr partitions is null shard idx: " << shard.Idx << " Path: " << txState->TargetPathId);

            auto tabletId = context.SS->ShardInfos[shard.Idx].TabletID;

            if (solomonPartition->TabletId != InvalidTabletId && tabletId != solomonPartition->TabletId) {
                Y_FAIL_S("Solomon partition tablet id mismatch"
                    << ": expected: " << solomonPartition->TabletId
                    << ", got: " << tabletId);
            }

            solomonPartition->TabletId = tabletId;
        }

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        return true;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TAlterSolomon TPropose"
            << ", operationId: " << OperationId;
    }
public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
                       {TEvHive::TEvCreateTabletReply::EventType, TEvHive::TEvAdoptTabletReply::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if(!txState) {
            return false;
        }

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        auto solomon = context.SS->SolomonVolumes[txState->TargetPathId];
        Y_VERIFY_S(solomon, "solomon volume is null. PathId: " << txState->TargetPathId);
        Y_VERIFY_S(solomon->AlterData, "solomon volume alter data is null. PathId: " << txState->TargetPathId);

        context.SS->TabletCounters->Simple()[COUNTER_SOLOMON_PARTITIONS_COUNT].Sub(solomon->Partitions.size());
        context.SS->TabletCounters->Simple()[COUNTER_SOLOMON_PARTITIONS_COUNT].Add(solomon->AlterData->Partitions.size());

        context.SS->PersistSolomonVolume(db, txState->TargetPathId, solomon->AlterData);
        context.SS->SolomonVolumes[txState->TargetPathId] = solomon->AlterData;

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterSolomonVolume);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TAlterSolomon: public TSubOperation {
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

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& alter = Transaction.GetAlterSolomonVolume();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = alter.GetName();
        const ui32 channelProfileId = alter.GetChannelProfileId();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterSolomon Propose"
                         << ", path: "<< parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", channelProfileId: " << channelProfileId
                         << ", at schemeshard: " << ssId);

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));

        TString errStr;

        TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsSolomon()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (checks) {
                TSolomonVolumeInfo::TPtr solomon = context.SS->SolomonVolumes.at(path.Base()->PathId);
                if (alter.GetPartitionCount() > solomon->Partitions.size()) {
                    const ui64 shardsToCreate = alter.GetPartitionCount() - solomon->Partitions.size();

                    checks
                        .ShardsLimit(shardsToCreate)
                        .PathShardsLimit(shardsToCreate);
                }

            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TSolomonVolumeInfo::TPtr solomon = context.SS->SolomonVolumes.at(path.Base()->PathId);

        if (!alter.HasPartitionCount() && !alter.GetUpdateChannelsBinding()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Empty alter");
            return result;
        }

        if (alter.GetUpdateChannelsBinding() && !AppData()->FeatureFlags.GetAllowUpdateChannelsBindingOfSolomonPartitions()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, "Updating of channels binding is not available");
            return result;
        }

        if (alter.HasPartitionCount()) {
            if (alter.GetPartitionCount() < solomon->Partitions.size()) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "solomon volume has more shards than requested");
                return result;
            }

            if (alter.GetPartitionCount() == solomon->Partitions.size()) {
                result->SetError(NKikimrScheme::StatusSuccess, "solomon volume has already the same shards as requested");
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!alter.HasChannelProfileId()) {
            result->SetError(TEvSchemeShard::EStatus::StatusInvalidParameter, "set channel profile id, please");
            return result;
        }

        TChannelsBindings channelsBinding;
        bool isResolved = false;
        if (alter.HasStorageConfig()) {
            isResolved = context.SS->ResolveSolomonChannels(alter.GetStorageConfig(), path.GetPathIdForDomain(), channelsBinding);
        } else {
            isResolved = context.SS->ResolveSolomonChannels(channelProfileId, path.GetPathIdForDomain(), channelsBinding);
        }
        if (!isResolved) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Unable to construct channel binding with the storage pool");
            return result;
        }

        result->SetPathId(path.Base()->PathId.LocalPathId);

        TSolomonVolumeInfo::TPtr alterSolomon = solomon->CreateAlter();

        NIceDb::TNiceDb db(context.GetDB());

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterSolomonVolume,  path.Base()->PathId);

        TShardInfo solomonPartitionInfo = TShardInfo::SolomonPartitionInfo(OperationId.GetTxId(), path.Base()->PathId);
        solomonPartitionInfo.BindedChannels = channelsBinding;

        path.Base()->LastTxId = OperationId.GetTxId();
        path.Base()->PathState = TPathElement::EPathState::EPathStateAlter;

        context.SS->PersistLastTxId(db, path.Base());

        if (alter.GetUpdateChannelsBinding()) {
            txState.Shards.reserve(alter.HasPartitionCount() ? alter.GetPartitionCount() : solomon->Partitions.size());
        } else {
            Y_ABORT_UNLESS(alter.HasPartitionCount());
            txState.Shards.reserve(alter.GetPartitionCount() - solomon->Partitions.size());
        }

        if (alter.GetUpdateChannelsBinding()) {
            for (const auto& [shardIdx, partitionInfo] : solomon->Partitions) {
                txState.Shards.emplace_back(shardIdx, TTabletTypes::KeyValue, TTxState::CreateParts);

                auto& shardInfo = context.SS->ShardInfos.at(shardIdx);
                shardInfo.CurrentTxId = OperationId.GetTxId();
                shardInfo.BindedChannels = channelsBinding;

                context.SS->PersistShardMapping(db, shardIdx, partitionInfo->TabletId, path.Base()->PathId, OperationId.GetTxId(), solomonPartitionInfo.TabletType);
                context.SS->PersistChannelsBinding(db, shardIdx, channelsBinding);
            }
        }

        if (alter.HasPartitionCount()) {
            const ui64 shardsToCreate = alter.GetPartitionCount() - solomon->Partitions.size();

            for (ui64 i = 0; i < shardsToCreate; ++i) {
                const auto shardIdx = context.SS->RegisterShardInfo(solomonPartitionInfo);
                context.SS->PersistShardMapping(db, shardIdx, InvalidTabletId, path.Base()->PathId, OperationId.GetTxId(), solomonPartitionInfo.TabletType);
                context.SS->PersistChannelsBinding(db, shardIdx, channelsBinding);

                alterSolomon->Partitions[shardIdx] = new TSolomonPartitionInfo(solomon->Partitions.size() + i);
                txState.Shards.emplace_back(shardIdx, TTabletTypes::KeyValue, TTxState::CreateParts);
            }
            context.SS->PersistUpdateNextShardIdx(db);

            path.Base()->IncShardsInside(shardsToCreate);
        }

        solomon->AlterData = alterSolomon;
        context.SS->PersistAlterSolomonVolume(db, path.Base()->PathId, solomon);

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->PersistTxState(db, OperationId);

        path.DomainInfo()->AddInternalShards(txState, context.SS);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterSolomon");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterSolomon AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterSolomon(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterSolomon>(id, tx);
}

ISubOperation::TPtr CreateAlterSolomon(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterSolomon>(id, state);
}

}
