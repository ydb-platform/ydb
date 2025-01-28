#pragma once
#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_billing_helpers.h"
#include "schemeshard_impl.h"
#include "schemeshard_types.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/metering/metering.h>

#include <util/generic/utility.h>

namespace NKikimr::NSchemeShard {

template <typename TKind>
class TConfigurePart: public TSubOperationState {
    const TTxState::ETxType TxType;
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << TKind::Name() << " TConfigurePart"
                << ", opId: " << OperationId;
    }

    static TVirtualTimestamp GetSnapshotTime(const TSchemeShard* ss, const TPathId& pathId) {
        Y_ABORT_UNLESS(ss->PathsById.contains(pathId));
        TPathElement::TPtr path = ss->PathsById.at(pathId);
        return TVirtualTimestamp(path->StepCreated, path->CreateTxId);
    }

public:
    TConfigurePart(TTxState::ETxType type, TOperationId id)
        : TxType(type)
        , OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 DebugHint() << " HandleReply TEvProposeTransactionResult"
                 << " at tabletId# " << ssId
                 << " message# " << ev->Get()->Record.ShortDebugString());

        return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
    }

    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    TKind::Name() << " TConfigurePart ProgressState"
                        << ", opId: " << OperationId
                        << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->TxInFlight.FindPtr(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TxType);
        Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

        txState->ClearShardsInProgress();
        if constexpr (TKind::NeedSnapshotTime()) {
            TKind::ProposeTx(OperationId, *txState, context, GetSnapshotTime(context.SS, txState->TargetPathId));
        } else {
            TKind::ProposeTx(OperationId, *txState, context);
        }
        txState->UpdateShardsInProgress(TTxState::ConfigureParts);

        return false;
    }
};

template <typename TKind>
class TProposedWaitParts: public TSubOperationState {
protected:
    const TTxState::ETxType TxType;
    const TOperationId OperationId;

private:
    TString DebugHint() const override {
        return TStringBuilder()
                << TKind::Name() << " TProposedWaitParts"
                << ", opId: " << OperationId;
    }

public:
    TProposedWaitParts(TTxState::ETxType type, TOperationId id)
        : TxType(type)
        , OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            { TEvHive::TEvCreateTabletReply::EventType
            , TEvDataShard::TEvProposeTransactionResult::EventType
            , TEvPrivate::TEvOperationPlan::EventType }
        );
    }

    static void Bill(TOperationId operationId, const TPathId& pathId, const TShardIdx& shardIdx, ui64 ru, TOperationContext& context) {
        const auto path = TPath::Init(pathId, context.SS);
        const auto pathIdForDomainId = path.GetPathIdForDomain();
        const auto domainPath = TPath::Init(pathIdForDomainId, context.SS);

        auto unableToMakeABill = [&](const TStringBuf reason) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to make a bill"
                << ": kind# " << TKind::Name()
                << ", opId# " << operationId
                << ", reason# " << reason
                << ", domain# " << domainPath.PathString()
                << ", domainPathId# " << pathIdForDomainId
                << ", IsDomainSchemeShard: " << context.SS->IsDomainSchemeShard
                << ", ParentDomainId: " << context.SS->ParentDomainId
                << ", ResourcesDomainId: " << domainPath.DomainInfo()->GetResourcesDomainId());
        };

        if (!context.SS->IsServerlessDomain(domainPath)) {
            return unableToMakeABill("domain is not a serverless db");
        }

        const auto& attrs = domainPath.Base()->UserAttrs->Attrs;
        if (!attrs.contains("cloud_id")) {
            return unableToMakeABill("cloud_id not found in user attributes");
        }

        if (!attrs.contains("folder_id")) {
            return unableToMakeABill("folder_id not found in user attributes");
        }

        if (!attrs.contains("database_id")) {
            return unableToMakeABill("database_id not found in user attributes");
        }

        if (!TKind::NeedToBill(pathId, context)) {
            return unableToMakeABill("does not need to bill");
        }

        const auto now = context.Ctx.Now();
        const TString id = TStringBuilder() << operationId.GetTxId()
            << "-" << pathId.OwnerId << "-" << pathId.LocalPathId
            << "-" << shardIdx.GetOwnerId() << "-" << shardIdx.GetLocalId();

        const TString billRecord = TBillRecord()
            .Id(id)
            .CloudId(attrs.at("cloud_id"))
            .FolderId(attrs.at("folder_id"))
            .ResourceId(attrs.at("database_id"))
            .SourceWt(now)
            .Usage(TBillRecord::RequestUnits(Max(ui64(1), ru), now))
            .ToString();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Make a bill"
            << ": kind# " << TKind::Name()
            << ", opId# " << operationId
            << ", domain# " << domainPath.PathString()
            << ", domainPathId# " << pathIdForDomainId
            << ", record# " << billRecord);

        context.OnComplete.Send(NMetering::MakeMeteringServiceID(),
            new NMetering::TEvMetering::TEvWriteMeteringJson(std::move(billRecord)));
    }

    static void CollectStats(TOperationId operationId, const TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) {
        const auto& evRecord = ev->Get()->Record;

        if (!evRecord.HasOpResult() || !evRecord.GetOpResult().HasSuccess()) {
            return;
        }

        Y_ABORT_UNLESS(context.SS->FindTx(operationId));
        TTxState& txState = *context.SS->FindTx(operationId);

        auto tabletId = TTabletId(evRecord.GetOrigin());
        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

        if (!txState.SchemeChangeNotificationReceived.contains(shardIdx)) {
            return;
        }

        ui32 generation = evRecord.GetGeneration();
        if (txState.SchemeChangeNotificationReceived[shardIdx].second != generation) {
            return;
        }

        NIceDb::TNiceDb db(context.GetDB());
        const auto& result = evRecord.GetOpResult();

        if (!txState.ShardStatuses.contains(shardIdx)) {
            auto& shardStatus = txState.ShardStatuses[shardIdx];
            shardStatus.Success = result.GetSuccess();
            shardStatus.Error = result.GetExplain();
            shardStatus.BytesProcessed = result.GetBytesProcessed();
            shardStatus.RowsProcessed = result.GetRowsProcessed();
            context.SS->PersistTxShardStatus(db, operationId, shardIdx, shardStatus);

            const ui64 ru = TKind::RequestUnits(shardStatus.BytesProcessed, shardStatus.RowsProcessed);
            Bill(operationId, txState.TargetPathId, shardIdx, ru, context);
        }

        // TODO(ilnaz): backward compatibility, remove it
        if (result.GetSuccess()) {
            if (result.HasBytesProcessed()) {
                txState.DataTotalSize += result.GetBytesProcessed();

                db.Table<Schema::TxInFlightV2>().Key(operationId.GetTxId(), operationId.GetSubTxId()).Update(
                            NIceDb::TUpdate<Schema::TxInFlightV2::DataTotalSize>(txState.DataTotalSize));
            }
        } else {
            if (result.HasExplain()) {
                TString explain = result.GetExplain();

                if (context.SS->IsLocalId(shardIdx)) {
                    db.Table<Schema::ShardBackupStatus>().Key(operationId.GetTxId(), shardIdx.GetLocalId()).Update(
                        NIceDb::TUpdate<Schema::ShardBackupStatus::Explain>(explain));
                } else {
                    db.Table<Schema::MigratedShardBackupStatus>().Key(operationId.GetTxId(), shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
                        NIceDb::TUpdate<Schema::MigratedShardBackupStatus::Explain>(explain));
                }
            }
        }
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvSchemaChanged"
                     << " at tablet# " << ssId
                     << " message# " << evRecord.ShortDebugString());

        bool allnotificationsReceived = NTableState::CollectSchemaChanged(OperationId, ev, context);
        CollectStats(OperationId, ev, context);

        if (!allnotificationsReceived) {
            return false;
        }

        Y_ABORT_UNLESS(context.SS->FindTx(OperationId));
        TTxState& txState = *context.SS->FindTx(OperationId);

        if (!txState.ReadyForNotifications) {
            return false;
        }

        TKind::Finish(OperationId, txState, context);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);

        NIceDb::TNiceDb db(context.GetDB());

        txState->ClearShardsInProgress();
        for (TTxState::TShardOperation& shard : txState->Shards) {
            if (shard.Operation < TTxState::ProposedWaitParts) {
                shard.Operation = TTxState::ProposedWaitParts;
                context.SS->PersistUpdateTxShard(db, OperationId, shard.Idx, shard.Operation);
            }
            context.OnComplete.RouteByTablet(OperationId, context.SS->ShardInfos.at(shard.Idx).TabletID);
        }
        txState->UpdateShardsInProgress(TTxState::ProposedWaitParts);

        if (txState->Cancel) {
            context.SS->ChangeTxState(db, OperationId, TTxState::Aborting);
            return true;
        }

        // Move all notifications that were already received
        // NOTE: SchemeChangeNotification is sent form DS after it has got PlanStep from coordinator and the schema tx has completed
        // At that moment the SS might not have received PlanStep from coordinator yet (this message might be still on its way to SS)
        // So we are going to accumulate SchemeChangeNotification that are received before this Tx switches to WaitParts state
        txState->AcceptPendingSchemeNotification();

        // Got notifications from all datashards?
        if (txState->ShardsInProgress.empty()) {
            NTableState::AckAllSchemaChanges(OperationId, *txState, context);
            context.SS->ChangeTxState(db, OperationId, TTxState::Done);
            TKind::Finish(OperationId, *txState, context);
            return true;
        }

        return false;
    }
};

template <typename TKind, typename TEvCancel>
class TAborting: public TProposedWaitParts<TKind> {
    using TProposedWaitParts<TKind>::OperationId;
    using TProposedWaitParts<TKind>::TxType;

    TString DebugHint() const override {
        return TStringBuilder()
                << TKind::Name() << " TAborting"
                << ", opId: " << OperationId;
    }

public:
    TAborting(TTxState::ETxType type, TOperationId id)
        : TProposedWaitParts<TKind>(type, id)
    {
        this->IgnoreMessages(DebugHint(),
            { TEvHive::TEvCreateTabletReply::EventType
            , TEvDataShard::TEvProposeTransactionResult::EventType
            , TEvPrivate::TEvOperationPlan::EventType }
        );
    }

    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " ProgressState"
                    << " at tablet" << ssId);

        TTxState* txState = context.SS->TxInFlight.FindPtr(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TxType);
        Y_ABORT_UNLESS(txState->State == TTxState::Aborting);

        txState->ClearShardsInProgress();

        NIceDb::TNiceDb db(context.GetDB());

        for (TTxState::TShardOperation& shard : txState->Shards) {
            if (shard.Operation < TTxState::ProposedWaitParts) {
                shard.Operation = TTxState::ProposedWaitParts;
                context.SS->PersistUpdateTxShard(
                    db, OperationId, shard.Idx, shard.Operation);
            }
        }

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            auto idx = txState->Shards[i].Idx;
            auto datashardId = context.SS->ShardInfos[idx].TabletID;

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        TKind::Name() << " Abort"
                            << ", on datashard: " << datashardId
                            << ", opId: " << OperationId
                            << ", at schemeshard: " << context.SS->TabletID());

            auto event = MakeHolder<TEvCancel>(ui64(OperationId.GetTxId()), txState->TargetPathId.LocalPathId);
            context.OnComplete.BindMsgToPipe(OperationId, datashardId, idx, event.Release());
        }

        txState->UpdateShardsInProgress(TTxState::ProposedWaitParts);

        txState->AcceptPendingSchemeNotification();

        // Got notifications from all datashards?
        if (txState->ShardsInProgress.empty()) {
            NTableState::AckAllSchemaChanges(OperationId, *txState, context);
            context.SS->ChangeTxState(db, OperationId, TTxState::Done);
            TKind::Finish(OperationId, *txState, context);
            return true;
        }

        return false;
    }
};

template <typename TKind>
class TPropose: public TSubOperationState {
    const TTxState::ETxType TxType;
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << TKind::Name() << " TPropose"
                << ", opId: " << OperationId;
    }

public:
    TPropose(TTxState::ETxType type, TOperationId id)
        : TxType(type)
        , OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvDataShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSchemaChanged"
                               << " triggers early, save it"
                               << ", at schemeshard: " << ssId);

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        TProposedWaitParts<TKind>::CollectStats(OperationId, ev, context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        auto step = TStepId(ev->Get()->StepId);
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", stepId: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TxType);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        TKind::PersistDone(pathId, context);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TxType);

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            auto idx = shard.Idx;
            auto tablet = context.SS->ShardInfos.at(idx).TabletID;
            shardSet.insert(tablet);
        }

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);
        return false;
    }
};

template <typename TKind, typename TEvCancel>
class TBackupRestoreOperationBase: public TSubOperation {
    const TTxState::ETxType TxType;
    const TPathElement::EPathState Lock;

    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState) const override {
        Y_ABORT("unreachable");
    }

    TTxState::ETxState NextState(TTxState::ETxState state, TOperationContext& context) const {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::ProposedWaitParts;
        case TTxState::ProposedWaitParts: {
            TTxState* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);
            Y_ABORT_UNLESS(txState->TxType == TxType);

            if (txState->Cancel) {
                if (txState->State == TTxState::Done) {
                    return TTxState::Done;
                }

                Y_ABORT_UNLESS(txState->State == TTxState::Aborting);
                return TTxState::Aborting;
            }
            return TTxState::Done;
        }
        case TTxState::Aborting:
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
            return MakeHolder<TConfigurePart<TKind>>(TxType, OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose<TKind>>(TxType, OperationId);
        case TTxState::ProposedWaitParts:
            return MakeHolder<TProposedWaitParts<TKind>>(TxType, OperationId);
        case TTxState::Aborting:
            return MakeHolder<TAborting<TKind, TEvCancel>>(TxType, OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    void StateDone(TOperationContext& context) override {
        auto state = NextState(GetState(), context);
        SetState(state);

        if (state != TTxState::Invalid) {
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    TBackupRestoreOperationBase(
            TTxState::ETxType type, TPathElement::EPathState lock,
            TOperationId id, const TTxTransaction& tx)
        : TSubOperation(id, tx)
        , TxType(type)
        , Lock(lock)
    {
    }

    TBackupRestoreOperationBase(
            TTxState::ETxType type, TPathElement::EPathState lock,
            TOperationId id, TTxState::ETxState state)
        : TSubOperation(id, state)
        , TxType(type)
        , Lock(lock)
    {
        SetState(state);
    }

    void PrepareChanges(TPathElement::TPtr path, TOperationContext& context) {
        Y_ABORT_UNLESS(context.SS->Tables.contains(path->PathId));
        TTableInfo::TPtr& table = context.SS->Tables.at(path->PathId);

        path->LastTxId = OperationId.GetTxId();
        path->PathState = Lock;

        TTxState& txState = context.SS->CreateTx(OperationId, TxType, path->PathId);

        txState.Shards.reserve(table->GetPartitions().size());
        for (const auto& shard : table->GetPartitions()) {
            auto shardIdx = shard.ShardIdx;
            txState.Shards.emplace_back(shardIdx, ETabletType::DataShard, TTxState::ConfigureParts);
        }

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistTxState(db, OperationId);

        TKind::PersistTask(path->PathId, Transaction, context);

        for (auto splitTx: table->GetSplitOpsInFlight()) {
            context.OnComplete.Dependence(splitTx.GetTxId(), OperationId.GetTxId());
        }

        txState.State = TTxState::CreateParts;
        context.OnComplete.ActivateTx(OperationId);
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const TString& parentPath = Transaction.GetWorkingDir();
        const TString name = TKind::GetTableName(Transaction);

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     TKind::Name() << " Propose"
                         << ", path: " << parentPath << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!Transaction.HasWorkingDir()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter,
                             "Malformed request: no working dir");
            return result;
        }

        if (!TKind::HasTask(Transaction)) {
            result->SetError(
                NKikimrScheme::StatusInvalidParameter,
                "Malformed request");
            return result;
        }

        if (name.empty()) {
            result->SetError(
                NKikimrScheme::StatusInvalidParameter,
                "No table name in task");
            return result;
        }

        TPath path = TPath::Resolve(parentPath, context.SS).Dive(name);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTable()
                .NotAsyncReplicaTable()
                .NotUnderOperation()
                .IsCommonSensePath() //forbid alter impl index tables
                .CanBackupTable(); //forbid backup table with indexes

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!context.SS->CheckLocks(path.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        PrepareChanges(path.Base(), context);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TBackupRestoreOperationBase");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     TKind::Name() << " AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}
