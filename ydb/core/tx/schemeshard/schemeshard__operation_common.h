#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/base/subdomain.h>

namespace NKikimr {
namespace NSchemeShard {

TSet<ui32> AllIncomingEvents();

void IncParentDirAlterVersionWithRepublishSafeWithUndo(const TOperationId& opId, const TPath& path, TSchemeShard* ss, TSideEffects& onComplete);
void IncParentDirAlterVersionWithRepublish(const TOperationId& opId, const TPath& path, TOperationContext& context);

NKikimrSchemeOp::TModifyScheme MoveTableTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst);
NKikimrSchemeOp::TModifyScheme MoveTableIndexTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst);

namespace NTableState {

bool CollectProposeTransactionResults(const TOperationId& operationId, const TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context);
bool CollectProposeTransactionResults(const TOperationId& operationId, const TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context);
bool CollectSchemaChanged(const TOperationId& operationId, const TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context);

void SendSchemaChangedNotificationAck(const TOperationId& operationId, TActorId ackTo, TShardIdx shardIdx, TOperationContext& context);
void AckAllSchemaChanges(const TOperationId& operationId, TTxState& txState, TOperationContext& context);

bool CheckPartitioningChangedForTableModification(TTxState& txState, TOperationContext& context);
void UpdatePartitioningForTableModification(TOperationId txId, TTxState& txState, TOperationContext& context);

TVector<TTableShardInfo> ApplyPartitioningCopyTable(const TShardInfo& templateDatashardInfo, TTableInfo::TPtr srcTableInfo, TTxState& txState, TSchemeShard* ss);

bool SourceTablePartitioningChangedForCopyTable(const TTxState& txState, TOperationContext& context);
void UpdatePartitioningForCopyTable(TOperationId operationId, TTxState& txState, TOperationContext& context);

class TProposedWaitParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NTableState::TProposedWaitParts"
                << " operationId# " << OperationId;
    }

public:
    TProposedWaitParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            { TEvHive::TEvCreateTabletReply::EventType
            , TEvDataShard::TEvProposeTransactionResult::EventType
            , TEvPrivate::TEvOperationPlan::EventType }
        );
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const auto& evRecord = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSchemaChanged"
                               << " at tablet: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvSchemaChanged"
                                << " at tablet: " << ssId
                                << " message: " << evRecord.ShortDebugString());

        if (!NTableState::CollectSchemaChanged(OperationId, ev, context)) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " HandleReply TEvSchemaChanged"
                                    << " CollectSchemaChanged: false");
            return false;
        }

        Y_VERIFY(context.SS->FindTx(OperationId));
        TTxState& txState = *context.SS->FindTx(OperationId);

        if (!txState.ReadyForNotifications) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " HandleReply TEvSchemaChanged"
                                    << " ReadyForNotifications: false");
            return false;
        }

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " ProgressState"
                     << " at tablet: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);

        NIceDb::TNiceDb db(context.GetDB());

        txState->ClearShardsInProgress();
        for (TTxState::TShardOperation& shard : txState->Shards) {
            if (shard.Operation < TTxState::ProposedWaitParts) {
                shard.Operation = TTxState::ProposedWaitParts;
                context.SS->PersistUpdateTxShard(db, OperationId, shard.Idx, shard.Operation);
            }
            Y_VERIFY(context.SS->ShardInfos.contains(shard.Idx));
            context.OnComplete.RouteByTablet(OperationId,  context.SS->ShardInfos.at(shard.Idx).TabletID);
        }
        txState->UpdateShardsInProgress(TTxState::ProposedWaitParts);

        // Move all notifications that were already received
        // NOTE: SchemeChangeNotification is sent form DS after it has got PlanStep from coordinator and the schema tx has completed
        // At that moment the SS might not have received PlanStep from coordinator yet (this message might be still on its way to SS)
        // So we are going to accumulate SchemeChangeNotification that are received before this Tx switches to WaitParts state
        txState->AcceptPendingSchemeNotification();

        // Got notifications from all datashards?
        if (txState->ShardsInProgress.empty()) {
            NTableState::AckAllSchemaChanges(OperationId, *txState, context);
            context.SS->ChangeTxState(db, OperationId, TTxState::Done);
            return true;
        }

        return false;
    }
};

}

namespace NSubDomainState {

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NSubDomainState::TConfigureParts"
                << " operationId#" << OperationId;
    }
public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvSchemeShard::TEvInitTenantSchemeShardResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint()
                       << " HandleReply TEvInitTenantSchemeShardResult"
                       << " operationId: " << OperationId
                       << " at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateSubDomain
                 || txState->TxType == TTxState::TxAlterSubDomain
                 || txState->TxType == TTxState::TxAlterExtSubDomain);

        const auto& record = ev->Get()->Record;

        NIceDb::TNiceDb db(context.GetDB());

        TTabletId tabletId = TTabletId(record.GetTenantSchemeShard());
        auto status = record.GetStatus();

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        Y_VERIFY(context.SS->ShardInfos.contains(shardIdx));

        if (status != NKikimrScheme::EStatus::StatusSuccess && status != NKikimrScheme::EStatus::StatusAlreadyExists) {
            LOG_CRIT_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint()
                           << " Got error status on SubDomain Configure"
                           << "from tenant schemeshard tablet: " << tabletId
                           << " shard: " << shardIdx
                           << " status: " << NKikimrScheme::EStatus_Name(status)
                           << " opId: " << OperationId
                           << " schemeshard: " << ssId);
            return false;
        }

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint()
                        << " Got OK TEvInitTenantSchemeShardResult from schemeshard"
                        << " tablet: " << tabletId
                        << " shardIdx: " << shardIdx
                        << " at schemeshard: " << ssId);

        txState->ShardsInProgress.erase(shardIdx);
        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, shardIdx);

        if (txState->ShardsInProgress.empty()) {
            // All tablets have replied so we can done this transaction
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }

    bool HandleReply(TEvSubDomain::TEvConfigureStatus::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint()
                    << " HandleReply TEvConfigureStatus"
                    << " operationId:" << OperationId
                    << " at schemeshard:" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateSubDomain
                 || txState->TxType == TTxState::TxAlterSubDomain
                 || txState->TxType == TTxState::TxAlterExtSubDomain);

        const auto& record = ev->Get()->Record;

        NIceDb::TNiceDb db(context.GetDB());

        TTabletId tabletId = TTabletId(record.GetOnTabletId());
        auto status = record.GetStatus();

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        Y_VERIFY(context.SS->ShardInfos.contains(shardIdx));

        if (status == NKikimrTx::TEvSubDomainConfigurationAck::REJECT) {
            LOG_CRIT_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint()
                           << " Got REJECT on SubDomain Configure"
                           << "from tablet: " << tabletId
                           << " shard: " << shardIdx
                           << " opId: " << OperationId
                           << " schemeshard: " << ssId);
            return false;
        }

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() <<
                    " Got OK TEvConfigureStatus from "
                    << " tablet# " << tabletId
                    << " shardIdx# " << shardIdx
                    << " at schemeshard# " << ssId);

        txState->ShardsInProgress.erase(shardIdx);
        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, shardIdx);

        if (txState->ShardsInProgress.empty()) {
            // All tablets have replied so we can done this transaction
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }


    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint()
                       << " ProgressState"
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateSubDomain
                 || txState->TxType == TTxState::TxAlterSubDomain
                 || txState->TxType == TTxState::TxAlterExtSubDomain);

        txState->ClearShardsInProgress();

        if (txState->Shards.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        auto pathId = txState->TargetPathId;
        Y_VERIFY(context.SS->PathsById.contains(pathId));
        TPath path = TPath::Init(pathId, context.SS);

        Y_VERIFY(context.SS->SubDomains.contains(pathId));
        auto subDomain = context.SS->SubDomains.at(pathId);
        auto alterData = subDomain->GetAlter();
        Y_VERIFY(alterData);
        alterData->Initialize(context.SS->ShardInfos);
        auto processing = alterData->GetProcessingParams();
        auto storagePools = alterData->GetStoragePools();
        auto& schemeLimits = subDomain->GetSchemeLimits();

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            auto &shard = txState->Shards[i];
            TShardIdx idx = shard.Idx;
            Y_VERIFY(context.SS->ShardInfos.contains(idx));
            TTabletId tabletID = context.SS->ShardInfos[idx].TabletID;
            auto type = context.SS->ShardInfos[idx].TabletType;

            switch (type) {
            case ETabletType::Coordinator:
            case ETabletType::Mediator: {
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Send configure request to coordinator/mediator: " << tabletID <<
                    " opId: " << OperationId <<
                    " schemeshard: " << ssId);
                shard.Operation = TTxState::ConfigureParts;
                auto event = new TEvSubDomain::TEvConfigure(processing);
                context.OnComplete.BindMsgToPipe(OperationId, tabletID, idx, event);
                break;
            }
            case ETabletType::Hive: {
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Send configure request to hive: " << tabletID <<
                    " opId: " << OperationId <<
                    " schemeshard: " << ssId);
                shard.Operation = TTxState::ConfigureParts;
                auto event = new TEvHive::TEvConfigureHive(TSubDomainKey(pathId.OwnerId, pathId.LocalPathId));
                context.OnComplete.BindMsgToPipe(OperationId, tabletID, idx, event);
                break;
            }
            case ETabletType::SysViewProcessor: {
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Send configure request to sysview processor: " << tabletID <<
                    " opId: " << OperationId <<
                    " schemeshard: " << ssId);
                auto event = new NSysView::TEvSysView::TEvConfigureProcessor(path.PathString());
                shard.Operation = TTxState::ConfigureParts;
                context.OnComplete.BindMsgToPipe(OperationId, tabletID, idx, event);
                break;
            }
            case ETabletType::SchemeShard: {
                auto event = new TEvSchemeShard::TEvInitTenantSchemeShard(ui64(ssId),
                                                                              pathId.LocalPathId, path.PathString(),
                                                                              path.Base()->Owner, path.GetEffectiveACL(), path.GetEffectiveACLVersion(),
                                                                              processing, storagePools,
                                                                              path.Base()->UserAttrs->Attrs, path.Base()->UserAttrs->AlterVersion,
                                                                              schemeLimits, ui64(alterData->GetSharedHive()), alterData->GetResourcesDomainId()
                                                                              );
                if (alterData->GetDeclaredSchemeQuotas()) {
                    event->Record.MutableDeclaredSchemeQuotas()->CopyFrom(*alterData->GetDeclaredSchemeQuotas());
                }
                if (alterData->GetDatabaseQuotas()) {
                    event->Record.MutableDatabaseQuotas()->CopyFrom(*alterData->GetDatabaseQuotas());
                }
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Send configure request to schemeshard: " << tabletID <<
                                " opId: " << OperationId <<
                                " schemeshard: " << ssId <<
                                " msg: " << event->Record.ShortDebugString());

                shard.Operation = TTxState::ConfigureParts;
                context.OnComplete.BindMsgToPipe(OperationId, tabletID, idx, event);
                break;
            }
            default:
                Y_FAIL_S("Unexpected type, we don't create tablets with type " << ETabletType::TypeToStr(type));
            }
        }

        txState->UpdateShardsInProgress(TTxState::ConfigureParts);
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NSubDomainState::TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvHive::TEvCreateTabletReply::EventType, TEvSubDomain::TEvConfigureStatus::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "NSubDomainState::TPropose HandleReply TEvOperationPlan"
                     << " operationId#" << OperationId
                     << " at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if (!txState) {
            return false;
        }
        Y_VERIFY(txState->TxType == TTxState::TxCreateSubDomain
                 || txState->TxType == TTxState::TxAlterSubDomain
                 || txState->TxType == TTxState::TxCreateExtSubDomain
                 || txState->TxType == TTxState::TxAlterExtSubDomain);

        TPathId pathId = txState->TargetPathId;
        Y_VERIFY(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        Y_VERIFY(context.SS->SubDomains.contains(pathId));
        auto subDomain = context.SS->SubDomains.at(pathId);
        auto alter = subDomain->GetAlter();
        Y_VERIFY(alter);
        Y_VERIFY(subDomain->GetVersion() < alter->GetVersion());

        subDomain->ActualizeAlterData(context.SS->ShardInfos, context.Ctx.Now(),
                /* isExternal */ path->PathType == TPathElement::EPathType::EPathTypeExtSubDomain,
                context.SS);

        context.SS->SubDomains[pathId] = alter;
        context.SS->PersistSubDomain(db, pathId, *alter);
        context.SS->PersistSubDomainSchemeQuotas(db, pathId, *alter);

        if (txState->TxType == TTxState::TxCreateSubDomain || txState->TxType == TTxState::TxCreateExtSubDomain) {
            auto parentDir = context.SS->PathsById.at(path->ParentPathId);
            ++parentDir->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentDir);
            context.SS->ClearDescribePathCaches(parentDir);
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
        }

        context.OnComplete.UpdateTenant(pathId);
        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "NSubDomainState::TPropose ProgressState"
                       << ", operationId: " << OperationId
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateSubDomain
                 || txState->TxType == TTxState::TxAlterSubDomain
                 || txState->TxType == TTxState::TxCreateExtSubDomain
                 || txState->TxType == TTxState::TxAlterExtSubDomain);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

}

class TCreateParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateParts"
                << " operationId: " << OperationId;
    }

public:
    TCreateParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvHive::TEvAdoptTabletReply::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvAdoptTablet"
                   << ", at tabletId: " << context.SS->TabletID());
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvAdoptTablet"
                    << ", message% " << DebugReply(ev));

        NIceDb::TNiceDb db(context.GetDB());

        const TString& explain = ev->Get()->Record.GetExplain();
        TTabletId tabletId = TTabletId(ev->Get()->Record.GetTabletID()); // global id from hive
        auto shardIdx = context.SS->MakeLocalId(TLocalShardIdx(ev->Get()->Record.GetOwnerIdx())); // global id from hive
        TTabletId hive = TTabletId(ev->Get()->Record.GetOrigin());

        Y_VERIFY(context.SS->TabletID() == ev->Get()->Record.GetOwner());

        NKikimrProto::EReplyStatus status = ev->Get()->Record.GetStatus();
        Y_VERIFY_S(status ==  NKikimrProto::OK || status == NKikimrProto::ALREADY,
                   "Unexpected status " << NKikimrProto::EReplyStatus_Name(status)
                                        << " in AdoptTabletReply for tabletId " << tabletId
                                        << " with explain " << explain);

        // Note that HIVE might send duplicate TTxAdoptTabletReply in case of restarts
        // So we just ignore the event if we cannot find the Tx or if it is in a different
        // state

        Y_VERIFY(context.SS->ShardInfos.contains(shardIdx));

        if (!context.SS->AdoptedShards.contains(shardIdx)) {
            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " HandleReply TEvAdoptTablet"
                       << " Got TTxAdoptTabletReply for shard but it is not present in AdoptedShards"
                       << ", shardIdx: " << shardIdx
                       << ", tabletId:" << tabletId);
            return false;
        }

        TShardInfo& shardInfo = context.SS->ShardInfos[shardIdx];
        Y_VERIFY(shardInfo.TabletID == InvalidTabletId || shardInfo.TabletID == tabletId);

        Y_VERIFY(tabletId != InvalidTabletId);
        shardInfo.TabletID = tabletId;
        context.SS->TabletIdToShardIdx[tabletId] = shardIdx;

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->State == TTxState::CreateParts);

        txState->ShardsInProgress.erase(shardIdx);

        context.SS->AdoptedShards.erase(shardIdx);
        context.SS->PersistDeleteAdopted(db, shardIdx);
        context.SS->PersistShardMapping(db, shardIdx, tabletId, shardInfo.PathId, OperationId.GetTxId(), shardInfo.TabletType);

        context.OnComplete.UnbindMsgFromPipe(OperationId, hive, shardIdx);
        context.OnComplete.ActivateShardCreated(shardIdx, OperationId.GetTxId());

        // If all datashards have been created
        if (txState->ShardsInProgress.empty()) {
            context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);
            return true;
        }

        return false;
    }

    bool HandleReply(TEvHive::TEvCreateTabletReply::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvCreateTabletReply"
                   << ", at tabletId: " << context.SS->TabletID());
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvCreateTabletReply"
                    << ", message: " << DebugReply(ev));

        NIceDb::TNiceDb db(context.GetDB());

        auto shardIdx = TShardIdx(ev->Get()->Record.GetOwner(),
                                  TLocalShardIdx(ev->Get()->Record.GetOwnerIdx()));

        auto tabletId = TTabletId(ev->Get()->Record.GetTabletID()); // global id from hive
        auto hive = TTabletId(ev->Get()->Record.GetOrigin());

        NKikimrProto::EReplyStatus status = ev->Get()->Record.GetStatus();

        Y_VERIFY_S(status ==  NKikimrProto::OK
                       || status == NKikimrProto::ALREADY
                       || status ==  NKikimrProto::INVALID_OWNER
                       || status == NKikimrProto::BLOCKED,
                   "Unexpected status " << NKikimrProto::EReplyStatus_Name(status)
                                        << " in CreateTabletReply shard idx " << shardIdx << " tabletId " << tabletId);

        if (status ==  NKikimrProto::BLOCKED) {
            Y_VERIFY(!context.SS->IsDomainSchemeShard);

            LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         DebugHint() << " CreateRequest BLOCKED "
                                     << " at Hive: " << hive
                                     << " msg: " << DebugReply(ev));

            // do not unsubscribe message
            // context.OnComplete.UnbindMsgFromPipe(OperationId, hive, shardIdx);

            // just stay calm and hung until tenant schemeshard is deleted
            return false;
        }

        TTxState& txState = *context.SS->FindTx(OperationId);

        TShardInfo& shardInfo = context.SS->ShardInfos.at(shardIdx);
        Y_VERIFY(shardInfo.TabletID == InvalidTabletId || shardInfo.TabletID == tabletId);

        if (status ==  NKikimrProto::INVALID_OWNER) {
            auto redirectTo = TTabletId(ev->Get()->Record.GetForwardRequest().GetHiveTabletId());
            Y_VERIFY(redirectTo);
            Y_VERIFY(tabletId);

            context.OnComplete.UnbindMsgFromPipe(OperationId, hive, shardIdx);

            auto path = context.SS->PathsById.at(txState.TargetPathId);
            auto request = CreateRequest(path, shardIdx, context);

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " CreateRequest"
                                    << " Redirect from Hive: " << hive
                                    << " to Hive: " << redirectTo
                                    << " msg:  " << request->Record.DebugString());

            context.OnComplete.BindMsgToPipe(OperationId, redirectTo, shardIdx, request.Release());
            return false;
        }

        if (shardInfo.TabletID == InvalidTabletId) {
            switch (shardInfo.TabletType) {
            case ETabletType::DataShard:
                context.SS->TabletCounters->Simple()[COUNTER_TABLE_SHARD_ACTIVE_COUNT].Add(1);
                break;
            case ETabletType::Coordinator:
                context.SS->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_COORDINATOR_COUNT].Add(1);
                break;
            case ETabletType::Mediator:
                context.SS->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_MEDIATOR_COUNT].Add(1);
                break;
            case ETabletType::Hive:
                context.SS->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_HIVE_COUNT].Add(1);
                break;
            case ETabletType::SysViewProcessor:
                context.SS->TabletCounters->Simple()[COUNTER_SYS_VIEW_PROCESSOR_COUNT].Add(1);
                break;
            default:
                break;
            }
        }

        Y_VERIFY(tabletId != InvalidTabletId);
        shardInfo.TabletID = tabletId;
        context.SS->TabletIdToShardIdx[tabletId] = shardIdx;

        Y_VERIFY(OperationId.GetTxId() == shardInfo.CurrentTxId);

        txState.ShardsInProgress.erase(shardIdx);

        context.SS->PersistShardMapping(db, shardIdx, tabletId, shardInfo.PathId, OperationId.GetTxId(), shardInfo.TabletType);
        context.OnComplete.UnbindMsgFromPipe(OperationId, hive, shardIdx);
        context.OnComplete.ActivateShardCreated(shardIdx, OperationId.GetTxId());

        // If all datashards have been created
        if (txState.ShardsInProgress.empty()) {
            context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);
            return true;
        }

        return false;
    }

    THolder<TEvHive::TEvCreateTablet> CreateRequest(TPathElement::TPtr targetPath, TShardIdx shardIdx, TOperationContext& context) {
        auto tablePartitionConfig = context.SS->GetTablePartitionConfigWithAlterData(targetPath->PathId);
        const auto& shard = context.SS->ShardInfos[shardIdx];

        if (shard.TabletType == ETabletType::BlockStorePartition ||
            shard.TabletType == ETabletType::BlockStorePartition2)
        {
            auto it = context.SS->BlockStoreVolumes.FindPtr(targetPath->PathId);
            Y_VERIFY(it, "Missing BlockStoreVolume while creating BlockStorePartition tablet");
            auto volume = *it;
            const auto* volumeConfig = &volume->VolumeConfig;
            if (volume->AlterData) {
                volumeConfig = &volume->AlterData->VolumeConfig;
            }
        }

        THolder<TEvHive::TEvCreateTablet> ev = MakeHolder<TEvHive::TEvCreateTablet>(ui64(shardIdx.GetOwnerId()), ui64(shardIdx.GetLocalId()), shard.TabletType, shard.BindedChannels);

        TPathId domainId = context.SS->ResolveDomainId(targetPath);

        TPathElement::TPtr domainEl = context.SS->PathsById.at(domainId);
        auto objectDomain = ev->Record.MutableObjectDomain();
        if (domainEl->IsRoot()) {
            objectDomain->SetSchemeShard(context.SS->ParentDomainId.OwnerId);
            objectDomain->SetPathId(context.SS->ParentDomainId.LocalPathId);
        } else {
            objectDomain->SetSchemeShard(domainId.OwnerId);
            objectDomain->SetPathId(domainId.LocalPathId);
        }

        Y_VERIFY(context.SS->SubDomains.contains(domainId));
        TSubDomainInfo::TPtr subDomain = context.SS->SubDomains.at(domainId);

        TPathId resourcesDomainId;
        if (subDomain->GetResourcesDomainId()) {
            resourcesDomainId = subDomain->GetResourcesDomainId();
        } else if (subDomain->GetAlter() && subDomain->GetAlter()->GetResourcesDomainId()) {
            resourcesDomainId = subDomain->GetAlter()->GetResourcesDomainId();
        } else {
            Y_FAIL("Cannot retrieve resources domain id");
        }

        auto allowedDomain = ev->Record.AddAllowedDomains();
        allowedDomain->SetSchemeShard(resourcesDomainId.OwnerId);
        allowedDomain->SetPathId(resourcesDomainId.LocalPathId);

        if (tablePartitionConfig) {
            if (tablePartitionConfig->FollowerGroupsSize()) {
                ev->Record.MutableFollowerGroups()->CopyFrom(tablePartitionConfig->GetFollowerGroups());
            } else {
                if (tablePartitionConfig->HasAllowFollowerPromotion()) {
                    ev->Record.SetAllowFollowerPromotion(tablePartitionConfig->GetAllowFollowerPromotion());
                }

                if (tablePartitionConfig->HasCrossDataCenterFollowerCount()) {
                    ev->Record.SetCrossDataCenterFollowerCount(tablePartitionConfig->GetCrossDataCenterFollowerCount());
                } else if (tablePartitionConfig->HasFollowerCount()) {
                    ev->Record.SetFollowerCount(tablePartitionConfig->GetFollowerCount());
                }
            }
        }

        if (shard.TabletType == ETabletType::BlockStorePartition   ||
            shard.TabletType == ETabletType::BlockStorePartition2 ||
            shard.TabletType == ETabletType::RTMRPartition) {
            // Partitions should never be booted by local
            ev->Record.SetTabletBootMode(NKikimrHive::TABLET_BOOT_MODE_EXTERNAL);
        }

        ev->Record.SetObjectId(targetPath->PathId.LocalPathId);

        if (shard.TabletID) {
            ev->Record.SetTabletID(ui64(shard.TabletID));
        }

        return ev;
    }

    THolder<TEvHive::TEvAdoptTablet> AdoptRequest(TShardIdx shardIdx, TOperationContext& context) {
        Y_VERIFY(context.SS->AdoptedShards.contains(shardIdx));
        auto& adoptedShard = context.SS->AdoptedShards[shardIdx];
        auto& shard = context.SS->ShardInfos[shardIdx];

        THolder<TEvHive::TEvAdoptTablet> ev = MakeHolder<TEvHive::TEvAdoptTablet>(
            ui64(shard.TabletID),
            adoptedShard.PrevOwner, ui64(adoptedShard.PrevShardIdx),
            shard.TabletType,
            context.SS->TabletID(), ui64(shardIdx.GetLocalId()));

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " AdoptRequest"
                    << " Event to Hive: " << ev->Record.DebugString().c_str());

        return ev;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet" << ssId);

        if (txState->TxType == TTxState::TxDropTable
            || txState->TxType == TTxState::TxAlterTable
            || txState->TxType == TTxState::TxBackup
            || txState->TxType == TTxState::TxRestore) {
            if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
                LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           DebugHint() << " ProgressState"
                                       << " SourceTablePartitioningChangedForModification"
                                       << ", tx type: " << TTxState::TypeName(txState->TxType));
                NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
            }
        } else if (txState->TxType == TTxState::TxCopyTable) {
            if (NTableState::SourceTablePartitioningChangedForCopyTable(*txState, context)) {
                LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           DebugHint() << " ProgressState"
                           << " SourceTablePartitioningChangedForCopyTable"
                           << ", tx type: " << TTxState::TypeName(txState->TxType));
                NTableState::UpdatePartitioningForCopyTable(OperationId, *txState, context);
            }
        }

        txState->ClearShardsInProgress();

        bool nothingToDo = true;
        for (const auto& shard : txState->Shards) {
            if (shard.Operation != TTxState::CreateParts) {
                continue;
            }
            nothingToDo = false;

            if (context.SS->AdoptedShards.contains(shard.Idx)) {
                auto ev = AdoptRequest(shard.Idx, context);
                context.OnComplete.BindMsgToPipe(OperationId, context.SS->GetGlobalHive(context.Ctx)    , shard.Idx, ev.Release());
            } else {
                auto path = context.SS->PathsById.at(txState->TargetPathId);
                auto ev = CreateRequest(path, shard.Idx, context);

                auto hiveToRequest = context.SS->ResolveHive(shard.Idx, context.Ctx);

                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            DebugHint() << " CreateRequest"
                                        << " Event to Hive: " << hiveToRequest
                                        << " msg:  "<< ev->Record.DebugString().c_str());

                context.OnComplete.BindMsgToPipe(OperationId, hiveToRequest, shard.Idx, ev.Release());
            }
            context.OnComplete.RouteByShardIdx(OperationId, shard.Idx);
        }

        if (nothingToDo) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        DebugHint() << " ProgressState"
                                    << " no shards to create, do next state");

            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::ConfigureParts);
            return true;
        }

        txState->UpdateShardsInProgress(TTxState::CreateParts);
        return false;
    }
};


class TDone: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TDone operationId#" << OperationId;
    }

public:
    TDone(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), AllIncomingEvents());
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);

        TPathId pathId = txState->TargetPathId;
        Y_VERIFY(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        Y_VERIFY_S(path->PathState != TPathElement::EPathState::EPathStateNoChanges,
                   "with context"
                       << ", PathState: " << NKikimrSchemeOp::EPathState_Name(path->PathState)
                       << ", PathId: " << path->PathId
                       << ", PathName: " << path->Name);

        if (path->IsPQGroup() && txState->IsCreate()) {
            TPathElement::TPtr parentDir = context.SS->PathsById.at(path->ParentPathId);
            // can't be removed until KIKIMR-8861
            // at least lets wrap it into condition
            // it helps to actualize PATHSTATE inside children listing
            context.SS->ClearDescribePathCaches(parentDir);
        }

        if (txState->IsDrop()) {
            context.OnComplete.ReleasePathState(OperationId, path->PathId, TPathElement::EPathState::EPathStateNotExist);
        } else {
            context.OnComplete.ReleasePathState(OperationId, path->PathId, TPathElement::EPathState::EPathStateNoChanges);
        }

        if (txState->SourcePathId != InvalidPathId) {
            Y_VERIFY(context.SS->PathsById.contains(txState->SourcePathId));
            TPathElement::TPtr srcPath = context.SS->PathsById.at(txState->SourcePathId);
            if (srcPath->PathState == TPathElement::EPathState::EPathStateCopying) {
                context.OnComplete.ReleasePathState(OperationId, srcPath->PathId, TPathElement::EPathState::EPathStateNoChanges);
            }
        }

        // OlapStore tracks all tables that are under operation, make sure to unlink
        if (context.SS->OlapTables.contains(pathId)) {
            auto tableInfo = context.SS->OlapTables.at(pathId);
            if (context.SS->OlapStores.contains(tableInfo->OlapStorePathId)) {
                auto storeInfo = context.SS->OlapStores.at(tableInfo->OlapStorePathId);
                storeInfo->OlapTablesUnderOperation.erase(pathId);
            }
        }

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }
};


namespace NPQState {

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NPQState::TConfigureParts"
                << " operationId#" << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvPersQueue::TEvUpdateConfigResponse::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     DebugHint() << " HandleReply TEvUpdateConfigResponse"
                     << " at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

        TTabletId tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        NKikimrPQ::EStatus status = ev->Get()->Record.GetStatus();

        // Schemeshard always sends a valid config to the PQ tablet and PQ tablet is not supposed to reject it
        // Also Schemeshard never send multiple different config updates simultaneously (same config
        // can be sent more than once in case of retries due to restarts or disconnects)
        // If PQ tablet is not able to save a valid config it should kill itself and restart without
        // sending error response
        Y_VERIFY_S(status == NKikimrPQ::OK || status == NKikimrPQ::ERROR_UPDATE_IN_PROGRESS,
                   "Unexpected error in UpdateConfigResponse,"
                       << " status: " << NKikimrPQ::EStatus_Name(status)
                       << " Tx " << OperationId
                       << " tablet "<< tabletId
                       << " at schemeshard: " << ssId);

        if (status == NKikimrPQ::ERROR_UPDATE_IN_PROGRESS) {
            LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "PQ reconfiguration is in progress. We'll try to finish it later."
                            << " Tx " << OperationId
                            << " tablet " << tabletId
                            << " at schemeshard: " << ssId);
            return false;
        }

        Y_VERIFY(txState->State == TTxState::ConfigureParts);

        TShardIdx idx = context.SS->MustGetShardIdx(tabletId);
        txState->ShardsInProgress.erase(idx);

        // Dettach datashard pipe
        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

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
                   DebugHint()
                       << " HandleReply ProgressState"
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

        txState->ClearShardsInProgress();

        TString topicName = context.SS->PathsById.at(txState->TargetPathId)->Name;
        Y_VERIFY_S(topicName.size(),
                   "topicName is empty"
                       <<", pathId: " << txState->TargetPathId);

        TPersQueueGroupInfo::TPtr pqGroup = context.SS->PersQueueGroups[txState->TargetPathId];
        Y_VERIFY_S(pqGroup,
                   "pqGroup is null"
                       << ", pathId " << txState->TargetPathId);

        TString* tabletConfig = &pqGroup->TabletConfig;
        if (pqGroup->AlterData) {
            if (!pqGroup->AlterData->TabletConfig.empty())
                tabletConfig = &pqGroup->AlterData->TabletConfig;
        }

        const TPathElement::TPtr dbRootEl = context.SS->PathsById.at(context.SS->RootPathId());
        TString cloudId;
        if (dbRootEl->UserAttrs->Attrs.contains("cloud_id")) {
            cloudId = dbRootEl->UserAttrs->Attrs.at("cloud_id");
        }
        TString folderId;
        if (dbRootEl->UserAttrs->Attrs.contains("folder_id")) {
           folderId = dbRootEl->UserAttrs->Attrs.at("folder_id");
        }
        TString databaseId;
        if (dbRootEl->UserAttrs->Attrs.contains("database_id")) {
            databaseId = dbRootEl->UserAttrs->Attrs.at("database_id");
        }

        TString databasePath = TPath::Init(context.SS->RootPathId(), context.SS).PathString();

        for (auto shard : txState->Shards) {
            TShardIdx idx = shard.Idx;
            TTabletId tabletId = context.SS->ShardInfos.at(idx).TabletID;

            if (shard.TabletType == ETabletType::PersQueue) {
                TPQShardInfo::TPtr pqShard = pqGroup->Shards.at(idx);
                Y_VERIFY_S(pqShard, "pqShard is null, idx is " << idx << " has was "<< THash<TShardIdx>()(idx));

                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Propose configure PersQueue"
                                << ", opId: " << OperationId
                                << ", tabletId: " << tabletId
                                << ", PQInfos size: " << pqShard->PQInfos.size()
                                << ", at schemeshard: " << ssId);

                TAutoPtr<TEvPersQueue::TEvUpdateConfig> event(new TEvPersQueue::TEvUpdateConfig());
                event->Record.SetTxId(ui64(OperationId.GetTxId()));
                if (!tabletConfig->empty()) {
                    bool parseOk = ParseFromStringNoSizeLimit(*event->Record.MutableTabletConfig(), *tabletConfig);
                    Y_VERIFY(parseOk);
                }
                event->Record.MutableTabletConfig()->SetTopicName(topicName);
                event->Record.MutableTabletConfig()->SetTopicPath(TPath::Init(txState->TargetPathId, context.SS).PathString());
                event->Record.MutableTabletConfig()->MutablePartitionConfig()->SetTotalPartitions(pqGroup->AlterData ? pqGroup->AlterData->TotalGroupCount : pqGroup->TotalGroupCount);

                event->Record.MutableTabletConfig()->SetYdbDatabaseId(databaseId);
                event->Record.MutableTabletConfig()->SetYcCloudId(cloudId);
                event->Record.MutableTabletConfig()->SetYcFolderId(folderId);
                event->Record.MutableTabletConfig()->SetYdbDatabasePath(databasePath);

                event->Record.MutableTabletConfig()->SetVersion(pqGroup->AlterVersion + 1);

                for (const auto& pq : pqShard->PQInfos) {
                    event->Record.MutableTabletConfig()->AddPartitionIds(pq.PqId);

                    auto& partition = *event->Record.MutableTabletConfig()->AddPartitions();
                    partition.SetPartitionId(pq.PqId);
                    if (pq.KeyRange) {
                        pq.KeyRange->SerializeToProto(*partition.MutableKeyRange());
                    }
                }

                if (pqGroup->AlterData && pqGroup->AlterData->BootstrapConfig) {
                    Y_VERIFY(txState->TxType == TTxState::TxCreatePQGroup);
                    const bool ok = ParseFromStringNoSizeLimit(*event->Record.MutableBootstrapConfig(), pqGroup->AlterData->BootstrapConfig);
                    Y_VERIFY(ok);
                }

                context.OnComplete.BindMsgToPipe(OperationId, tabletId, idx, event.Release());
            } else {
                Y_VERIFY(shard.TabletType == ETabletType::PersQueueReadBalancer);

                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Propose configure PersQueueReadBalancer"
                                << ", opId: " << OperationId
                                << ", tabletId: " << tabletId
                                << ", at schemeshard: " << ssId);

                pqGroup->BalancerTabletID = tabletId;
                if (pqGroup->AlterData) {
                    pqGroup->AlterData->BalancerTabletID = tabletId;
                }

                if (pqGroup->AlterData) {
                    pqGroup->AlterData->BalancerShardIdx = idx;
                }

                TAutoPtr<TEvPersQueue::TEvUpdateBalancerConfig> event(new TEvPersQueue::TEvUpdateBalancerConfig());
                event->Record.SetTxId(ui64(OperationId.GetTxId()));

                if (!tabletConfig->empty()) {
                    bool parseOk = ParseFromStringNoSizeLimit(*event->Record.MutableTabletConfig(), *tabletConfig);
                    Y_VERIFY(parseOk);
                }

                Y_VERIFY(pqGroup->AlterData);

                event->Record.SetTopicName(topicName);
                event->Record.SetPathId(txState->TargetPathId.LocalPathId);
                event->Record.SetPath(TPath::Init(txState->TargetPathId, context.SS).PathString());
                event->Record.SetPartitionPerTablet(pqGroup->AlterData ? pqGroup->AlterData->MaxPartsPerTablet : pqGroup->MaxPartsPerTablet);
                event->Record.SetSchemeShardId(context.SS->TabletID());

                event->Record.SetTotalGroupCount(pqGroup->AlterData ? pqGroup->AlterData->TotalGroupCount : pqGroup->TotalGroupCount);
                event->Record.SetNextPartitionId(pqGroup->AlterData ? pqGroup->AlterData->NextPartitionId : pqGroup->NextPartitionId);

                event->Record.SetVersion(pqGroup->AlterVersion + 1);

                for (const auto& p : pqGroup->Shards) {
                    const auto& pqShard = p.second;
                    const auto& tabletId = context.SS->ShardInfos[p.first].TabletID;
                    auto tablet = event->Record.AddTablets();
                    tablet->SetTabletId(ui64(tabletId));
                    tablet->SetOwner(context.SS->TabletID());
                    tablet->SetIdx(ui64(p.first.GetLocalId()));
                    for (const auto& pq : pqShard->PQInfos) {
                        auto info = event->Record.AddPartitions();
                        info->SetPartition(pq.PqId);
                        info->SetTabletId(ui64(tabletId));
                        info->SetGroup(pq.GroupId);
                    }
                }

                context.OnComplete.BindMsgToPipe(OperationId, tabletId, idx, event.Release());
            }
        }

        txState->UpdateShardsInProgress();
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NPQState::TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvPersQueue::TEvUpdateConfigResponse::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint()
                       << " HandleReply TEvOperationPlan"
                       << ", step: " << step
                       << ", at tablet: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if(!txState) {
            return false;
        }
        Y_VERIFY(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        if (txState->TxType == TTxState::TxCreatePQGroup) {
            auto parentDir = context.SS->PathsById.at(path->ParentPathId);
           ++parentDir->DirAlterVersion;
           context.SS->PersistPathDirAlterVersion(db, parentDir);
           context.SS->ClearDescribePathCaches(parentDir);
           context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
        }

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        TPersQueueGroupInfo::TPtr pqGroup = context.SS->PersQueueGroups[pathId];
        pqGroup->FinishAlter();
        context.SS->PersistPersQueueGroup(db, pathId, pqGroup);
        context.SS->PersistRemovePersQueueGroupAlter(db, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "NPQState::TPropose ProgressState"
                       << ", operationId: " << OperationId
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

}

namespace NBSVState {

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "NBSVState::TConfigureParts"
            << " operationId: " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSetConfigResult"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateBlockStoreVolume || txState->TxType == TTxState::TxAlterBlockStoreVolume);
        Y_VERIFY(txState->State == TTxState::ConfigureParts);

        TTabletId tabletId = TTabletId(ev->Get()->Record.GetOrigin());
        NKikimrBlockStore::EStatus status = ev->Get()->Record.GetStatus();

        // Schemeshard never sends invalid or outdated configs
        Y_VERIFY_S(status == NKikimrBlockStore::OK || status == NKikimrBlockStore::ERROR_UPDATE_IN_PROGRESS,
                   "Unexpected error in UpdateVolumeConfigResponse,"
                       << " status " << NKikimrBlockStore::EStatus_Name(status)
                       << " Tx " << OperationId
                       << " tablet " << tabletId);

        if (status == NKikimrBlockStore::ERROR_UPDATE_IN_PROGRESS) {
            LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "BlockStore reconfiguration is in progress. We'll try to finish it later."
                            << " Tx " << OperationId
                            << " tablet " << tabletId);
            return false;
        }

        TShardIdx idx = context.SS->MustGetShardIdx(tabletId);
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
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateBlockStoreVolume || txState->TxType == TTxState::TxAlterBlockStoreVolume);
        Y_VERIFY(!txState->Shards.empty());

        txState->ClearShardsInProgress();

        TBlockStoreVolumeInfo::TPtr volume = context.SS->BlockStoreVolumes[txState->TargetPathId];
        Y_VERIFY_S(volume, "volume is null. PathId: " << txState->TargetPathId);

        ui64 version = volume->AlterVersion;
        const auto* volumeConfig = &volume->VolumeConfig;
        if (volume->AlterData) {
            version = volume->AlterData->AlterVersion;
            volumeConfig = &volume->AlterData->VolumeConfig;
        }

        for (auto shard : txState->Shards) {
            if (shard.TabletType == ETabletType::BlockStorePartition ||
                shard.TabletType == ETabletType::BlockStorePartition2) {
                continue;
            }

            Y_VERIFY(shard.TabletType == ETabletType::BlockStoreVolume);
            TShardIdx shardIdx = shard.Idx;
            TTabletId tabletId = context.SS->ShardInfos[shardIdx].TabletID;

            volume->VolumeTabletId = tabletId;
            if (volume->AlterData) {
                volume->AlterData->VolumeTabletId = tabletId;
                volume->AlterData->VolumeShardIdx = shardIdx;
            }

            TAutoPtr<TEvBlockStore::TEvUpdateVolumeConfig> event(new TEvBlockStore::TEvUpdateVolumeConfig());
            event->Record.SetTxId(ui64(OperationId.GetTxId()));

            event->Record.MutableVolumeConfig()->CopyFrom(*volumeConfig);
            event->Record.MutableVolumeConfig()->SetVersion(version);

            for (const auto& p : volume->Shards) {
                const auto& part = p.second;
                const auto& partTabletId = context.SS->ShardInfos[p.first].TabletID;
                auto info = event->Record.AddPartitions();
                info->SetPartitionId(part->PartitionId);
                info->SetTabletId(ui64(partTabletId));
            }

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
                << "NBSVState::TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvBlockStore::TEvUpdateVolumeConfigResponse::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if (!txState) {
            return false;
        }
        Y_VERIFY(txState->TxType == TTxState::TxCreateBlockStoreVolume || txState->TxType == TTxState::TxAlterBlockStoreVolume);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        TBlockStoreVolumeInfo::TPtr volume = context.SS->BlockStoreVolumes.at(pathId);

        auto oldVolumeSpace = volume->GetVolumeSpace();
        volume->FinishAlter();
        auto newVolumeSpace = volume->GetVolumeSpace();
        // Decrease in occupied space is appled on tx finish
        auto domainDir = context.SS->PathsById.at(context.SS->ResolveDomainId(path));
        Y_VERIFY(domainDir);
        domainDir->ChangeVolumeSpaceCommit(newVolumeSpace, oldVolumeSpace);

        context.SS->PersistBlockStoreVolume(db, pathId, volume);
        context.SS->PersistRemoveBlockStoreVolumeAlter(db, pathId);

        if (txState->TxType == TTxState::TxCreateBlockStoreVolume) {
            auto parentDir = context.SS->PathsById.at(path->ParentPathId);
            ++parentDir->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentDir);
            context.SS->ClearDescribePathCaches(parentDir);
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
        }

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
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxCreateBlockStoreVolume || txState->TxType == TTxState::TxAlterBlockStoreVolume);


        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

}

namespace NCdcStreamState {

class TConfigurePartsAtTable: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "NCdcStreamState::TConfigurePartsAtTable"
            << " operationId: " << OperationId;
    }

    static bool IsExpectedTxType(TTxState::ETxType txType) {
        switch (txType) {
        case TTxState::TxCreateCdcStreamAtTable:
        case TTxState::TxAlterCdcStreamAtTable:
        case TTxState::TxDropCdcStreamAtTable:
            return true;
        default:
            return false;
        }
    }

protected:
    virtual void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const = 0;

public:
    explicit TConfigurePartsAtTable(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        auto* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(IsExpectedTxType(txState->TxType));
        const auto& pathId = txState->TargetPathId;

        if (NTableState::CheckPartitioningChangedForTableModification(*txState, context)) {
            NTableState::UpdatePartitioningForTableModification(OperationId, *txState, context);
        }

        NKikimrTxDataShard::TFlatSchemeTransaction tx;
        context.SS->FillSeqNo(tx, context.SS->StartRound(*txState));
        FillNotice(pathId, tx, context);

        TString txBody;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);

        txState->ClearShardsInProgress();
        Y_VERIFY(txState->Shards.size());

        for (ui32 i = 0; i < txState->Shards.size(); ++i) {
            const auto& idx = txState->Shards[i].Idx;
            const auto datashardId = context.SS->ShardInfos[idx].TabletID;

            auto ev = MakeHolder<TEvDataShard::TEvProposeTransaction>(
                NKikimrTxDataShard::TX_KIND_SCHEME, context.SS->TabletID(), context.Ctx.SelfID,
                ui64(OperationId.GetTxId()), txBody,
                context.SS->SelectProcessingPrarams(pathId)
            );

            context.OnComplete.BindMsgToPipe(OperationId, datashardId, idx, ev.Release());
        }

        txState->UpdateShardsInProgress(TTxState::ConfigureParts);
        return false;
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply " << ev->Get()->ToString()
                               << ", at schemeshard: " << context.SS->TabletID());

        if (!NTableState::CollectProposeTransactionResults(OperationId, ev, context)) {
            return false;
        }

        return true;
    }

private:
    const TOperationId OperationId;

}; // TConfigurePartsAtTable

class TProposeAtTable: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "NCdcStreamState::TProposeAtTable"
            << " operationId: " << OperationId;
    }

    static bool IsExpectedTxType(TTxState::ETxType txType) {
        switch (txType) {
        case TTxState::TxCreateCdcStreamAtTable:
        case TTxState::TxAlterCdcStreamAtTable:
        case TTxState::TxDropCdcStreamAtTable:
            return true;
        default:
            return false;
        }
    }

public:
    explicit TProposeAtTable(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvDataShard::TEvProposeTransactionResult::EventType});
    }

    bool ProgressState(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << context.SS->TabletID());

        const auto* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(IsExpectedTxType(txState->TxType));

        TSet<TTabletId> shardSet;
        for (const auto& shard : txState->Shards) {
            Y_VERIFY(context.SS->ShardInfos.contains(shard.Idx));
            shardSet.insert(context.SS->ShardInfos.at(shard.Idx).TabletID);
        }

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << ev->Get()->StepId
                               << ", at schemeshard: " << context.SS->TabletID());

        const auto* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);
        Y_VERIFY(IsExpectedTxType(txState->TxType));
        const auto& pathId = txState->TargetPathId;

        Y_VERIFY(context.SS->PathsById.contains(pathId));
        auto path = context.SS->PathsById.at(pathId);

        Y_VERIFY(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);

        table->AlterVersion += 1;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistTableAlterVersion(db, pathId, table);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedWaitParts);
        return true;
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " TEvDataShard::TEvSchemaChanged"
                               << " triggers early, save it"
                               << ", at schemeshard: " << context.SS->TabletID());

        NTableState::CollectSchemaChanged(OperationId, ev, context);
        return false;
    }

private:
    const TOperationId OperationId;

}; // TProposeAtTable

} // NCdcStreamState

namespace NForceDrop {
void ValidateNoTrasactionOnPathes(TOperationId operationId, const THashSet<TPathId>& pathes, TOperationContext& context);

void CollectShards(const THashSet<TPathId>& pathes, TOperationId operationId, TTxState* txState, TOperationContext& context);
}

}
}
