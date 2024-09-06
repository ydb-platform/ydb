#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_path_describer.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TWait: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TWaiting"
            << " operationId: " << OperationId;
    }

public:
    TWait(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", operation type: " << TTxState::TypeName(txState->TxType)
                               << ", at tablet" << ssId);

        bool isDone = true;

        // wait all transaction inside
        auto paths = context.SS->ListSubTree(txState->TargetPathId, context.Ctx);
        auto relatedTx = context.SS->GetRelatedTransactions(paths, context.Ctx);
        for (auto otherTxId: relatedTx) {
            if (otherTxId == OperationId.GetTxId()) {
                continue;
            }
            TStringBuilder errMsg;
            errMsg << "TWait ProgressState, dependence has found, but actually it is unexpected"
                   << ", dependent transaction: " << OperationId.GetTxId()
                   << ", parent transaction: " << otherTxId
                   << ", at schemeshard: " << ssId;

            LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, errMsg);
            Y_FAIL_S(errMsg);

            Y_ABORT_UNLESS(context.SS->Operations.contains(otherTxId));
            context.OnComplete.Dependence(otherTxId, OperationId.GetTxId());

            isDone = false;
        }

        if (isDone) {
            NIceDb::TNiceDb db(context.GetDB());

            TPathId pathId = txState->TargetPathId;
            TPathElement::TPtr elem = context.SS->PathsById.at(pathId);
            elem->LastTxId = OperationId.GetTxId();
            context.SS->PersistLastTxId(db, elem);

            context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
            return true;
        }

        return false;
    }
};

class TConfigure: public TSubOperationState {
private:
    TOperationId OperationId;

    TTabletId TenantSchemeShardId = InvalidTabletId;
    THashSet<TPathId> PathsInside;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TConfigure"
            << " operationId: " << OperationId;
    }

public:
    TConfigure(TOperationId id)
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
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxUpgradeSubDomain);
        TPathId pathId = txState->TargetPathId;

        const auto& record = ev->Get()->Record;

        auto tabletId = TTabletId(record.GetTenantSchemeShard());
        auto status = record.GetStatus();

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

        if (status != NKikimrScheme::EStatus::StatusSuccess && status != NKikimrScheme::EStatus::StatusAlreadyExists) {
            LOG_CRIT_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint()
                           << " Got error status on SubDomain Configure"
                           << " from tenant schemeshard tablet: " << tabletId
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

        context.OnComplete.UnbindMsgFromPipe(OperationId, TenantSchemeShardId, pathId);
        PathsInside.erase(pathId);

        auto next = NextMessage(context);
        if (!next) {
            // All tablets have replied so we can done this transaction
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::PublishTenantReadOnly);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        auto nextPathId = next->GetPathId(); //specify pathId lifetime
        context.OnComplete.BindMsgToPipe(OperationId, TenantSchemeShardId, nextPathId, next.Release());

        return false;
    }

    NKikimrScheme::TMigrateShard DescribeShard(TOperationContext& context, const TShardIdx& shardIdx) {
        NKikimrScheme::TMigrateShard descr;
        descr.MutableShardIdx()->SetOwnerId(shardIdx.GetOwnerId());
        descr.MutableShardIdx()->SetLocalId(ui64(shardIdx.GetLocalId()));

        const TShardInfo& info = context.SS->ShardInfos.at(shardIdx);
        descr.SetType(info.TabletType);
        descr.SetTabletId(ui64(info.TabletID));

        for (auto& binding: info.BindedChannels) {
            descr.MutableBindedStoragePool()->Add()->CopyFrom(binding);
        }

        return descr;
    }

    NKikimrScheme::TMigratePath DescribePath(TOperationContext& context, TPathId pathId) {
        NKikimrScheme::TMigratePath descr;
        descr.MutablePathId()->SetOwnerId(pathId.OwnerId);
        descr.MutablePathId()->SetLocalId(pathId.LocalPathId);

        TPathElement::TPtr elem = context.SS->PathsById.at(pathId);
        descr.MutableParentPathId()->SetOwnerId(elem->ParentPathId.OwnerId);
        descr.MutableParentPathId()->SetLocalId(elem->ParentPathId.LocalPathId);
        descr.SetName(elem->Name);
        descr.SetPathType(elem->PathType);
        descr.SetStepCreated(ui64(elem->StepCreated));
        descr.SetCreateTxId(ui64(elem->CreateTxId));
        descr.SetOwner(elem->Owner);
        descr.SetACL(elem->ACL);
        descr.SetDirAlterVersion(elem->DirAlterVersion);
        descr.SetUserAttrsAlterVersion(elem->UserAttrs->AlterVersion);
        descr.SetACLVersion(elem->ACLVersion);

        for (auto& item: elem->UserAttrs->Attrs) {
            auto add = descr.AddUserAttributes();
            add->SetKey(item.first);
            add->SetValue(item.second);
        }

        return descr;
    }

    NKikimrScheme::TMigrateTable DescribeTable(TOperationContext& context, TPathId pathId) {
        NKikimrScheme::TMigrateTable descr;

        TTableInfo::TPtr tableInfo = context.SS->Tables.at(pathId);
        descr.SetNextColId(tableInfo->NextColumnId);

        TString partitionConfig;
        Y_PROTOBUF_SUPPRESS_NODISCARD tableInfo->PartitionConfig().SerializeToString(&partitionConfig);
        descr.SetPartitionConfig(partitionConfig);

        descr.SetAlterVersion(tableInfo->AlterVersion);
        descr.SetPartitioningVersion(tableInfo->PartitioningVersion);

        for (auto& item: tableInfo->Columns) {
            ui32 columnId = item.first;
            const TTableInfo::TColumn& column = item.second;

            auto colDescr = descr.AddColumns();
            colDescr->SetId(columnId);
            colDescr->SetName(column.Name);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.PType, column.PTypeMod);
            colDescr->SetColType(columnType.TypeId);
            if (columnType.TypeInfo) {
                *colDescr->MutableColTypeInfo() = *columnType.TypeInfo;
            }
            colDescr->SetColKeyOrder(column.KeyOrder);
            colDescr->SetCreateVersion(column.CreateVersion);
            colDescr->SetDeleteVersion(column.DeleteVersion);
            colDescr->SetFamily(column.Family);
            colDescr->SetNotNull(column.NotNull);
            colDescr->SetIsBuildInProgress(column.IsBuildInProgress);
            if (column.DefaultKind != ETableColumnDefaultKind::None) {
                colDescr->SetDefaultKind(ui32(column.DefaultKind));
                colDescr->SetDefaultValue(column.DefaultValue);
            }
        }

        for (ui32 partNum = 0; partNum < tableInfo->GetPartitions().size(); ++partNum) {
            const TTableShardInfo& partition = tableInfo->GetPartitions().at(partNum);

            auto partDescr = descr.AddPartitions();
            partDescr->SetId(partNum);
            partDescr->SetRangeEnd(partition.EndOfRange);
            partDescr->MutableShardIdx()->SetOwnerId(partition.ShardIdx.GetOwnerId());
            partDescr->MutableShardIdx()->SetLocalId(ui64(partition.ShardIdx.GetLocalId()));
            if (tableInfo->PerShardPartitionConfig.contains(partition.ShardIdx)) {
                TString partitionConfig;
                Y_PROTOBUF_SUPPRESS_NODISCARD tableInfo->PerShardPartitionConfig.at(partition.ShardIdx).SerializeToString(&partitionConfig);
                partDescr->SetPartitionConfig(partitionConfig);
            }
        }

        return descr;
    }

    NKikimrScheme::TMigrateTableIndex DescribeTableIndex(TOperationContext& context, TPathId pathId) {
        NKikimrScheme::TMigrateTableIndex descr;

        TTableIndexInfo::TPtr indexInfo = context.SS->Indexes.at(pathId);
        descr.SetAlterVersion(indexInfo->AlterVersion);
        descr.SetType(indexInfo->Type);
        descr.SetState(indexInfo->State);

        for (auto& keyName: indexInfo->IndexKeys) {
            descr.AddKeys(keyName);
        }

        return descr;
    }

    NKikimrScheme::TMigrateKesus DescribeKesus(TOperationContext& context, TPathId pathId) {
        NKikimrScheme::TMigrateKesus descr;

        TKesusInfo::TPtr kesusInfo = context.SS->KesusInfos.at(pathId);

        descr.MutablePathId()->SetOwnerId(pathId.OwnerId);
        descr.MutablePathId()->SetLocalId(pathId.LocalPathId);
        descr.SetVersion(kesusInfo->Version);

        TString config;
        Y_PROTOBUF_SUPPRESS_NODISCARD kesusInfo->Config.SerializeToString(&config);
        descr.SetConfig(config);

        return descr;
    }

    THolder<TEvSchemeShard::TEvMigrateSchemeShard> NextMessage(TOperationContext& context) {
        if (!PathsInside) {
            return nullptr;
        }

        TPathId pathId = *PathsInside.begin();
        TPath path = TPath::Init(pathId, context.SS);


        auto event = MakeHolder<TEvSchemeShard::TEvMigrateSchemeShard>();
        event->Record.SetSchemeShardGeneration(context.SS->Generation());

        *event->Record.MutablePath() = DescribePath(context, pathId);
        *event->Record.MutablePathVersion() = context.SS->GetPathVersion(path);

        auto migrateShards = event->Record.MutableShards();
        switch (path.Base()->PathType) {
            case NKikimrSchemeOp::EPathType::EPathTypeDir:
            case NKikimrSchemeOp::EPathType::EPathTypeExternalTable:
            case NKikimrSchemeOp::EPathType::EPathTypeExternalDataSource:
            case NKikimrSchemeOp::EPathType::EPathTypeView:
            case NKikimrSchemeOp::EPathType::EPathTypeResourcePool:
                Y_ABORT_UNLESS(!path.Base()->IsRoot());
                //no shards
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeSubDomain:
            case NKikimrSchemeOp::EPathType::EPathTypeExtSubDomain:
                Y_ABORT("impossible to migrate subDomain or extSubDomain as part of the other subDomain");
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeTable:
            {
                Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));

                *event->Record.MutableTable() = DescribeTable(context, pathId);

                TTableInfo::TPtr tableInfo = context.SS->Tables.at(pathId);
                for (auto part: tableInfo->GetPartitions()) {
                    TShardIdx shardIdx = part.ShardIdx;
                    *migrateShards->Add() = DescribeShard(context, shardIdx);
                }

                //tinfo->PerShardPartitionConfig add that data
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeTableIndex:
            {
                Y_ABORT_UNLESS(context.SS->Indexes.contains(pathId));
                *event->Record.MutableTableIndex() = DescribeTableIndex(context, pathId);

                //no shards

                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeKesus:
            {
                Y_ABORT_UNLESS(context.SS->KesusInfos.contains(pathId));
                *event->Record.MutableKesus() = DescribeKesus(context, pathId);

                TKesusInfo::TPtr kesusInfo = context.SS->KesusInfos.at(pathId);
                Y_ABORT_UNLESS(kesusInfo->KesusShardIdx);
                Y_ABORT_UNLESS(context.SS->ShardInfos.contains(kesusInfo->KesusShardIdx));
                *migrateShards->Add() = DescribeShard(context, kesusInfo->KesusShardIdx);

                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypePersQueueGroup:
            case NKikimrSchemeOp::EPathType::EPathTypeBlockStoreVolume:
            case NKikimrSchemeOp::EPathType::EPathTypeFileStore:
            case NKikimrSchemeOp::EPathType::EPathTypeRtmrVolume:
            case NKikimrSchemeOp::EPathType::EPathTypeSolomonVolume:
            case NKikimrSchemeOp::EPathType::EPathTypeColumnStore:
            case NKikimrSchemeOp::EPathType::EPathTypeColumnTable:
            case NKikimrSchemeOp::EPathType::EPathTypeCdcStream:
            case NKikimrSchemeOp::EPathType::EPathTypeSequence:
            case NKikimrSchemeOp::EPathType::EPathTypeReplication:
            case NKikimrSchemeOp::EPathType::EPathTypeBlobDepot:
                Y_ABORT("UNIMPLEMENTED");
            case NKikimrSchemeOp::EPathType::EPathTypeInvalid:
                Y_UNREACHABLE();
        }

        return event;
    }

    bool HandleReply(TEvSchemeShard::TEvMigrateSchemeShardResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSchemeShard::TEvMigrateSchemeShardResult"
                               << ", at tablet" << ssId);

        Y_ABORT_UNLESS(ev->Get()->GetPathId().OwnerId == context.SS->TabletID());

        TPathId pathId = ev->Get()->GetPathId();
        context.OnComplete.UnbindMsgFromPipe(OperationId, TenantSchemeShardId, pathId);
        PathsInside.erase(pathId);

        auto next = NextMessage(context);
        if (!next) {
            // All tablets have replied so we can done this transaction
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::PublishTenantReadOnly);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        auto nexPathId = next->GetPathId(); //specify pathId lifetime
        context.OnComplete.BindMsgToPipe(OperationId, TenantSchemeShardId, nexPathId, next.Release());

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxUpgradeSubDomain);

        TPathId pathId = txState->TargetPathId;
        TPath path = TPath::Init(pathId, context.SS);

        auto subDomain = context.SS->SubDomains.at(pathId);
        auto alterData = subDomain->GetAlter();
        Y_ABORT_UNLESS(alterData);
        alterData->Initialize(context.SS->ShardInfos);

        auto processing = alterData->GetProcessingParams();
        auto storagePools = alterData->GetStoragePools();
        auto& schemeLimits = subDomain->GetSchemeLimits();

        auto event = new TEvSchemeShard::TEvInitTenantSchemeShard(
            ui64(ssId),
            pathId.LocalPathId, path.PathString(),
            path.Base()->Owner, path.GetEffectiveACL(), path.GetEffectiveACLVersion(),
            processing, storagePools,
            path.Base()->UserAttrs->Attrs, path.Base()->UserAttrs->AlterVersion,
            schemeLimits, ui64(InvalidTabletId));
        event->Record.SetInitiateMigration(true);

        Y_ABORT_UNLESS(1 == txState->Shards.size());
        auto &shard = *txState->Shards.begin();
        shard.Operation = TTxState::ConfigureParts;

        TenantSchemeShardId = TTabletId(processing.GetSchemeShard());
        PathsInside = context.SS->ListSubTree(path.Base()->PathId, context.Ctx);

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint()
                        << "Send configure request to schemeshard: " << TenantSchemeShardId
                        << " schemeshard: " << ssId
                        << " msg: " << event->Record.ShortDebugString());
        context.OnComplete.BindMsgToPipe(OperationId, TenantSchemeShardId, pathId, event);


        return false;
    }
};

class TPublishTenantReadOnly: public TSubOperationState {
private:
    TOperationId OperationId;
    TTabletId TenantSchemeShardId = InvalidTabletId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TPublishTenantReadOnly"
            << " operationId: " << OperationId;
    }

public:
    TPublishTenantReadOnly(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType,
                                     TEvSchemeShard::TEvMigrateSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvInitTenantSchemeShardResult::EventType});
    }

    bool HandleReply(TEvSchemeShard::TEvPublishTenantAsReadOnlyResult::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvSchemeShard::TEvPublishTenantAsReadOnlyResult"
                               << ", at tablet" << ssId);

        Y_ABORT_UNLESS(TTabletId(ev->Get()->Record.GetTenantSchemeShard()) == TenantSchemeShardId);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::PublishGlobal);
        context.OnComplete.ActivateTx(OperationId);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        Y_ABORT_UNLESS(1 == txState->Shards.size());
        auto &shard = *txState->Shards.begin();
        shard.Operation = TTxState::ConfigureParts;
        txState->UpdateShardsInProgress();

        TPathId pathId = txState->TargetPathId;
        auto subDomain = context.SS->SubDomains.at(pathId);
        auto alterData = subDomain->GetAlter();
        Y_ABORT_UNLESS(alterData);
        alterData->Initialize(context.SS->ShardInfos);
        auto processing = alterData->GetProcessingParams();
        TenantSchemeShardId = TTabletId(processing.GetSchemeShard());

        auto event = new TEvSchemeShard::TEvPublishTenantAsReadOnly(ui64(ssId));

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint()
                        << " Send publish as RO request to schemeshard: " << TenantSchemeShardId
                        << " schemeshard: " << ssId
                        << " msg: " << event->Record.ShortDebugString());
        context.OnComplete.BindMsgToPipe(OperationId, TenantSchemeShardId, pathId, event);

        return false;
    }
};

class TPublishGlobal: public TSubOperationState {
private:
    TOperationId OperationId;
    TTxState::ETxState& UpgradeDecision;

    TPathElement::TChildrenCont HiddenChildren;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TPublishGlobal"
            << " operationId: " << OperationId;
    }

public:
    TPublishGlobal(TOperationId id, TTxState::ETxState& nextState)
        : OperationId(id)
        , UpgradeDecision(nextState)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType,
                                     TEvSchemeShard::TEvMigrateSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvInitTenantSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvPublishTenantAsReadOnlyResult::EventType});
    }

    bool HandleReply(TEvPrivate::TEvCommitTenantUpdate::TPtr& /*ev*/, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvCommitTenantUpdate"
                               << ", at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        TPathId pathId = txState->TargetPathId;
        auto path = context.SS->PathsById.at(pathId);

        path->SwapChildren(HiddenChildren); //return back children, now we do not pretend that there no children, we define them as Migrated
        auto pathsInside = context.SS->ListSubTree(pathId, context.Ctx);
        pathsInside.erase(pathId);
        for (auto pId: pathsInside) {
            auto item = context.SS->PathsById.at(pId);

            context.SS->MarkAsMigrated(item, context.Ctx);
            context.SS->ClearDescribePathCaches(item); //something has changed let's show it
        }

        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(path->PathType == TPathElement::EPathType::EPathTypeExtSubDomain);
        context.SS->PersistPath(db, path->PathId);
        context.SS->ClearDescribePathCaches(path);

        auto subDomain = context.SS->SubDomains.at(pathId);
        subDomain->SetAlterPrivate(nullptr);
        context.SS->PersistSubDomain(db, pathId, *subDomain);
        context.SS->PersistSubDomainSchemeQuotas(db, pathId, *subDomain);

        context.SS->ChangeTxState(db, OperationId, TTxState::RewriteOwners);
        context.OnComplete.ActivateTx(OperationId);
        UpgradeDecision = TTxState::RewriteOwners;

        return true;
    }

    bool HandleReply(TEvPrivate::TEvUndoTenantUpdate::TPtr& /*ev*/, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvUndoTenantUpdate"
                               << ", at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        NIceDb::TNiceDb db(context.GetDB());
        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr item = context.SS->PathsById.at(pathId);

        Y_ABORT_UNLESS(item->PathType == TPathElement::EPathType::EPathTypeExtSubDomain);
        item->PathType = TPathElement::EPathType::EPathTypeSubDomain;

        item->SwapChildren(HiddenChildren); //return back children
        item->PreSerializedChildrenListing.clear();

        auto subDomain = context.SS->SubDomains.at(pathId);
        Y_ABORT_UNLESS(subDomain);
        auto alterData = subDomain->GetAlter();
        Y_ABORT_UNLESS(alterData);
        alterData->Initialize(context.SS->ShardInfos);
        subDomain->ActualizeAlterData(context.SS->ShardInfos, context.Ctx.Now(), /* isExternal */ false, context.SS);

        context.SS->RevertedMigrations[pathId].push_back(subDomain->GetTenantSchemeShardID());
        context.SS->PersistRevertedMigration(db, pathId, subDomain->GetTenantSchemeShardID());

        alterData->SetAlterPrivate(nullptr);
        subDomain->SetAlterPrivate(nullptr);

        alterData->SetVersion(alterData->GetVersion() + 1);
        context.SS->SubDomains[pathId] = alterData;

        context.SS->PersistSubDomainVersion(db, pathId, *alterData);
        context.SS->PersistSubDomainSchemeQuotas(db, pathId, *alterData);
        context.SS->PersistDeleteSubDomainAlter(db, pathId, *subDomain);

        item->ACLVersion += 100;
        context.SS->PersistACL(db, item);
        context.SS->ClearDescribePathCaches(item);

        auto subtree = context.SS->ListSubTree(pathId, context.Ctx);
        for (const TPathId pathId : subtree) {
            context.OnComplete.RePublishToSchemeBoard(OperationId, pathId);
        }

        TPathElement::TPtr parentItem = context.SS->PathsById.at(item->ParentPathId);
        ++parentItem->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentItem);
        context.SS->ClearDescribePathCaches(parentItem);
        context.OnComplete.RePublishToSchemeBoard(OperationId, item->ParentPathId);

        context.SS->ChangeTxState(db, OperationId, TTxState::DeleteTenantSS);
        context.OnComplete.ActivateTx(OperationId);

        UpgradeDecision = TTxState::DeleteTenantSS;
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet" << ssId);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr item = context.SS->PathsById.at(pathId);

        item->PathType = TPathElement::EPathType::EPathTypeExtSubDomain;

        auto subDomain = context.SS->SubDomains.at(pathId);
        auto alterData = subDomain->GetAlter();
        Y_ABORT_UNLESS(alterData);
        Y_ABORT_UNLESS(subDomain->GetVersion() < alterData->GetVersion());

        alterData->Initialize(context.SS->ShardInfos);
        subDomain->ActualizeAlterData(context.SS->ShardInfos, context.Ctx.Now(), /* isExternal */ true, context.SS);

        alterData->SetAlterPrivate(subDomain);
        subDomain->SetAlterPrivate(nullptr);
        context.SS->SubDomains[pathId] = alterData;

        item->SwapChildren(HiddenChildren);
        item->PreSerializedChildrenListing.clear();

        TPathElement::TPtr parentItem = context.SS->PathsById.at(item->ParentPathId);
        ++parentItem->DirAlterVersion;
        context.SS->ClearDescribePathCaches(parentItem);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentItem->PathId);

        context.SS->ClearDescribePathCaches(item);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        context.OnComplete.ReadyToNotify(OperationId);

        return false;
    }
};

class TDeleteTenantSS: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDeleteTenantSS"
            << " operationId: " << OperationId;
    }

public:
    TDeleteTenantSS(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType,
                                     TEvSchemeShard::TEvMigrateSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvInitTenantSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvPublishTenantAsReadOnlyResult::EventType,
                                     TEvSchemeShard::TEvRewriteOwnerResult::EventType,
                                     TEvSchemeShard::TEvPublishTenantResult::EventType});
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        NIceDb::TNiceDb db(context.GetDB());

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        TShardIdx tenantSchemeShardIdx = txState->Shards.front().Idx;

        context.OnComplete.DeleteShard(tenantSchemeShardIdx);

        TPathId pathId = txState->TargetPathId;
        auto subDomain = context.SS->SubDomains.at(pathId);
        Y_ABORT_UNLESS(subDomain);
        auto alterData = subDomain->GetAlter();
        Y_ABORT_UNLESS(!alterData);

        context.OnComplete.ReleasePathState(OperationId, pathId, TPathElement::EPathState::EPathStateAlter);
        context.OnComplete.DoneOperation(OperationId);
        return true;
    }
};

class TRewriteOwner: public TSubOperationState {
private:
    TOperationId OperationId;

    TTabletId TenantSchemeShardId = InvalidTabletId;
    TSet<TTabletId> DatashardsInside;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TRewriteOwner"
            << " operationId: " << OperationId;
    }

public:
    TRewriteOwner(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType,
                                     TEvSchemeShard::TEvMigrateSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvInitTenantSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvPublishTenantAsReadOnlyResult::EventType});
    }

    bool HandleReply(TEvDataShard::TEvMigrateSchemeShardResponse::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        const NKikimrTxDataShard::TEvMigrateSchemeShardResponse& record = ev->Get()->Record;

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvDataShard::TEvMigrateSchemeShardResponse"
                               << ", at tablet" << ssId);

        auto dataShardId = TTabletId(record.GetTabletId());
        auto status = record.GetStatus();

        if (!DatashardsInside.contains(dataShardId)) {
            return false;
        }

        Y_ABORT_UNLESS(status == NKikimrTxDataShard::TEvMigrateSchemeShardResponse::Success
                 || status == NKikimrTxDataShard::TEvMigrateSchemeShardResponse::Already);

        DatashardsInside.erase(dataShardId);
        context.OnComplete.UnbindMsgFromPipe(OperationId, dataShardId, TPipeMessageId(0, 0));

        auto nextEvent = NextRequest(context);

        if (!nextEvent) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::PublishTenant);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint()
                        << " Send next migrate schemeshard event to datashard"
                        << " at schemeshard: " << ssId
                        << " msg: " << nextEvent->Record.ShortDebugString());

        auto nextDataShardId = TTabletId(nextEvent->GetDatashardId());
        context.OnComplete.BindMsgToPipe(OperationId, nextDataShardId, TPipeMessageId(0, 0), nextEvent.Release());

        return false;
    }


    THolder<TEvDataShard::TEvMigrateSchemeShardRequest> NextRequest(TOperationContext& context) {
        if (!DatashardsInside) {
            return nullptr;
        }

        TTabletId tabletId = *DatashardsInside.begin();

        auto ev = MakeHolder<TEvDataShard::TEvMigrateSchemeShardRequest>();
        ev->Record.SetCurrentSchemeShardId(ui64(context.SS->SelfTabletId()));
        ev->Record.SetNewSchemeShardId(ui64(TenantSchemeShardId));
        ev->Record.SetTabletId(ui64(tabletId));

        return ev;
    }


    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet" << ssId);

        TPathId targetPathId = txState->TargetPathId;

        auto subDomain = context.SS->SubDomains.at(targetPathId);
        TenantSchemeShardId = subDomain->GetTenantSchemeShardID();
        Y_ABORT_UNLESS(TenantSchemeShardId);

        auto pathsInside = context.SS->ListSubTree(targetPathId, context.Ctx);
        pathsInside.erase(targetPathId);
        for (auto pId: pathsInside) {
            TPathElement::TPtr item = context.SS->PathsById.at(pId);

            switch (item->PathType) {
                case NKikimrSchemeOp::EPathType::EPathTypeDir:
                    // no shards
                    break;
                case NKikimrSchemeOp::EPathType::EPathTypeTable:
                {
                    Y_ABORT_UNLESS(context.SS->Tables.contains(pId));
                    TTableInfo::TPtr table = context.SS->Tables.at(pId);
                    for (auto item: table->GetPartitions()) {
                        auto shardIdx = item.ShardIdx;
                        const auto& shardInfo = context.SS->ShardInfos.at(shardIdx);

                        bool inserted = false;
                        std::tie(std::ignore, inserted) = DatashardsInside.insert(shardInfo.TabletID);
                        Y_ABORT_UNLESS(inserted);
                    }
                    break;
                }
                case NKikimrSchemeOp::EPathType::EPathTypeTableIndex:
                    // no shards
                    break;
                case NKikimrSchemeOp::EPathType::EPathTypeKesus:
                    // the is one shard, but it's owner shouldn't be rewritten
                    break;
                default:
                    Y_FAIL_S("Not implemented for " << NKikimrSchemeOp::EPathType_Name(item->PathType));
            };
        }

        auto event = NextRequest(context);

        if (!event) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::PublishTenant);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint()
                        << " Send migrate schemeshard event to datashard"
                        << " at schemeshard: " << ssId
                        << " msg: " << event->Record.ShortDebugString());

        auto dataShardId = TTabletId(event->GetDatashardId());
        context.OnComplete.BindMsgToPipe(OperationId, dataShardId, TPipeMessageId(0, 0), event.Release());

        return false;
    }
};

class TPublishTenant: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TPublishTenant"
            << " operationId: " << OperationId;
    }

public:
    TPublishTenant(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType,
                                     TEvSchemeShard::TEvMigrateSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvInitTenantSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvPublishTenantAsReadOnlyResult::EventType,
                                     TEvSchemeShard::TEvRewriteOwnerResult::EventType});
    }

    bool HandleReply(TEvSchemeShard::TEvPublishTenantResult::TPtr& /*ev*/, TOperationContext& context) override {
        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::DoneMigrateTree);
        context.OnComplete.ActivateTx(OperationId);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet" << ssId);

        TPathId pathId = txState->TargetPathId;

        auto event = new TEvSchemeShard::TEvPublishTenant(ui64(ssId));

        auto subDomain = context.SS->SubDomains.at(pathId);
        auto tenantSchemeShardId = TTabletId(subDomain->GetProcessingParams().GetSchemeShard());

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint()
                        << "Send publish request to schemeshard: " << tenantSchemeShardId
                        << " schemeshard: " << ssId
                        << " msg: " << event->Record.ShortDebugString());
        context.OnComplete.BindMsgToPipe(OperationId, tenantSchemeShardId, pathId, event);

        return false;
    }
};

class TDoneMigrateTree: public TSubOperationState {
private:
    TOperationId OperationId;

    //we do not forget any shards. We leave them until database life. And we ensure it deleted at the removing database
    //but hang up them to the domain path
    TDeque<TShardIdx> ShardsToRemember;

    bool IsInited = false;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDoneMigrateTree"
            << " operationId: " << OperationId;
    }

public:
    TDoneMigrateTree(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType,
                                     TEvSchemeShard::TEvMigrateSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvInitTenantSchemeShardResult::EventType,
                                     TEvSchemeShard::TEvPublishTenantAsReadOnlyResult::EventType,
                                     TEvSchemeShard::TEvRewriteOwnerResult::EventType,
                                     TEvSchemeShard::TEvPublishTenantResult::EventType});
    }

    void Init(TPathId pathId, TOperationContext& context) {
        if (IsInited) {
            return;
        }
        IsInited = true;

        auto paths = context.SS->ListSubTree(pathId, context.Ctx);
        paths.erase(pathId);

        auto shards = context.SS->CollectAllShards(paths);
        ShardsToRemember.assign(shards.begin(), shards.end());
    }

    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        Init(txState->TargetPathId, context);

        NIceDb::TNiceDb db(context.GetDB());

        if (!ShardsToRemember.empty()) {
            const auto shardIdx = ShardsToRemember.back();
            TShardInfo& shardInfo = context.SS->ShardInfos.at(shardIdx);

            context.SS->IncrementPathDbRefCount(txState->TargetPathId);
            context.SS->DecrementPathDbRefCount(shardInfo.PathId);

            shardInfo.PathId = txState->TargetPathId;
            db.Table<Schema::SubDomainShards>().Key(txState->TargetPathId.LocalPathId, shardIdx.GetLocalId()).Update();
            db.Table<Schema::Shards>().Key(shardIdx.GetLocalId()).Update(
                NIceDb::TUpdate<Schema::Shards::PathId>(txState->TargetPathId.LocalPathId));

            shardInfo.CurrentTxId = OperationId.GetTxId();
            context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());

            TPathElement::TPtr domainPath = context.SS->PathsById.at(txState->TargetPathId);
            domainPath->IncShardsInside();
            TSubDomainInfo::TPtr domainInfo = context.SS->ResolveDomainInfo(txState->TargetPathId);
            domainInfo->AddPrivateShard(shardIdx);


            ShardsToRemember.pop_back();
            context.OnComplete.ActivateTx(OperationId);

            return false;
        }

        TPathId pathId = txState->TargetPathId;

        auto path = context.SS->PathsById.at(pathId);
        ++path->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, path);
        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.RePublishToSchemeBoard(OperationId, pathId);

        auto parentPath = context.SS->PathsById.at(path->ParentPathId);
        ++parentPath->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath);
        context.SS->ClearDescribePathCaches(parentPath);
        context.OnComplete.RePublishToSchemeBoard(OperationId, parentPath->PathId);

        context.OnComplete.ReleasePathState(OperationId, pathId, TPathElement::EPathState::EPathStateAlter);
        context.OnComplete.DoneOperation(OperationId);

        context.OnComplete.UpdateTenant(txState->TargetPathId);

        return true;
    }
};

class TUpgradeSubDomain: public TSubOperation {
    TTxState::ETxState UpgradeSubDomainDecision = TTxState::Invalid;

    static TTxState::ETxState NextState() {
        return TTxState::Waiting;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
            return TTxState::CreateParts;
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::PublishTenantReadOnly;
        case TTxState::PublishTenantReadOnly:
            return TTxState::PublishGlobal;
        case TTxState::PublishGlobal:
            return UpgradeSubDomainDecision;
        case TTxState::RewriteOwners:
            return TTxState::PublishTenant;
        case TTxState::PublishTenant:
            return TTxState::DoneMigrateTree;
        case TTxState::DeleteTenantSS:
            return TTxState::Invalid;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
            return MakeHolder<TWait>(OperationId);
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigure>(OperationId);
        case TTxState::PublishTenantReadOnly:
            return MakeHolder<TPublishTenantReadOnly>(OperationId);
        case TTxState::PublishGlobal:
            return MakeHolder<TPublishGlobal>(OperationId, UpgradeSubDomainDecision);
        case TTxState::RewriteOwners:
            return MakeHolder<TRewriteOwner>(OperationId);
        case TTxState::PublishTenant:
            return MakeHolder<TPublishTenant>(OperationId);
        case TTxState::DoneMigrateTree:
            return MakeHolder<TDoneMigrateTree>(OperationId);
        case TTxState::DeleteTenantSS:
            return MakeHolder<TDeleteTenantSS>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& info = Transaction.GetUpgradeSubDomain();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = info.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TUpgradeSubDomain Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));
        TString errStr;

        TPath parentPath = TPath::Resolve(parentPathStr, context.SS);
        TPath path = parentPath.Child(name);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted()
                .IsSubDomain()
                .NotUnderOperation()
                .ShardsLimit(1);

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        auto pathId = path.Base()->PathId;

        Y_ABORT_UNLESS(context.SS->SubDomains.contains(pathId));
        TSubDomainInfo::TPtr subDomain = context.SS->SubDomains.at(pathId);
        if (!subDomain->IsSupportTransactions()) {
            result->SetError(NKikimrScheme::StatusSchemeError, "There are no sense to upgrade subdomain with out transactions support (NBS?).");
            return result;
        }

        TChannelsBindings channelBindings;
        if (!context.SS->ResolveSubdomainsChannels(subDomain->GetStoragePools(), channelBindings)) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Unable construct channels binding");
            return result;
        }

        const THashSet<TPathElement::EPathType> allowedPathsToUpgrade = {
            TPathElement::EPathType::EPathTypeDir,
            TPathElement::EPathType::EPathTypeTable,
            TPathElement::EPathType::EPathTypeTableIndex,
            TPathElement::EPathType::EPathTypeKesus
        };

        auto pathsInside = context.SS->ListSubTree(pathId, context.Ctx);
        for (auto pId: pathsInside) {
            if (pId == pathId) {
                continue;
            }

            auto pElem = context.SS->PathsById.at(pId);
            if (allowedPathsToUpgrade.contains(pElem->PathType)) {
                continue;
            }

            TString msg = TStringBuilder() << "Unable to upgrade subdomain"
                                           << ", path type " << NKikimrSchemeOp::EPathType_Name(pElem->PathType) << " is forbidden to migrate"
                                           << ", pathId: " << pId;
            result->SetError(NKikimrScheme::StatusPreconditionFailed, msg);
            return result;
        }

        for (auto pId: pathsInside) {
            if (!context.SS->LockedPaths.contains(pId)) {
                continue;
            }

            auto pElem = context.SS->PathsById.at(pId);
            TString msg = TStringBuilder() << "Unable to upgrade subdomain"
                                           << "path under lock has been found"
                                           << ", path id: " << pId
                                           << ", path type: " << NKikimrSchemeOp::EPathType_Name(pElem->PathType)
                                           << ", path state: " << NKikimrSchemeOp::EPathState_Name(pElem->PathState)
                                           << ", locked by: " << context.SS->LockedPaths.at(pId);
            result->SetError(NKikimrScheme::StatusMultipleModifications, msg);
            return result;
        }

        TSubDomainInfo::TPtr alterData = new TSubDomainInfo(*subDomain,
                                                            subDomain->GetPlanResolution(),
                                                            subDomain->GetTCB());

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxUpgradeSubDomain, pathId);
        txState.State = TTxState::Waiting;
        path.Base()->PathState = TPathElement::EPathState::EPathStateUpgrade;

        // add tenant schemeshard into tx shards
        const TShardIdx shardIdx = context.SS->RegisterShardInfo(
            TShardInfo(OperationId.GetTxId(), pathId, TTabletTypes::SchemeShard)
                .WithBindedChannels(channelBindings));
        const TShardInfo& shardInfo = context.SS->ShardInfos.at(shardIdx);
        txState.Shards.emplace_back(shardIdx, TTabletTypes::SchemeShard, TTxState::CreateParts);
        alterData->AddPrivateShard(shardIdx);
        subDomain->SetAlter(alterData);


        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistUpdateNextShardIdx(db);

        context.SS->PersistTxState(db, OperationId);
        context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());

        context.SS->PersistShardMapping(db, shardIdx, InvalidTabletId, shardInfo.PathId, shardInfo.CurrentTxId, shardInfo.TabletType);
        context.SS->PersistChannelsBinding(db, shardIdx, shardInfo.BindedChannels);

        context.SS->PersistSubDomainAlter(db, pathId, *alterData);

        // wait all transaction inside
        auto relatedTx = context.SS->GetRelatedTransactions(pathsInside, context.Ctx);
        for (auto otherTxId: relatedTx) {
            if (otherTxId == OperationId.GetTxId()) {
                continue;
            }
            LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TUpgradeSubDomain Propose, dependence has found"
                             << ", dependent transaction: " << OperationId.GetTxId()
                             << ", parent transaction: " << otherTxId
                             << ", at schemeshard: " << ssId);

            Y_ABORT_UNLESS(context.SS->Operations.contains(otherTxId));
            context.OnComplete.Dependence(otherTxId, OperationId.GetTxId());
        }

        path.DomainInfo()->AddInternalShards(txState);
        path.Base()->IncShardsInside();

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TUpgradeSubDomain");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TUpgradeSubDomain AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        TTxState* upgradeState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(upgradeState);
        Y_ABORT_UNLESS(upgradeState->TxType == TTxState::TxUpgradeSubDomain);

        if (upgradeState->State == TTxState::PublishGlobal) {
            Y_ABORT_UNLESS(context.SS->Operations.contains(OperationId.GetTxId()));
            TOperation::TPtr operation = context.SS->Operations.at(OperationId.GetTxId());
            Y_ABORT_UNLESS(operation->Parts.size());

            THolder<TEvPrivate::TEvUndoTenantUpdate> msg = MakeHolder<TEvPrivate::TEvUndoTenantUpdate>();
            TEvPrivate::TEvUndoTenantUpdate::TPtr personalEv = (TEventHandle<TEvPrivate::TEvUndoTenantUpdate>*) new IEventHandle(
                context.SS->SelfId(), context.SS->SelfId(), msg.Release());
            operation->Parts.front()->HandleReply(personalEv, context);
        }

        context.OnComplete.DoneOperation(OperationId);
    }
};

class TDecisionDone: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TDecisionDone operationId#" << OperationId;
    }

public:
    TDecisionDone(TOperationId id)
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
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        Y_VERIFY_S(path->PathState == TPathElement::EPathState::EPathStateAlter,
                   "with context"
                       << ", PathState: " << NKikimrSchemeOp::EPathState_Name(path->PathState)
                       << ", PathId: " << path->PathId
                       << ", PathName: " << path->Name);

        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        context.OnComplete.ReleasePathState(OperationId, pathId, TPathElement::EPathState::EPathStateNoChanges);

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }
};

class TUpgradeSubDomainDecision: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Done;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Done:
            return MakeHolder<TDecisionDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& info = Transaction.GetUpgradeSubDomain();
        auto decision = info.GetDecision();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = info.GetName();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TUpgradeSubDomainDecision Propose "
                       << " path: " << parentPathStr << "/" << name
                       << " decision: " << NKikimrSchemeOp::TUpgradeSubDomain::EDecision_Name(decision)
                       << ", at tablet" << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));
        TString errStr;

        TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted()
                .IsExternalSubDomain()
                .IsUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TTxId txId = path.Base()->LastTxId;
        if (!context.SS->Operations.contains(txId)) {
            errStr = "no transaction has been found at path";
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }
        TOperation::TPtr operation = context.SS->Operations.at(txId);
        Y_ABORT_UNLESS(operation->Parts.size());

        TTxState* upgradeState = context.SS->FindTx(TOperationId(txId, 0));
        Y_ABORT_UNLESS(upgradeState);
        if (upgradeState->TxType != TTxState::TxUpgradeSubDomain) {
            errStr = "no update subdomain transaction has been found at path";
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (upgradeState->State != TTxState::PublishGlobal) {
            errStr = "no update subdomain transaction has been found at path";
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        switch (decision) {
        case NKikimrSchemeOp::TUpgradeSubDomain::Commit: {
            THolder<TEvPrivate::TEvCommitTenantUpdate> msg = MakeHolder<TEvPrivate::TEvCommitTenantUpdate>();
            TEvPrivate::TEvCommitTenantUpdate::TPtr personalEv = (TEventHandle<TEvPrivate::TEvCommitTenantUpdate>*) new IEventHandle(
                context.SS->SelfId(), context.SS->SelfId(), msg.Release());
            operation->Parts.front()->HandleReply(personalEv, context);
            break;
        }
        case NKikimrSchemeOp::TUpgradeSubDomain::Undo: {
            THolder<TEvPrivate::TEvUndoTenantUpdate> msg = MakeHolder<TEvPrivate::TEvUndoTenantUpdate>();
            TEvPrivate::TEvUndoTenantUpdate::TPtr personalEv = (TEventHandle<TEvPrivate::TEvUndoTenantUpdate>*) new IEventHandle(
                context.SS->SelfId(), context.SS->SelfId(), msg.Release());
            operation->Parts.front()->HandleReply(personalEv, context);
            break;
        }
        case NKikimrSchemeOp::TUpgradeSubDomain::Invalid:
            errStr = "Invalid task param";
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxUpgradeSubDomainDecision, path.Base()->PathId);
        txState.State = TTxState::Waiting;

        NIceDb::TNiceDb db(context.GetDB());

        context.SS->PersistTxState(db, OperationId);

        TStringBuilder errMsg;
        errMsg << "TWait ProgressState"
               << ", dependent transaction: " << OperationId.GetTxId()
               << ", parent transaction: " << txId
               << ", at schemeshard: " << ssId;

        LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, errMsg);

        context.OnComplete.Dependence(txId, OperationId.GetTxId());
        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TUpgradeSubDomainDecision");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TUpgradeSubDomainDecision AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateUpgradeSubDomain(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TUpgradeSubDomain>(id, tx);
}

ISubOperation::TPtr CreateUpgradeSubDomain(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TUpgradeSubDomain>(id, state);
}

ISubOperation::TPtr CreateUpgradeSubDomainDecision(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TUpgradeSubDomainDecision>(id, tx);
}

ISubOperation::TPtr CreateUpgradeSubDomainDecision(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TUpgradeSubDomainDecision>(id, state);
}

ISubOperation::TPtr CreateCompatibleSubdomainDrop(TSchemeShard* ss, TOperationId id, const TTxTransaction& tx) {
    const auto& info = tx.GetDrop();

    const TString& parentPathStr = tx.GetWorkingDir();
    const TString& name = info.GetName();

    TPath path = TPath::Resolve(parentPathStr, ss).Dive(name);

    {
        TPath::TChecker checks = path.Check();
        checks
            .NotEmpty()
            .IsResolved()
            .NotDeleted();

        if (!checks) {
            return CreateForceDropSubDomain(id, tx);
        }
    }

    if (path.Base()->IsExternalSubDomainRoot()) {
        return CreateForceDropExtSubDomain(id, tx);
    }

    return CreateForceDropSubDomain(id, tx);
}

ISubOperation::TPtr CreateCompatibleSubdomainAlter(TSchemeShard* ss, TOperationId id, const TTxTransaction& tx) {
    const auto& info = tx.GetSubDomain();

    const TString& parentPathStr = tx.GetWorkingDir();
    const TString& name = info.GetName();

    TPath path = TPath::Resolve(parentPathStr, ss).Dive(name);

    {
        TPath::TChecker checks = path.Check();
        checks
            .NotEmpty()
            .IsResolved()
            .NotDeleted();

        if (!checks) {
            return CreateAlterSubDomain(id, tx);
        }
    }

    if (path.Base()->IsExternalSubDomainRoot()) {
        return CreateAlterExtSubDomain(id, tx);
    }

    return CreateAlterSubDomain(id, tx);
}

}
