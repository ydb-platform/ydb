#pragma once

#include "schemeshard_impl.h"
#include "schemeshard__operation_part.h"

namespace NKikimr {
namespace NSchemeShard {

inline bool CheckStoragePoolsInQuotas(
    const Ydb::Cms::DatabaseQuotas& quotas,
    const TVector<TStoragePool>& pools,
    const TString& path,
    TString& error
) {
    TVector<TString> quotedKinds;
    quotedKinds.reserve(quotas.storage_quotas_size());
    for (const auto& storageQuota : quotas.storage_quotas()) {
        quotedKinds.emplace_back(storageQuota.unit_kind());
    }
    Sort(quotedKinds);
    if (const auto equalKinds = AdjacentFind(quotedKinds);
        equalKinds != quotedKinds.end()
    ) {
        error = TStringBuilder()
            << "Malformed subdomain request: storage kinds in DatabaseQuotas must be unique, but "
            << *equalKinds << " appears twice in the quotas definition of the " << path << " subdomain.";
        return false;
    }

    TVector<TString> existingKinds;
    existingKinds.reserve(pools.size());
    for (const auto& pool : pools) {
        existingKinds.emplace_back(pool.GetKind());
    }
    Sort(existingKinds);
    TVector<TString> unknownKinds;
    SetDifference(quotedKinds.begin(), quotedKinds.end(),
                  existingKinds.begin(), existingKinds.end(),
                  std::back_inserter(unknownKinds)
    );
    if (!unknownKinds.empty()) {
        error = TStringBuilder()
            << "Malformed subdomain request: cannot set storage quotas of the following kinds: " << JoinSeq(", ", unknownKinds)
            << ", because no storage pool in the subdomain " << path << " has the specified kinds. "
            << "Existing storage kinds are: " << JoinSeq(", ", existingKinds);
        return false;
    }
    return true;
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
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSubDomain
            || txState->TxType == TTxState::TxAlterSubDomain
            || txState->TxType == TTxState::TxAlterExtSubDomain
            || txState->TxType == TTxState::TxAlterExtSubDomainCreateHive
        );

        const auto& record = ev->Get()->Record;

        NIceDb::TNiceDb db(context.GetDB());

        TTabletId tabletId = TTabletId(record.GetTenantSchemeShard());
        auto status = record.GetStatus();

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

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
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSubDomain
            || txState->TxType == TTxState::TxAlterSubDomain
            || txState->TxType == TTxState::TxAlterExtSubDomain
            || txState->TxType == TTxState::TxAlterExtSubDomainCreateHive
        );

        const auto& record = ev->Get()->Record;

        NIceDb::TNiceDb db(context.GetDB());

        TTabletId tabletId = TTabletId(record.GetOnTabletId());
        auto status = record.GetStatus();

        auto shardIdx = context.SS->MustGetShardIdx(tabletId);
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

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
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSubDomain
            || txState->TxType == TTxState::TxAlterSubDomain
            || txState->TxType == TTxState::TxAlterExtSubDomain
            || txState->TxType == TTxState::TxAlterExtSubDomainCreateHive
        );

        txState->ClearShardsInProgress();

        if (txState->Shards.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        auto pathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        TPath path = TPath::Init(pathId, context.SS);

        Y_ABORT_UNLESS(context.SS->SubDomains.contains(pathId));
        auto subDomain = context.SS->SubDomains.at(pathId);
        auto alterData = subDomain->GetAlter();
        Y_ABORT_UNLESS(alterData);
        alterData->Initialize(context.SS->ShardInfos);
        auto processing = alterData->GetProcessingParams();
        auto storagePools = alterData->GetStoragePools();
        auto& schemeLimits = subDomain->GetSchemeLimits();

        for (auto& shard : txState->Shards) {
            if (shard.Operation != TTxState::CreateParts) {
                continue;
            }
            TShardIdx idx = shard.Idx;
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(idx));
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
                    "Send configure request to sys view processor: " << tabletID <<
                    " opId: " << OperationId <<
                    " schemeshard: " << ssId);
                auto event = new NSysView::TEvSysView::TEvConfigureProcessor(path.PathString());
                shard.Operation = TTxState::ConfigureParts;
                context.OnComplete.BindMsgToPipe(OperationId, tabletID, idx, event);
                break;
            }
            case ETabletType::StatisticsAggregator: {
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Send configure request to statistics aggregator: " << tabletID <<
                    " opId: " << OperationId <<
                    " schemeshard: " << ssId);
                auto event = new NStat::TEvStatistics::TEvConfigureAggregator(path.PathString());
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
                if (alterData->GetAuditSettings()) {
                    event->Record.MutableAuditSettings()->CopyFrom(*alterData->GetAuditSettings());
                }
                if (alterData->GetServerlessComputeResourcesMode()) {
                    event->Record.SetServerlessComputeResourcesMode(*alterData->GetServerlessComputeResourcesMode());
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
            case ETabletType::GraphShard: {
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Send configure request to graph shard: " << tabletID <<
                    " opId: " << OperationId <<
                    " schemeshard: " << ssId);
                shard.Operation = TTxState::ConfigureParts;
                auto event = new TEvSubDomain::TEvConfigure(processing);
                context.OnComplete.BindMsgToPipe(OperationId, tabletID, idx, event);
                break;
            }
            case ETabletType::BackupController: {
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Send configure request to backup controller tablet: " << tabletID <<
                    " opId: " << OperationId <<
                    " schemeshard: " << ssId);
                shard.Operation = TTxState::ConfigureParts;
                auto event = new TEvSubDomain::TEvConfigure(processing);
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
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NSubDomainState::TPropose"
                << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvHive::TEvCreateTabletReply::EventType,
            TEvSubDomain::TEvConfigureStatus::EventType,
            TEvPrivate::TEvCompleteBarrier::EventType,
        });
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "NSubDomainState::TPropose HandleReply TEvOperationPlan"
                     << ", operationId " << OperationId
                     << ", at tablet " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if (!txState) {
            return false;
        }
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSubDomain
            || txState->TxType == TTxState::TxAlterSubDomain
            || txState->TxType == TTxState::TxCreateExtSubDomain
            || txState->TxType == TTxState::TxAlterExtSubDomain
            || txState->TxType == TTxState::TxAlterExtSubDomainCreateHive
        );

        TPathId pathId = txState->TargetPathId;
        Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        if (path->StepCreated == InvalidStepId) {
            path->StepCreated = step;
            context.SS->PersistCreateStep(db, pathId, step);
        }

        Y_ABORT_UNLESS(context.SS->SubDomains.contains(pathId));
        auto subDomain = context.SS->SubDomains.at(pathId);
        auto alter = subDomain->GetAlter();
        Y_ABORT_UNLESS(alter);
        Y_VERIFY_S(subDomain->GetVersion() < alter->GetVersion(), "" << subDomain->GetVersion() << " and " << alter->GetVersion());

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

        if (txState->NeedSyncHive) {
            context.SS->ChangeTxState(db, OperationId, TTxState::SyncHive);
        } else {
            context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        }
        
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "NSubDomainState::TPropose HandleReply TEvOperationPlan"
                     << ", operationId " << OperationId
                     << ", at tablet " << ssId);

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "NSubDomainState::TPropose ProgressState"
                       << ", operationId: " << OperationId
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateSubDomain
            || txState->TxType == TTxState::TxAlterSubDomain
            || txState->TxType == TTxState::TxCreateExtSubDomain
            || txState->TxType == TTxState::TxAlterExtSubDomain
            || txState->TxType == TTxState::TxAlterExtSubDomainCreateHive
        );

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "NSubDomainState::TPropose ProgressState leave"
                     << ", operationId " << OperationId
                     << ", at tablet " << ssId);

        return false;
    }
};

} // namespace NSubDomainState

} // namespace NSchemeShard
} // namespace NKikimr
