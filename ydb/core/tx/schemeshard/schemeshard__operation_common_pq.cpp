#include "schemeshard__operation_common.h"
#include "schemeshard_private.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>


namespace NKikimr::NSchemeShard::NPQState {

namespace {

void ParsePQTabletConfig(NKikimrPQ::TPQTabletConfig& config,
                                const TTopicInfo& pqGroup)
{
    const TString* source = &pqGroup.TabletConfig;
    if (pqGroup.AlterData) {
        if (!pqGroup.AlterData->TabletConfig.empty())
            source = &pqGroup.AlterData->TabletConfig;
    }

    if (!source->empty()) {
        Y_ABORT_UNLESS(ParseFromStringNoSizeLimit(config, *source));
    }
}

void FillPartition(NKikimrPQ::TPQTabletConfig::TPartition& partition, const TTopicTabletInfo::TTopicPartitionInfo* pq, ui64 tabletId) {
    partition.SetPartitionId(pq->PqId);
    partition.SetCreateVersion(pq->CreateVersion);
    if (pq->KeyRange) {
        pq->KeyRange->SerializeToProto(*partition.MutableKeyRange());
    }
    partition.SetStatus(pq->Status);
    partition.MutableParentPartitionIds()->Reserve(pq->ParentPartitionIds.size());
    for (const auto parent : pq->ParentPartitionIds) {
        partition.MutableParentPartitionIds()->AddAlreadyReserved(parent);
    }
    partition.MutableChildPartitionIds()->Reserve(pq->ChildPartitionIds.size());
    for (const auto children : pq->ChildPartitionIds) {
        partition.MutableChildPartitionIds()->AddAlreadyReserved(children);
    }
    partition.SetTabletId(tabletId);
}

void MakePQTabletConfig(const TOperationContext& context,
                                NKikimrPQ::TPQTabletConfig& config,
                                const TTopicInfo& pqGroup,
                                const TTopicTabletInfo& pqShard,
                                const TString& topicName,
                                const TString& topicPath,
                                const TString& cloudId,
                                const TString& folderId,
                                const TString& databaseId,
                                const TString& databasePath)
{
    ParsePQTabletConfig(config, pqGroup);

    config.SetTopicName(topicName);
    config.SetTopicPath(topicPath);
    config.MutablePartitionConfig()->SetTotalPartitions(pqGroup.AlterData ? pqGroup.AlterData->TotalGroupCount : pqGroup.TotalGroupCount);

    config.SetYcCloudId(cloudId);
    config.SetYcFolderId(folderId);
    config.SetYdbDatabaseId(databaseId);
    config.SetYdbDatabasePath(databasePath);

    if (pqGroup.AlterData) {
        config.SetVersion(pqGroup.AlterData->AlterVersion);
    }

    THashSet<ui32> linkedPartitions;

    for(const auto& pq : pqShard.Partitions) {
        config.AddPartitionIds(pq->PqId);

        auto& partition = *config.AddPartitions();
        FillPartition(partition, pq.Get(), 0);

        linkedPartitions.insert(pq->PqId);
        linkedPartitions.insert(pq->ParentPartitionIds.begin(), pq->ParentPartitionIds.end());
        linkedPartitions.insert(pq->ChildPartitionIds.begin(), pq->ChildPartitionIds.end());
        for (auto c : pq->ChildPartitionIds) {
            auto it = pqGroup.Partitions.find(c);
            if (it == pqGroup.Partitions.end()) {
                continue;
            }
            linkedPartitions.insert(it->second->ParentPartitionIds.begin(), it->second->ParentPartitionIds.end());
        }
    }

    for(auto lp : linkedPartitions) {
        auto it = pqGroup.Partitions.find(lp);
        if (it == pqGroup.Partitions.end()) {
            continue;
        }

        auto* partitionInfo = it->second;
        const auto& tabletId = context.SS->ShardInfos[partitionInfo->ShardIdx].TabletID;

        auto& partition = *config.AddAllPartitions();
        FillPartition(partition, partitionInfo, ui64(tabletId));
    }
}

class TBootstrapConfigWrapper: public NKikimrPQ::TBootstrapConfig {
    struct TSerializedProposeTransaction {
        TString Value;

        static TSerializedProposeTransaction Serialize(const NKikimrPQ::TBootstrapConfig& value) {
            NKikimrPQ::TEvProposeTransaction record;
            record.MutableConfig()->MutableBootstrapConfig()->CopyFrom(value);
            return {record.SerializeAsString()};
        }
    };

    struct TSerializedUpdateConfig {
        TString Value;

        static TSerializedUpdateConfig Serialize(const NKikimrPQ::TBootstrapConfig& value) {
            NKikimrPQ::TUpdateConfig record;
            record.MutableBootstrapConfig()->CopyFrom(value);
            return {record.SerializeAsString()};
        }
    };

    mutable std::optional<std::variant<
        TSerializedProposeTransaction,
        TSerializedUpdateConfig
    >> PreSerialized;

    template <typename T>
    const TString& Get() const {
        if (!PreSerialized) {
            PreSerialized.emplace(T::Serialize(*this));
        }

        const auto* value = std::get_if<T>(&PreSerialized.value());
        Y_ABORT_UNLESS(value);

        return value->Value;
    }

public:
    const TString& GetPreSerializedProposeTransaction() const {
        return Get<TSerializedProposeTransaction>();
    }

    const TString& GetPreSerializedUpdateConfig() const {
        return Get<TSerializedUpdateConfig>();
    }
};

THolder<TEvPersQueue::TEvProposeTransaction> MakeEvProposeTransaction(
        TTxId txId,
        const TTopicInfo& pqGroup,
        const TTopicTabletInfo& pqShard,
        const TString& topicName,
        const TString& topicPath,
        const std::optional<TBootstrapConfigWrapper>& bootstrapConfig,
        const TString& cloudId,
        const TString& folderId,
        const TString& databaseId,
        const TString& databasePath,
        TTxState::ETxType txType,
        const TOperationContext& context
    )
{
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransactionBuilder>();
    event->Record.SetTxId(ui64(txId));
    ActorIdToProto(context.SS->SelfId(), event->Record.MutableSourceActor());

    MakePQTabletConfig(context,
                      *event->Record.MutableConfig()->MutableTabletConfig(),
                       pqGroup,
                       pqShard,
                       topicName,
                       topicPath,
                       cloudId,
                       folderId,
                       databaseId,
                       databasePath);
    if (bootstrapConfig) {
        Y_ABORT_UNLESS(txType == TTxState::TxCreatePQGroup);
        event->PreSerializedData += bootstrapConfig->GetPreSerializedProposeTransaction();
    }

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Propose configure PersQueue" <<
                ", message: " << event->Record.ShortUtf8DebugString());

    return event;
}

THolder<TEvPersQueue::TEvUpdateConfig> MakeEvUpdateConfig(
        TTxId txId,
        const TTopicInfo& pqGroup,
        const TTopicTabletInfo& pqShard,
        const TString& topicName,
        const TString& topicPath,
        const std::optional<TBootstrapConfigWrapper>& bootstrapConfig,
        const TString& cloudId,
        const TString& folderId,
        const TString& databaseId,
        const TString& databasePath,
        TTxState::ETxType txType,
        const TOperationContext& context
    )
{
    auto event = MakeHolder<TEvPersQueue::TEvUpdateConfigBuilder>();
    event->Record.SetTxId(ui64(txId));

    MakePQTabletConfig(context,
                       *event->Record.MutableTabletConfig(),
                       pqGroup,
                       pqShard,
                       topicName,
                       topicPath,
                       cloudId,
                       folderId,
                       databaseId,
                       databasePath);
    if (bootstrapConfig) {
        Y_ABORT_UNLESS(txType == TTxState::TxCreatePQGroup);
        event->PreSerializedData += bootstrapConfig->GetPreSerializedUpdateConfig();
    }

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Propose configure PersQueue" <<
                ", message: " << event->Record.ShortUtf8DebugString());

    return event;
}

}  // anonymous namespace


bool CollectPQConfigChanged(const TOperationId& operationId,
                            const TEvPersQueue::TEvProposeTransactionResult::TPtr& ev,
                            TOperationContext& context)
{
    Y_ABORT_UNLESS(context.SS->FindTx(operationId));
    TTxState& txState = *context.SS->FindTx(operationId);

    const auto& evRecord = ev->Get()->Record;
    if (evRecord.GetStatus() == NKikimrPQ::TEvProposeTransactionResult::COMPLETE) {
        const auto ssId = context.SS->SelfTabletId();
        const TTabletId shardId(evRecord.GetOrigin());

        const auto shardIdx = context.SS->MustGetShardIdx(shardId);
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

        Y_ABORT_UNLESS(txState.State == TTxState::Propose);

        txState.ShardsInProgress.erase(shardIdx);

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CollectPQConfigChanged accept TEvPersQueue::TEvProposeTransactionResult"
                    << ", operationId: " << operationId
                    << ", shardIdx: " << shardIdx
                    << ", shard: " << shardId
                    << ", left await: " << txState.ShardsInProgress.size()
                    << ", txState.State: " << TTxState::StateName(txState.State)
                    << ", txState.ReadyForNotifications: " << txState.ReadyForNotifications
                    << ", at schemeshard: " << ssId);
    }

    return txState.ShardsInProgress.empty();
}

bool CollectPQConfigChanged(const TOperationId& operationId,
                            const TEvDataShard::TEvProposeTransactionAttachResult::TPtr& ev,
                            TOperationContext& context)
{
    Y_ABORT_UNLESS(context.SS->FindTx(operationId));
    TTxState& txState = *context.SS->FindTx(operationId);

    //
    // The PQ tablet can perform a transaction and send a TEvProposeTransactionResult(COMPLETE) response.
    // The SchemeShard tablet can restart at this point. After restarting at the TPropose step, it will
    // send the TEvProposeTransactionAttach message to the PQ tablets. If the NODATA status is specified in
    // the response TEvProposeTransactionAttachResult, then the PQ tablet has already completed the transaction.
    // Otherwise, she continues to execute the transaction
    //

    const auto& evRecord = ev->Get()->Record;
    if (evRecord.GetStatus() != NKikimrProto::NODATA) {
        //
        // If the PQ tablet returned something other than NODATA, then it continues to execute the transaction
        //
        return txState.ShardsInProgress.empty();
    }

    //
    // Otherwise, she has already completed the transaction and has forgotten about it. Then we can
    // remove PQ tablet from the list of shards
    //

    const auto ssId = context.SS->SelfTabletId();
    const TTabletId shardId(evRecord.GetTabletId());

    const auto shardIdx = context.SS->MustGetShardIdx(shardId);
    Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

    Y_ABORT_UNLESS(txState.State == TTxState::Propose);

    txState.ShardsInProgress.erase(shardIdx);

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CollectPQConfigChanged accept TEvDataShard::TEvProposeTransactionAttachResult"
                << ", operationId: " << operationId
                << ", shardIdx: " << shardIdx
                << ", shard: " << shardId
                << ", left await: " << txState.ShardsInProgress.size()
                << ", txState.State: " << TTxState::StateName(txState.State)
                << ", txState.ReadyForNotifications: " << txState.ReadyForNotifications
                << ", at schemeshard: " << ssId);

    return txState.ShardsInProgress.empty();
}


// NPQState::TConfigureParts
//
TConfigureParts::TConfigureParts(TOperationId id)
    : OperationId(id)
{
    IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
}

bool TConfigureParts::HandleReply(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context)
{
    const TTabletId ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               DebugHint() << " HandleReply TEvProposeTransactionResult"
               << ", at schemeshard: " << ssId);

    return NPQState::CollectProposeTransactionResults(OperationId, ev, context);
}

bool TConfigureParts::HandleReply(TEvPersQueue::TEvUpdateConfigResponse::TPtr& ev, TOperationContext& context) {
    TTabletId ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvUpdateConfigResponse"
                    << " at tablet" << ssId);
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " HandleReply TEvUpdateConfigResponse"
                    << " message: " << ev->Get()->Record.ShortUtf8DebugString()
                    << " at tablet" << ssId);

    TTxState* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);
    Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

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

    Y_ABORT_UNLESS(txState->State == TTxState::ConfigureParts);

    TShardIdx idx = context.SS->MustGetShardIdx(tabletId);
    txState->ShardsInProgress.erase(idx);

    // Detach datashard pipe
    context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

    if (txState->ShardsInProgress.empty()) {
        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        return true;
    }

    return false;
}

bool TConfigureParts::ProgressState(TOperationContext& context) {
    TTabletId ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint()
                    << " HandleReply ProgressState"
                    << ", at schemeshard: " << ssId);

    TTxState* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);
    Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

    txState->ClearShardsInProgress();

    TString topicName = context.SS->PathsById.at(txState->TargetPathId)->Name;
    Y_VERIFY_S(topicName.size(),
                "topicName is empty"
                    <<", pathId: " << txState->TargetPathId);

    TTopicInfo::TPtr pqGroup = context.SS->Topics[txState->TargetPathId];
    Y_VERIFY_S(pqGroup,
                "pqGroup is null"
                    << ", pathId " << txState->TargetPathId);

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
    auto topicPath = TPath::Init(txState->TargetPathId, context.SS);

    std::optional<TBootstrapConfigWrapper> bootstrapConfig;
    if (txState->TxType == TTxState::TxCreatePQGroup && topicPath.Parent().IsCdcStream()) {
        bootstrapConfig.emplace();

        auto tablePath = topicPath.Parent().Parent(); // table/cdc_stream/topic
        Y_ABORT_UNLESS(tablePath.IsResolved());

        Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
        auto table = context.SS->Tables.at(tablePath.Base()->PathId);

        const auto& partitions = table->GetPartitions();

        for (ui32 i = 0; i < partitions.size(); ++i) {
            const auto& cur = partitions.at(i);

            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(cur.ShardIdx));
            const auto& shard = context.SS->ShardInfos.at(cur.ShardIdx);

            auto& mg = *bootstrapConfig->AddExplicitMessageGroups();
            mg.SetId(NPQ::NSourceIdEncoding::EncodeSimple(ToString(shard.TabletID)));

            if (i != partitions.size() - 1) {
                mg.MutableKeyRange()->SetToBound(cur.EndOfRange);
            }

            if (i) {
                const auto& prev = partitions.at(i - 1);
                mg.MutableKeyRange()->SetFromBound(prev.EndOfRange);
            }
        }
    }

    for (auto shard : txState->Shards) {
        TShardIdx idx = shard.Idx;
        TTabletId tabletId = context.SS->ShardInfos.at(idx).TabletID;

        if (shard.TabletType == ETabletType::PersQueue) {
            TTopicTabletInfo::TPtr pqShard = pqGroup->Shards.at(idx);
            Y_VERIFY_S(pqShard, "pqShard is null, idx is " << idx << " has was "<< THash<TShardIdx>()(idx));

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Propose configure PersQueue"
                            << ", opId: " << OperationId
                            << ", tabletId: " << tabletId
                            << ", Partitions size: " << pqShard->Partitions.size()
                            << ", at schemeshard: " << ssId);

            THolder<NActors::IEventBase> event;
            if (context.SS->EnablePQConfigTransactionsAtSchemeShard) {
                event = MakeEvProposeTransaction(OperationId.GetTxId(),
                                                    *pqGroup,
                                                    *pqShard,
                                                    topicName,
                                                    topicPath.PathString(),
                                                    bootstrapConfig,
                                                    cloudId,
                                                    folderId,
                                                    databaseId,
                                                    databasePath,
                                                    txState->TxType,
                                                    context);
            } else {
                event = MakeEvUpdateConfig(OperationId.GetTxId(),
                                            *pqGroup,
                                            *pqShard,
                                            topicName,
                                            topicPath.PathString(),
                                            bootstrapConfig,
                                            cloudId,
                                            folderId,
                                            databaseId,
                                            databasePath,
                                            txState->TxType,
                                            context);
            }

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Propose configure PersQueue"
                            << ", opId: " << OperationId
                            << ", tabletId: " << tabletId
                            << ", at schemeshard: " << ssId);

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, idx, event.Release());
        } else {
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::PersQueueReadBalancer);

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

            ParsePQTabletConfig(*event->Record.MutableTabletConfig(), *pqGroup);

            Y_ABORT_UNLESS(pqGroup->AlterData);

            event->Record.SetTopicName(topicName);
            event->Record.SetPathId(txState->TargetPathId.LocalPathId);
            event->Record.SetPath(TPath::Init(txState->TargetPathId, context.SS).PathString());
            event->Record.SetPartitionPerTablet(pqGroup->AlterData ? pqGroup->AlterData->MaxPartsPerTablet : pqGroup->MaxPartsPerTablet);
            event->Record.SetSchemeShardId(ui64(context.SS->SelfTabletId()));

            event->Record.SetTotalGroupCount(pqGroup->AlterData ? pqGroup->AlterData->TotalGroupCount : pqGroup->TotalGroupCount);
            event->Record.SetNextPartitionId(pqGroup->AlterData ? pqGroup->AlterData->NextPartitionId : pqGroup->NextPartitionId);

            event->Record.SetVersion(pqGroup->AlterData->AlterVersion);

            for (const auto& p : pqGroup->Shards) {
                const auto& pqShard = p.second;
                const auto& tabletId = context.SS->ShardInfos[p.first].TabletID;
                auto tablet = event->Record.AddTablets();
                tablet->SetTabletId(ui64(tabletId));
                tablet->SetOwner(ui64(context.SS->SelfTabletId()));
                tablet->SetIdx(ui64(p.first.GetLocalId()));
                for (const auto& pq : pqShard->Partitions) {
                    auto info = event->Record.AddPartitions();
                    info->SetPartition(pq->PqId);
                    info->SetTabletId(ui64(tabletId));
                    info->SetGroup(pq->GroupId);
                    info->SetCreateVersion(pq->CreateVersion);

                    if (pq->KeyRange) {
                        pq->KeyRange->SerializeToProto(*info->MutableKeyRange());
                    }
                    info->SetStatus(pq->Status);
                    info->MutableParentPartitionIds()->Reserve(pq->ParentPartitionIds.size());
                    for (const auto parent : pq->ParentPartitionIds) {
                        info->MutableParentPartitionIds()->AddAlreadyReserved(parent);
                    }
                    info->MutableChildPartitionIds()->Reserve(pq->ChildPartitionIds.size());
                    for (const auto children : pq->ChildPartitionIds) {
                        info->MutableChildPartitionIds()->AddAlreadyReserved(children);
                    }
                }
            }

            if (const ui64 subDomainPathId = context.SS->ResolvePathIdForDomain(txState->TargetPathId).LocalPathId) {
                event->Record.SetSubDomainPathId(subDomainPathId);
            }

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Propose configure PersQueueReadBalancer"
                            << ", opId: " << OperationId
                            << ", tabletId: " << tabletId
                            << ", message: " << event->Record.ShortUtf8DebugString()
                            << ", at schemeshard: " << ssId);

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, idx, event.Release());
        }
    }

    txState->UpdateShardsInProgress();
    return false;
}


// NPQState::TPropose
//
TPropose::TPropose(TOperationId id)
    : OperationId(id)
{
    IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType, TEvPersQueue::TEvUpdateConfigResponse::EventType});
}

bool TPropose::HandleReply(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context)
{
    const TTabletId ssId = context.SS->SelfTabletId();
    const auto& evRecord = ev->Get()->Record;

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               DebugHint() << " HandleReply TEvProposeTransactionResult"
               << " triggers early"
               << ", at schemeshard: " << ssId
               << " message# " << evRecord.ShortDebugString());

    const bool collected = CollectPQConfigChanged(OperationId, ev, context);
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply TEvProposeTransactionResult"
                << " CollectPQConfigChanged: " << (collected ? "true" : "false"));

    return TryPersistState(context);
}

bool TPropose::HandleReply(TEvDataShard::TEvProposeTransactionAttachResult::TPtr& ev, TOperationContext& context)
{
    const auto ssId = context.SS->SelfTabletId();
    const auto& evRecord = ev->Get()->Record;

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               DebugHint() << " HandleReply TEvProposeTransactionAttachResult"
               << " triggers early"
               << ", at schemeshard: " << ssId
               << " message# " << evRecord.ShortDebugString());

    const bool collected = CollectPQConfigChanged(OperationId, ev, context);
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply TEvProposeTransactionAttachResult"
                << " CollectPQConfigChanged: " << (collected ? "true" : "false"));

    return TryPersistState(context);
}

bool TPropose::HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context)
{
    TStepId step = TStepId(ev->Get()->StepId);
    TTabletId ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint()
                    << " HandleReply TEvOperationPlan"
                    << ", step: " << step
                    << ", at tablet: " << ssId);

    TTxState* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);
    Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

    TPathId pathId = txState->TargetPathId;
    TPathElement::TPtr path = context.SS->PathsById.at(pathId);

    NIceDb::TNiceDb db(context.GetDB());

    if (path->StepCreated == InvalidStepId) {
        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);
    }

    return TryPersistState(context);
}

bool TPropose::ProgressState(TOperationContext& context)
{
    TTabletId ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "NPQState::TPropose ProgressState"
                    << ", operationId: " << OperationId
                    << ", at schemeshard: " << ssId);

    TTxState* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);
    Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreatePQGroup || txState->TxType == TTxState::TxAlterPQGroup);

    //
    // If the program works according to the new scheme, then we must add PQ tablets to the list for
    // the Coordinator. At this stage, we cannot rely on the value of
    // the EnablePQConfigTransactionsAtSchemeShard flag. Because the operation could have started on one tablet
    // and moved to another by that time.
    //
    // Therefore, here we check the value of the minStep field, which is filled in in
    // the TEvProposeTransactionResult handler
    //
    TSet<TTabletId> shardSet;
    if (ui64(txState->MinStep) > 0) {
        PrepareShards(*txState, shardSet, context);
    }
    context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, txState->MinStep, shardSet);

    return false;
}

void TPropose::PrepareShards(TTxState& txState, TSet<TTabletId>& shardSet, TOperationContext& context)
{
    txState.UpdateShardsInProgress();

    for (const auto& shard : txState.Shards) {
        const TShardIdx idx = shard.Idx;
        //
        // The operation involves the ETabletType::PersQueue and Tabletype::PersQueueReadBalancer shards.
        // The program receives responses from PersQueueReadBalancer at the previous stage. At this stage,
        // it only expects TEvProposeTransactionResult from PersQueue
        //
        if (shard.TabletType == ETabletType::PersQueue) {
            const TTabletId tablet = context.SS->ShardInfos.at(idx).TabletID;

            shardSet.insert(tablet);

            //
            // By this point, the SchemeShard tablet could restart and the actor ID changed. Therefore, we send
            // the TEvProposeTransactionAttach message to the PQ tablets so that they recognize the new recipient
            //
            SendEvProposeTransactionAttach(idx, tablet, context);
        } else {
            txState.ShardsInProgress.erase(idx);
        }
    }
}

void TPropose::SendEvProposeTransactionAttach(TShardIdx shard, TTabletId tablet,
                                              TOperationContext& context)
{
    auto event =
        MakeHolder<TEvPersQueue::TEvProposeTransactionAttach>(ui64(tablet),
                                                              ui64(OperationId.GetTxId()));
    context.OnComplete.BindMsgToPipe(OperationId, tablet, shard, event.Release());
}

bool TPropose::CanPersistState(const TTxState& txState,
                               TOperationContext& context)
{
    if (!txState.ShardsInProgress.empty()) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " can't persist state: " <<
                    "ShardsInProgress is not empty, remain: " << txState.ShardsInProgress.size());
        return false;
    }

    PathId = txState.TargetPathId;
    Path = context.SS->PathsById.at(PathId);

    if (Path->StepCreated == InvalidStepId) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " can't persist state: " <<
                    "StepCreated is invalid");
        return false;
    }

    return true;
}

void TPropose::PersistState(const TTxState& txState,
                            TOperationContext& context) const
{
    NIceDb::TNiceDb db(context.GetDB());

    if (txState.TxType == TTxState::TxCreatePQGroup) {
        auto parentDir = context.SS->PathsById.at(Path->ParentPathId);
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
    }

    context.SS->ClearDescribePathCaches(Path);
    context.OnComplete.PublishToSchemeBoard(OperationId, PathId);

    TTopicInfo::TPtr pqGroup = context.SS->Topics[PathId];

    NKikimrPQ::TPQTabletConfig tabletConfig = pqGroup->GetTabletConfig();
    NKikimrPQ::TPQTabletConfig newTabletConfig = pqGroup->AlterData->GetTabletConfig();

    pqGroup->FinishAlter();

    context.SS->PersistPersQueueGroup(db, PathId, pqGroup);
    context.SS->PersistRemovePersQueueGroupAlter(db, PathId);

    context.SS->ChangeTxState(db, OperationId, TTxState::Done);
}

bool TPropose::TryPersistState(TOperationContext& context)
{
    TTxState* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);

    if (!CanPersistState(*txState, context)) {
        return false;
    }

    PersistState(*txState, context);

    return true;
}

}  // namespace NKikimr::NSchemeShard::NPQState
