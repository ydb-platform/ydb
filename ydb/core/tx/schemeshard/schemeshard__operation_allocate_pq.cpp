#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/persqueue/config/config.h>
#include <ydb/core/mind/hive/hive.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TAllocatePQ: public TSubOperation {
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
            return MakeHolder<NPQState::TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<NPQState::TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const auto& allocateDesc = Transaction.GetAllocatePersQueueGroup();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = allocateDesc.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAllocatePQ Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAllocatePQ Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", allocateDesc: " << allocateDesc.ShortUtf8DebugString()
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
                .IsLikeDirectory();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
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
                    .FailOnExist(TPathElement::EPathType::EPathTypePersQueueGroup, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .IsValidLeafName()
                    .DepthLimit()
                    .PathsLimit()
                    .DirChildrenLimit()
                    .IsValidACL(acl);
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TString errStr;

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TTopicInfo::TPtr pqGroupInfo = new TTopicInfo;

        pqGroupInfo->TotalGroupCount = allocateDesc.GetTotalGroupCount();
        if (pqGroupInfo->TotalGroupCount == 0 || pqGroupInfo->TotalGroupCount > TSchemeShard::MaxPQGroupPartitionsCount) {
            auto errStr = TStringBuilder() << "Invalid total partition count specified: " << pqGroupInfo->TotalGroupCount;
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        if (allocateDesc.HasPQTabletConfig() && allocateDesc.GetPQTabletConfig().HasPartitionStrategy()) {
            const auto strategy = allocateDesc.GetPQTabletConfig().GetPartitionStrategy();
            if (strategy.GetMaxPartitionCount() < strategy.GetMinPartitionCount()) {
                errStr = Sprintf("Invalid min and max partition count specified: %u > %u", strategy.GetMinPartitionCount(), strategy.GetMaxPartitionCount());
                return nullptr;
            }
        }

        pqGroupInfo->TotalPartitionCount = allocateDesc.PartitionsSize();
        if (pqGroupInfo->TotalPartitionCount != pqGroupInfo->TotalGroupCount) {
            auto errStr = TStringBuilder() << "not all partitions has beed described, there is only: " << pqGroupInfo->TotalPartitionCount;
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        pqGroupInfo->MaxPartsPerTablet = allocateDesc.GetPartitionPerTablet();
        if (pqGroupInfo->MaxPartsPerTablet == 0 || pqGroupInfo->MaxPartsPerTablet > TSchemeShard::MaxPQTabletPartitionsCount) {
            auto errStr = TStringBuilder() << "Invalid partition per tablet count specified: " << pqGroupInfo->MaxPartsPerTablet;
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        pqGroupInfo->NextPartitionId = allocateDesc.GetNextPartitionId();
        if (pqGroupInfo->NextPartitionId == 0 || pqGroupInfo->NextPartitionId > pqGroupInfo->TotalPartitionCount) {
            auto errStr = TStringBuilder() << "Invalid NextPartitionId per specified: " << pqGroupInfo->NextPartitionId;
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        pqGroupInfo->AlterVersion = allocateDesc.GetAlterVersion() + 1;

        if (!allocateDesc.HasPQTabletConfig()) {
            auto errStr = TStringBuilder() << "No PQTabletConfig specified";
            result->SetError(NKikimrScheme::StatusSchemeError, errStr);
            return result;
        }

        auto tabletConfig = allocateDesc.GetPQTabletConfig();
        const auto& partitionConfig = tabletConfig.GetPartitionConfig();

        if ((ui32)partitionConfig.GetWriteSpeedInBytesPerSecond() > TSchemeShard::MaxPQWriteSpeedPerPartition) {
            auto errStr = TStringBuilder()
                    << "Invalid write speed per second in partition specified: " << partitionConfig.GetWriteSpeedInBytesPerSecond()
                    << " vs " << TSchemeShard::MaxPQWriteSpeedPerPartition;
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        if ((ui32)partitionConfig.GetLifetimeSeconds() > TSchemeShard::MaxPQLifetimeSeconds) {
            auto errStr = TStringBuilder()
                    << "Invalid retention period specified: " << partitionConfig.GetLifetimeSeconds()
                    << " vs " << TSchemeShard::MaxPQLifetimeSeconds;
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        if (tabletConfig.PartitionKeySchemaSize()) {
            auto errStr = TStringBuilder() << "No support for CDC streams";
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }



        TMap<TTabletId, TAdoptedShard> adoptedTablets;
        TMap<ui64, std::pair<ui64, TTabletId>> partitionToGroupAndTablet;

        for (auto& partition: allocateDesc.GetPartitions()) {
            auto partId = partition.GetPartitionId();
            auto gId = partition.GetGroupId();
            auto tabletId = TTabletId(partition.GetTabletId());

            auto prevOwnerId = partition.GetOwnerId();
            auto prevShardId = TLocalShardIdx(partition.GetShardId());

            if (adoptedTablets.contains(tabletId)) {
                Y_ABORT_UNLESS(adoptedTablets.at(tabletId).PrevOwner == prevOwnerId);
                Y_ABORT_UNLESS(adoptedTablets.at(tabletId).PrevShardIdx == prevShardId);
            } else {
                adoptedTablets[tabletId] = TAdoptedShard{prevOwnerId, prevShardId};
            }

            partitionToGroupAndTablet[partId] = std::make_pair(gId, tabletId);
        }
        Y_ABORT_UNLESS(pqGroupInfo->TotalGroupCount == partitionToGroupAndTablet.size());

        if (pqGroupInfo->ExpectedShardCount() != adoptedTablets.size()) {
            auto errStr = TStringBuilder() << "Invalid tablet count expectation: " << pqGroupInfo->ExpectedShardCount()
                                           << ", shards count: " << adoptedTablets.size();
            result->SetError(NKikimrScheme::StatusSchemeError, errStr);
            return result;
        }

        tabletConfig.ClearPartitionIds();
        tabletConfig.ClearPartitions();

        {
            TString errStr;
            if (!CheckPersQueueConfig(tabletConfig, false, &errStr)) {
                result->SetError(NKikimrScheme::StatusSchemeError, errStr);
                return result;
            }
        }

        const TPathElement::TPtr dbRootEl = context.SS->PathsById.at(context.SS->RootPathId());
        if (dbRootEl->UserAttrs->Attrs.contains("cloud_id")) {
            auto cloudId = dbRootEl->UserAttrs->Attrs.at("cloud_id");
            tabletConfig.SetYcCloudId(cloudId);
        }
        if (dbRootEl->UserAttrs->Attrs.contains("folder_id")) {
            auto folderId = dbRootEl->UserAttrs->Attrs.at("folder_id");
            tabletConfig.SetYcFolderId(folderId);
        }
        if (dbRootEl->UserAttrs->Attrs.contains("database_id")) {
            auto databaseId = dbRootEl->UserAttrs->Attrs.at("database_id");
            tabletConfig.SetYdbDatabaseId(databaseId);
        }

        const TString databasePath = TPath::Init(context.SS->RootPathId(), context.SS).PathString();
        tabletConfig.SetYdbDatabasePath(databasePath);

        Y_PROTOBUF_SUPPRESS_NODISCARD tabletConfig.SerializeToString(&pqGroupInfo->TabletConfig);

        const ui64 shardsToCreate = pqGroupInfo->ExpectedShardCount() + 1;
        const ui64 partitionsToCreate = pqGroupInfo->TotalPartitionCount;

        const PQGroupReserve reserve(tabletConfig, partitionsToCreate);

        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks
                .ShardsLimit(shardsToCreate)
                .PathShardsLimit(shardsToCreate)
                .PQPartitionsLimit(partitionsToCreate)
                .PQReservedStorageLimit(reserve.Storage);

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        // This profile id is only used for pq read balancer tablet when
        // explicit channel profiles are specified. Read balancer tablet is
        // a tablet with local db which doesn't use extra channels in any way.
        const ui32 tabletProfileId = 0;
        TChannelsBindings tabletChannelsBinding;
        if (!context.SS->ResolvePqChannels(tabletProfileId, dstPath.GetPathIdForDomain(), tabletChannelsBinding)) {
            result->SetError(NKikimrScheme::StatusInvalidParameter,
                             "Unable to construct channel binding for PQ with the storage pool");
            return result;
        }

        // This channel bindings are for PersQueue shards. They either use
        // explicit channel profiles, or reuse channel profile above.
        const auto& partConfig = allocateDesc.GetPQTabletConfig().GetPartitionConfig();
        TChannelsBindings pqChannelsBinding;
        if (partConfig.ExplicitChannelProfilesSize() > 0) {
            const auto& ecps = partConfig.GetExplicitChannelProfiles();

            if (ecps.size() < 3 || ui32(ecps.size()) > NHive::MAX_TABLET_CHANNELS) {
                auto errStr = Sprintf("ExplicitChannelProfiles has %u channels, should be [3 .. %lu]",
                                    ecps.size(),
                                    NHive::MAX_TABLET_CHANNELS);
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }

            TVector<TStringBuf> partitionPoolKinds;
            partitionPoolKinds.reserve(ecps.size());
            for (const auto& ecp : ecps) {
                partitionPoolKinds.push_back(ecp.GetPoolKind());
            }

            const auto resolved = context.SS->ResolveChannelsByPoolKinds(
                partitionPoolKinds,
                dstPath.GetPathIdForDomain(),
                pqChannelsBinding);
            if (!resolved) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                                "Unable to construct channel binding for PersQueue with the storage pool");
                return result;
            }

            context.SS->SetPqChannelsParams(ecps, pqChannelsBinding);
        } else {
            pqChannelsBinding = tabletChannelsBinding;
        }

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        context.SS->TabletCounters->Simple()[COUNTER_PQ_GROUP_COUNT].Add(1);

        TPathId pathId = dstPath.Base()->PathId;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAllocatePQ, pathId);

        TShardInfo shardInfo = TShardInfo::PersQShardInfo(OperationId.GetTxId(), pathId);
        shardInfo.BindedChannels = pqChannelsBinding;

        txState.Shards.reserve(shardsToCreate);
        const auto startShardIdx = context.SS->ReserveShardIdxs(shardsToCreate);
        ui64 i = 0;
        for (auto shardId = adoptedTablets.begin(); shardId != adoptedTablets.end(); ++shardId, ++i) {
            const auto idx = context.SS->NextShardIdx(startShardIdx, i);
            context.SS->RegisterShardInfo(idx, shardInfo);
            txState.Shards.emplace_back(idx, ETabletType::PersQueue, TTxState::CreateParts);

            auto tabletId = shardId->first;
            context.SS->AdoptedShards[idx] = shardId->second;
            context.SS->ShardInfos[idx].TabletID = tabletId;
            context.SS->TabletIdToShardIdx[tabletId] = idx;


            TTopicTabletInfo::TPtr pqShard = new TTopicTabletInfo();
            pqShard->Partitions.reserve(pqGroupInfo->MaxPartsPerTablet);
            pqGroupInfo->Shards[idx] = pqShard;
        }

        for (auto& partition : allocateDesc.GetPartitions()) {
            const auto& p = partitionToGroupAndTablet[partition.GetPartitionId()];

            ui64 partId = partition.GetPartitionId();
            ui64 groupId = p.first;
            auto tabletId = p.second;

            auto idx = context.SS->TabletIdToShardIdx.at(tabletId);

            auto pqInfo = MakeHolder<TTopicTabletInfo::TTopicPartitionInfo>();
            pqInfo->PqId = partId;
            pqInfo->GroupId = groupId;
            pqInfo->AlterVersion = pqGroupInfo->AlterVersion;

            pqInfo->Status = partition.GetStatus();
            for (const auto parent : partition.GetParentPartitionIds()) {
                pqInfo->ParentPartitionIds.emplace(parent);
            }
            if (partition.HasKeyRange()) {
                pqInfo->KeyRange.ConstructInPlace();
                (*pqInfo->KeyRange).DeserializeFromProto(partition.GetKeyRange());
            }

            pqGroupInfo->AddPartition(idx, pqInfo.Release());
        }

        pqGroupInfo->InitSplitMergeGraph();

        {
            const auto balancerIdx = context.SS->NextShardIdx(startShardIdx, shardsToCreate-1);
            context.SS->RegisterShardInfo(balancerIdx,
                                          TShardInfo::PQBalancerShardInfo(OperationId.GetTxId(), pathId)
                                          .WithBindedChannels(tabletChannelsBinding));
            txState.Shards.emplace_back(balancerIdx, ETabletType::PersQueueReadBalancer, TTxState::CreateParts);
            pqGroupInfo->BalancerTabletID = TTabletId(allocateDesc.GetBalancerTabletID());
            pqGroupInfo->BalancerShardIdx = balancerIdx;
            Y_ABORT_UNLESS(!context.SS->AdoptedShards.contains(balancerIdx));
            context.SS->AdoptedShards[balancerIdx] = TAdoptedShard{allocateDesc.GetBalancerOwnerId(), TLocalShardIdx(allocateDesc.GetBalancerShardId())};
            context.SS->ShardInfos[balancerIdx].TabletID = pqGroupInfo->BalancerTabletID;
            context.SS->TabletIdToShardIdx[pqGroupInfo->BalancerTabletID] = balancerIdx;
        }

        NIceDb::TNiceDb db(context.GetDB());

        for (auto& shard : pqGroupInfo->Shards) {
            auto shardIdx = shard.first;
            for (const auto& pqInfo : shard.second->Partitions) {
                context.SS->PersistPersQueue(db, pathId, shardIdx, *pqInfo);
            }
        }

        TTopicInfo::TPtr emptyGroup = new TTopicInfo;
        emptyGroup->Shards.swap(pqGroupInfo->Shards);

        context.SS->Topics[pathId] = emptyGroup;
        context.SS->Topics[pathId]->AlterData = pqGroupInfo;
        context.SS->Topics[pathId]->AlterVersion = pqGroupInfo->AlterVersion;

        context.SS->IncrementPathDbRefCount(pathId);

        context.SS->PersistPersQueueGroup(db, pathId, emptyGroup);
        context.SS->PersistAddPersQueueGroupAlter(db, pathId, pqGroupInfo);

        for (auto shard : txState.Shards) {
            Y_ABORT_UNLESS(shard.Operation == TTxState::CreateParts);
            auto tabletId = context.SS->ShardInfos[shard.Idx].TabletID;
            context.SS->PersistShardMapping(db, shard.Idx, tabletId, pathId, OperationId.GetTxId(), shard.TabletType);
            context.SS->PersistChannelsBinding(db, shard.Idx, context.SS->ShardInfos[shard.Idx].BindedChannels);
            context.SS->PersistAdoptedShardMapping(db, shard.Idx, tabletId, context.SS->AdoptedShards[shard.Idx].PrevOwner, context.SS->AdoptedShards[shard.Idx].PrevShardIdx);
        }

        Y_ABORT_UNLESS(txState.Shards.size() == shardsToCreate);
        context.SS->TabletCounters->Simple()[COUNTER_PQ_SHARD_COUNT].Add(shardsToCreate-1);
        context.SS->TabletCounters->Simple()[COUNTER_PQ_RB_SHARD_COUNT].Add(1);

        dstPath.Base()->CreateTxId = OperationId.GetTxId();
        dstPath.Base()->LastTxId = OperationId.GetTxId();
        dstPath.Base()->PathState = TPathElement::EPathState::EPathStateCreate;
        dstPath.Base()->PathType = TPathElement::EPathType::EPathTypePersQueueGroup;

        if (parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->PersistTxState(db, OperationId);

        if (!acl.empty()) {
            dstPath.Base()->ApplyACL(acl);
        }
        context.SS->PersistPath(db, dstPath.Base()->PathId);

        context.SS->PersistUpdateNextPathId(db);
        context.SS->PersistUpdateNextShardIdx(db);

        ++parentPath.Base()->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath.Base());
        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath.Base()->PathId);

        context.SS->ClearDescribePathCaches(dstPath.Base(), true);
        context.OnComplete.PublishToSchemeBoard(OperationId, dstPath.Base()->PathId);

        dstPath.DomainInfo()->IncPathsInside();
        dstPath.DomainInfo()->AddInternalShards(txState);
        dstPath.DomainInfo()->IncPQPartitionsInside(partitionsToCreate);
        dstPath.DomainInfo()->IncPQReservedStorage(reserve.Storage);
        dstPath.Base()->IncShardsInside(shardsToCreate);
        parentPath.Base()->IncAliveChildren();

        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_THROUGHPUT].Add(reserve.Throughput);
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Add(reserve.Storage);
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_SHARDS_COUNT].Add(partitionsToCreate);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TCreatePQ");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAllocatePQ AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAllocatePQ(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAllocatePQ>(id, tx);
}

ISubOperation::TPtr CreateAllocatePQ(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAllocatePQ>(id, state);
}

}
