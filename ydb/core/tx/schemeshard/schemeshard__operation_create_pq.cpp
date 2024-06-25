#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <library/cpp/int128/int128.h>

#include <ydb/core/base/subdomain.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/persqueue/config/config.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/services/lib/sharding/sharding.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

TTopicInfo::TPtr CreatePersQueueGroup(TOperationContext& context,
                                               const NKikimrSchemeOp::TPersQueueGroupDescription& op,
                                               TEvSchemeShard::EStatus& status, TString& errStr)
{
    TTopicInfo::TPtr pqGroupInfo = new TTopicInfo;

    ui32 partitionCount = 0;
    if (op.HasTotalGroupCount()) {
        partitionCount = op.GetTotalGroupCount();
    }

    ui32 partsPerTablet = TSchemeShard::DefaultPQTabletPartitionsCount;
    if (op.HasPartitionPerTablet()) {
        partsPerTablet = op.GetPartitionPerTablet();
    }

    if (op.PartitionsToDeleteSize() > 0) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = Sprintf("trying to delete partitions from not created PQGroup");
        return nullptr;
    }

    if (op.PartitionsToAddSize()) {
        errStr = Sprintf("creating topic with providing of partitions count is forbidden");
        return nullptr;
    }

    if (partitionCount == 0 || partitionCount > TSchemeShard::MaxPQGroupPartitionsCount) {
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = Sprintf("Invalid total partition count specified: %u", partitionCount);
        return nullptr;
    }

    if (op.HasPQTabletConfig() && op.GetPQTabletConfig().HasPartitionStrategy()) {
        const auto strategy = op.GetPQTabletConfig().GetPartitionStrategy();
        if (strategy.GetMaxPartitionCount() < strategy.GetMinPartitionCount()) {
            errStr = Sprintf("Invalid min and max partition count specified: %u > %u", strategy.GetMinPartitionCount(), strategy.GetMaxPartitionCount());
            return nullptr;
        }
    }

    if (!op.HasPQTabletConfig()) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = Sprintf("No PQTabletConfig specified");
        return nullptr;
    }

    if ((ui32)op.GetPQTabletConfig().GetPartitionConfig().GetWriteSpeedInBytesPerSecond() > TSchemeShard::MaxPQWriteSpeedPerPartition) {
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = TStringBuilder() << "Invalid write speed"
            << ": specified: " << op.GetPQTabletConfig().GetPartitionConfig().GetWriteSpeedInBytesPerSecond() << "bps"
            << ", max: " << TSchemeShard::MaxPQWriteSpeedPerPartition << "bps";
        return nullptr;
    }

    const auto lifetimeSeconds = op.GetPQTabletConfig().GetPartitionConfig().GetLifetimeSeconds();
    if (lifetimeSeconds <= 0 || (ui32)lifetimeSeconds > TSchemeShard::MaxPQLifetimeSeconds) {
        status = NKikimrScheme::StatusInvalidParameter;
        errStr = TStringBuilder() << "Invalid retention period"
            << ": specified: " << lifetimeSeconds << "s"
            << ", min: " << 1 << "s"
            << ", max: " << TSchemeShard::MaxPQLifetimeSeconds << "s";
        return nullptr;
    }

    if (op.GetPQTabletConfig().PartitionKeySchemaSize()) {
        if (op.PartitionBoundariesSize() != (partitionCount - 1)) {
            status = NKikimrScheme::StatusInvalidParameter;
            errStr = Sprintf("Partition count and partition boundaries size mismatch: %lu, %u",
                op.PartitionBoundariesSize(), partitionCount);
            return nullptr;
        }

        TString error;
        if (!pqGroupInfo->FillKeySchema(op.GetPQTabletConfig(), error)) {
            status = NKikimrScheme::StatusSchemeError;
            errStr = Sprintf("Invalid key schema: %s", error.data());
            return nullptr;
        }
    } else {
        if (op.PartitionBoundariesSize()) {
            status = NKikimrScheme::StatusInvalidParameter;
            errStr = "Missing key schema with specified partition boundaries";
            return nullptr;
        }
    }

    bool splitMergeEnabled = AppData()->FeatureFlags.GetEnableTopicSplitMerge();

    TString prevBound;
    for (ui32 i = 0; i < partitionCount; ++i) {
        using TKeyRange = TTopicTabletInfo::TKeyRange;
        TMaybe<TKeyRange> keyRange;

        if (op.PartitionBoundariesSize() || (splitMergeEnabled && 1 < partitionCount)) {
            keyRange.ConstructInPlace();

            if (i) {
                keyRange->FromBound = prevBound;
            }

            if (i != (partitionCount - 1)) {
                if (op.PartitionBoundariesSize()) {
                    TVector<TCell> cells;
                    TString error;
                    TVector<TString> memoryOwner;
                    if (!NMiniKQL::CellsFromTuple(nullptr, op.GetPartitionBoundaries(i), pqGroupInfo->KeySchema, {},
                                                  false, cells, error, memoryOwner)) {
                        status = NKikimrScheme::StatusSchemeError;
                        errStr = Sprintf("Invalid partition boundary at position: %u, error: %s", i, error.data());
                        return nullptr;
                    }

                    cells.resize(pqGroupInfo->KeySchema.size()); // Extend with NULLs
                    keyRange->ToBound = TSerializedCellVec::Serialize(cells);
                } else { // splitMergeEnabled
                    auto range = NDataStreams::V1::RangeFromShardNumber(i, partitionCount);
                    keyRange->ToBound = NPQ::AsKeyBound(range.End);
                }

                prevBound = *keyRange->ToBound;
            }
        }

        pqGroupInfo->PartitionsToAdd.emplace(i, i + 1, keyRange);
    }

    if (partsPerTablet == 0 || partsPerTablet > TSchemeShard::MaxPQTabletPartitionsCount) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = Sprintf("Invalid partition per tablet count specified: %u", partsPerTablet);
        return nullptr;
    }

    pqGroupInfo->NextPartitionId = partitionCount;
    pqGroupInfo->MaxPartsPerTablet = partsPerTablet;

    pqGroupInfo->TotalGroupCount = partitionCount;
    pqGroupInfo->TotalPartitionCount = partitionCount;

    ui32 tabletCount = pqGroupInfo->ExpectedShardCount();
    if (tabletCount > TSchemeShard::MaxPQGroupTabletsCount) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = Sprintf("Invalid tablet count specified: %u", tabletCount);
        return nullptr;
    }

    NKikimrPQ::TPQTabletConfig tabletConfig = op.GetPQTabletConfig();
    tabletConfig.ClearPartitionIds();
    tabletConfig.ClearPartitions();

    if (!CheckPersQueueConfig(tabletConfig, false, &errStr)) {
        status = NKikimrScheme::StatusSchemeError;
        return nullptr;
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

    if (op.HasBootstrapConfig()) {
        Y_PROTOBUF_SUPPRESS_NODISCARD op.GetBootstrapConfig().SerializeToString(&pqGroupInfo->BootstrapConfig);
    }

    return pqGroupInfo;
}

void ApplySharding(TTxId txId,
                   TPathId pathId,
                   TTopicInfo::TPtr pqGroup,
                   TTxState& txState,
                   const TChannelsBindings& rbBindedChannels,
                   const TChannelsBindings& pqBindedChannels,
                   TSchemeShard* ss) {
    pqGroup->AlterVersion = 0;
    TShardInfo shardInfo = TShardInfo::PersQShardInfo(txId, pathId);
    shardInfo.BindedChannels = pqBindedChannels;
    Y_ABORT_UNLESS(pqGroup->TotalGroupCount == pqGroup->PartitionsToAdd.size());
    const ui64 count = pqGroup->ExpectedShardCount();
    txState.Shards.reserve(count + 1);
    const auto startShardIdx = ss->ReserveShardIdxs(count + 1);
    for (ui64 i = 0; i < count; ++i) {
        const auto idx = ss->NextShardIdx(startShardIdx, i);
        ss->RegisterShardInfo(idx, shardInfo);
        txState.Shards.emplace_back(idx, ETabletType::PersQueue, TTxState::CreateParts);

        TTopicTabletInfo::TPtr pqShard = new TTopicTabletInfo();
        pqShard->Partitions.reserve(pqGroup->MaxPartsPerTablet);
        pqGroup->Shards[idx] = pqShard;
    }

    const auto idx = ss->NextShardIdx(startShardIdx, count);
    ss->RegisterShardInfo(idx,
        TShardInfo::PQBalancerShardInfo(txId, pathId)
            .WithBindedChannels(rbBindedChannels));
    txState.Shards.emplace_back(idx, ETabletType::PersQueueReadBalancer, TTxState::CreateParts);
    pqGroup->BalancerShardIdx = idx;

    auto it = pqGroup->PartitionsToAdd.begin();
    for (ui32 pqId = 0; pqId < pqGroup->TotalGroupCount; ++pqId, ++it) {
        auto idx = ss->NextShardIdx(startShardIdx, pqId / pqGroup->MaxPartsPerTablet);
        auto partition = MakeHolder<TTopicTabletInfo::TTopicPartitionInfo>();
        partition->PqId = it->PartitionId;
        partition->GroupId = it->GroupId;
        partition->KeyRange = it->KeyRange;
        partition->AlterVersion = 1;
        partition->CreateVersion = 1;
        partition->Status = NKikimrPQ::ETopicPartitionStatus::Active;

        pqGroup->AddPartition(idx, partition.Release());
    }
    pqGroup->InitSplitMergeGraph();
}


class TCreatePQ: public TSubOperation {
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
        const auto& createDEscription = Transaction.GetCreatePersQueueGroup();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = createDEscription.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreatePQ Propose"
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
                .NotUnderDeleting();

            if (checks) {
                if (parentPath.Base()->IsCdcStream()) {
                    checks
                        .IsUnderCreating(NKikimrScheme::StatusNameConflict)
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else {
                    checks
                        .IsCommonSensePath()
                        .IsLikeDirectory();
                }
            }

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

        TTopicInfo::TPtr pqGroup = CreatePersQueueGroup(
            context, createDEscription, status, errStr);

        if (!pqGroup.Get()) {
            result->SetError(status, errStr);
            return result;
        }

        const ui64 shardsToCreate = pqGroup->ExpectedShardCount() + 1;
        const ui64 partitionsToCreate = pqGroup->TotalPartitionCount;

        auto tabletConfig = pqGroup->TabletConfig;
        NKikimrPQ::TPQTabletConfig config;
        Y_ABORT_UNLESS(!tabletConfig.empty());

        bool parseOk = ParseFromStringNoSizeLimit(config, tabletConfig);
        Y_ABORT_UNLESS(parseOk);

        const PQGroupReserve reserve(config, partitionsToCreate);

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
        const auto& partConfig = createDEscription.GetPQTabletConfig().GetPartitionConfig();
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

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreatePQGroup, pathId);

        ApplySharding(OperationId.GetTxId(), pathId, pqGroup, txState, tabletChannelsBinding, pqChannelsBinding, context.SS);

        NIceDb::TNiceDb db(context.GetDB());

        for (auto& shard : pqGroup->Shards) {
            auto shardIdx = shard.first;
            for (const auto& pqInfo : shard.second->Partitions) {
                context.SS->PersistPersQueue(db, pathId, shardIdx, *pqInfo);
            }
        }

        TTopicInfo::TPtr emptyGroup = new TTopicInfo;
        emptyGroup->Shards.swap(pqGroup->Shards);

        context.SS->Topics[pathId] = emptyGroup;
        context.SS->Topics[pathId]->AlterData = pqGroup;
        context.SS->IncrementPathDbRefCount(pathId);

        context.SS->PersistPersQueueGroup(db, pathId, emptyGroup);
        context.SS->PersistAddPersQueueGroupAlter(db, pathId, pqGroup);

        for (auto shard : txState.Shards) {
            Y_ABORT_UNLESS(shard.Operation == TTxState::CreateParts);
            context.SS->PersistShardMapping(db, shard.Idx, InvalidTabletId, pathId, OperationId.GetTxId(), shard.TabletType);
            context.SS->PersistChannelsBinding(db, shard.Idx, tabletChannelsBinding);
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

        if (parentPath.Base()->IsCdcStream()) {
            auto tablePath = parentPath.Parent();
            Y_ABORT_UNLESS(tablePath.IsResolved());

            Y_ABORT_UNLESS(context.SS->Tables.contains(tablePath.Base()->PathId));
            auto table = context.SS->Tables.at(tablePath.Base()->PathId);

            for (const auto& splitOpId : table->GetSplitOpsInFlight()) {
                context.OnComplete.Dependence(splitOpId.GetTxId(), OperationId.GetTxId());
            }
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->PersistTxState(db, OperationId);

        context.SS->PersistPath(db, dstPath.Base()->PathId);

        if (!acl.empty()) {
            dstPath.Base()->ApplyACL(acl);
            context.SS->PersistACL(db, dstPath.Base());
        }

        context.SS->PersistUpdateNextPathId(db);
        context.SS->PersistUpdateNextShardIdx(db);

        ++parentPath.Base()->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath.Base());
        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath.Base()->PathId);

        context.SS->ClearDescribePathCaches(dstPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, dstPath.Base()->PathId);

        dstPath.DomainInfo()->IncPathsInside();
        dstPath.DomainInfo()->AddInternalShards(txState);
        dstPath.DomainInfo()->IncPQPartitionsInside(partitionsToCreate);
        dstPath.DomainInfo()->IncPQReservedStorage(reserve.Storage);

        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_THROUGHPUT].Add(reserve.Throughput);
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Add(reserve.Storage);

        context.SS->TabletCounters->Simple()[COUNTER_STREAM_SHARDS_COUNT].Add(partitionsToCreate);

        dstPath.Base()->IncShardsInside(shardsToCreate);
        parentPath.Base()->IncAliveChildren();

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TCreatePQ");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreatePQ AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateNewPQ(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreatePQ>(id, tx);
}

ISubOperation::TPtr CreateNewPQ(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreatePQ>(id, state);
}

}
