#pragma once

#include "schemeshard_info_types_base.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/util/pb.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/guid.h>

namespace NKikimr {
namespace NSchemeShard {

struct TTopicStats {
    TMessageSeqNo SeqNo;

    ui64 DataSize = 0;
    ui64 UsedReserveSize = 0;

    TString ToString() const {
        return TStringBuilder() << "TTopicStats {"
                                << " DataSize: " << DataSize
                                << " UsedReserveSize: " << UsedReserveSize
                                << " }";
    }
};

struct TTopicTabletInfo : TSimpleRefCount<TTopicTabletInfo> {
    using TPtr = TIntrusivePtr<TTopicTabletInfo>;
    using TKeySchema = TVector<NScheme::TTypeInfo>;

    struct TKeyRange {
        TMaybe<TString> FromBound;
        TMaybe<TString> ToBound;

        void SerializeToProto(NKikimrPQ::TPartitionKeyRange& proto) const;
        void DeserializeFromProto(const NKikimrPQ::TPartitionKeyRange& proto);
    };

    struct TTopicPartitionInfo {
        // Partition id
        ui32 PqId = 0;
        ui32 GroupId = 0;
        // AlterVersion of topic which partition was updated last.
        ui64 AlterVersion = 0;
        // AlterVersion of topic which partition was created.
        // For example, it required for generate "timebased" offsets for kinesis protocol.
        ui64 CreateVersion;

        NKikimrPQ::ETopicPartitionStatus Status = NKikimrPQ::ETopicPartitionStatus::Active;

        TMaybe<TKeyRange> KeyRange;

        // Split and merge operations form the partitions graph. Each partition created in this way has a parent
        // partition. In turn, the parent partition knows about the partitions formed after the split and merge
        // operations.
        TSet<ui32> ParentPartitionIds;
        TSet<ui32> ChildPartitionIds;

        TShardIdx ShardIdx;
        TInstant CreationTimestamp;

        void SetStatus(const TActorContext& ctx, ui32 value) {
            if (value >= NKikimrPQ::ETopicPartitionStatus::Active &&
                value <= NKikimrPQ::ETopicPartitionStatus::Deleted) {
                Status = static_cast<NKikimrPQ::ETopicPartitionStatus>(value);
            } else {
                LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Read unknown topic partition status value " << value);
                Status = NKikimrPQ::ETopicPartitionStatus::Active;
            }
        }
    };

    TVector<TAutoPtr<TTopicPartitionInfo>> Partitions;

    size_t PartsCount() const {
        return Partitions.size();
    }
};

/**
 * TTopicInfo -> TTopicTabletInfo -> TTopicPartitionInfo
 *
 * Each topic may contains many tablets.
 * Each tablet may serve many partitions.
 */
struct TTopicInfo : TSimpleRefCount<TTopicInfo> {
    using TPtr = TIntrusivePtr<TTopicInfo>;
    using TKeySchema = TTopicTabletInfo::TKeySchema;

    struct TPartitionToAdd {
        using TKeyRange = TTopicTabletInfo::TKeyRange;

        ui32 PartitionId;
        ui32 GroupId;
        TMaybe<TKeyRange> KeyRange;
        THashSet<ui32> ParentPartitionIds;

        explicit TPartitionToAdd(ui32 partitionId, ui32 groupId, const TMaybe<TKeyRange>& keyRange = Nothing(),
                                 const THashSet<ui32>& parentPartitionIds = {})
            : PartitionId(partitionId)
            , GroupId(groupId)
            , KeyRange(keyRange)
            , ParentPartitionIds(parentPartitionIds) {
        }

        bool operator==(const TPartitionToAdd& rhs) const {
            return PartitionId == rhs.PartitionId
                && GroupId == rhs.GroupId;
        }
    };

    ui64 TotalGroupCount = 0;
    ui64 TotalPartitionCount = 0;
    ui32 NextPartitionId = 0;
    TVector<TPartitionToAdd> PartitionsToAdd;
    THashSet<ui32> PartitionsToDelete;
    THashMap<ui32, TMaybe<TTopicTabletInfo::TKeyRange>> KeyRangesToChange;
    ui32 MaxPartsPerTablet = 0;
    ui64 AlterVersion = 0;
    TString TabletConfig;
    TString BootstrapConfig;
    THashMap<TShardIdx, TTopicTabletInfo::TPtr> Shards; // key - shardIdx
    TKeySchema KeySchema;
    TTopicInfo::TPtr AlterData; // changes to be applied
    TTabletId BalancerTabletID = InvalidTabletId;
    TShardIdx BalancerShardIdx = InvalidShardIdx;
    THashMap<ui32, TTopicTabletInfo::TTopicPartitionInfo*> Partitions;
    size_t ActivePartitionCount = 0;
    THashSet<ui32> OffloadDonePartitions;

    TString PreSerializedPathDescription; // Cached path description
    TString PreSerializedPartitionsDescription; // Cached partition description

    TTopicStats Stats;

    void AddPartition(TShardIdx shardIdx, TTopicTabletInfo::TTopicPartitionInfo* partition) {
        partition->ShardIdx = shardIdx;

        TTopicTabletInfo::TPtr& pqShard = Shards[shardIdx];
        if (!pqShard) {
            pqShard.Reset(new TTopicTabletInfo());
        }
        pqShard->Partitions.push_back(partition);
        Partitions[partition->PqId] = pqShard->Partitions.back().Get();
    }

    void UpdateSplitMergeGraph(const TTopicTabletInfo::TTopicPartitionInfo& partition) {
        for (const auto parent : partition.ParentPartitionIds) {
            auto it = Partitions.find(parent);
            Y_ENSURE(it != Partitions.end(),
                     "Partition " << partition.GroupId << " has parent partition " << parent << " which doesn't exists");
            it->second->ChildPartitionIds.emplace(partition.PqId);
        }
    }

    void InitSplitMergeGraph() {
        for (const auto& [_, partition] : Partitions) {
            UpdateSplitMergeGraph(*partition);
        }
    }

    bool SupportSplitMerge() {
        return KeySchema.empty();
    }

    bool FillKeySchema(const NKikimrPQ::TPQTabletConfig& tabletConfig, TString& error);
    bool FillKeySchema(const TString& tabletConfig);

    bool HasBalancer() const { return bool(BalancerTabletID); }

    ui32 ExpectedShardCount() const {

        Y_ENSURE(TotalPartitionCount);
        Y_ENSURE(MaxPartsPerTablet);

        ui32 partsPerTablet = MaxPartsPerTablet;
        ui32 pqTabletCount = TotalPartitionCount / partsPerTablet;
        if (TotalPartitionCount % partsPerTablet) {
            ++pqTabletCount;
        }
        return pqTabletCount;
    }

    ui32 ShardCount() const {
        return Shards.size();
    }

    TVector<std::pair<TShardIdx, TTopicTabletInfo::TTopicPartitionInfo*>> GetPartitions() {
        TVector<std::pair<TShardIdx, TTopicTabletInfo::TTopicPartitionInfo*>> partitions;
        partitions.reserve(TotalPartitionCount);

        for (auto& [shardIdx, tabletInfo] : Shards) {
            for (const auto& partitionInfo : tabletInfo->Partitions) {
                partitions.push_back({shardIdx, partitionInfo.Get()});
            }
        }

        std::sort(partitions.begin(), partitions.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.second->PqId < rhs.second->PqId;
        });

        return partitions;
    }

    NKikimrPQ::TPQTabletConfig GetTabletConfig() const {
        NKikimrPQ::TPQTabletConfig tabletConfig;
        if (!TabletConfig.empty()) {
            bool parseOk = ParseFromStringNoSizeLimit(tabletConfig, TabletConfig);
            Y_ENSURE(parseOk, "Previously serialized pq tablet config cannot be parsed");
        }
        return tabletConfig;
    }

    void PrepareAlter(TTopicInfo::TPtr alterData) {
        Y_ENSURE(alterData, "No alter data at Alter prepare");
        alterData->AlterVersion = AlterVersion + 1;
        Y_ENSURE(alterData->TotalGroupCount);
        Y_ENSURE(alterData->TotalPartitionCount);
        Y_ENSURE(0 < alterData->ActivePartitionCount && alterData->ActivePartitionCount <= alterData->TotalPartitionCount);
        Y_ENSURE(alterData->NextPartitionId);
        Y_ENSURE(alterData->MaxPartsPerTablet);
        alterData->KeySchema = KeySchema;
        alterData->BalancerTabletID = BalancerTabletID;
        alterData->BalancerShardIdx = BalancerShardIdx;
        AlterData = alterData;
    }

    void FinishAlter() {
        Y_ENSURE(AlterData, "No alter data at Alter complete");
        TotalGroupCount = AlterData->TotalGroupCount;
        NextPartitionId = AlterData->NextPartitionId;
        TotalPartitionCount = AlterData->TotalPartitionCount;
        ActivePartitionCount = AlterData->ActivePartitionCount;
        MaxPartsPerTablet = AlterData->MaxPartsPerTablet;
        if (!AlterData->TabletConfig.empty())
            TabletConfig = std::move(AlterData->TabletConfig);
        ++AlterVersion;
        Y_ENSURE(BalancerTabletID == AlterData->BalancerTabletID || !HasBalancer());
        Y_ENSURE(AlterData->HasBalancer());
        Y_ENSURE(AlterData->BalancerShardIdx);
        KeySchema = AlterData->KeySchema;
        BalancerTabletID = AlterData->BalancerTabletID;
        BalancerShardIdx = AlterData->BalancerShardIdx;
        AlterData.Reset();

        Partitions.clear();
        for (const auto& [_, shard] : Shards) {
            for (auto& partition : shard->Partitions) {
                Partitions[partition->PqId] = partition.Get();
            }
        }

        InitSplitMergeGraph();
    }
};

}
}
