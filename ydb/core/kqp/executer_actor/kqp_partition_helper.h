#pragma once

#include "kqp_tasks_graph.h"


#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <util/generic/variant.h>

namespace NKikimr::NKqp {

struct TShardInfo {
    struct TColumnWriteInfo {
        ui32 MaxValueSizeBytes = 0;
    };

    TMaybe<TShardKeyRanges> KeyReadRanges;  // empty -> no reads
    TMaybe<TShardKeyRanges> KeyWriteRanges; // empty -> no writes
    THashMap<TString, TColumnWriteInfo> ColumnWrites;

    TString ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const;
};

class TShardInfoWithId: public TShardInfo {
public:
    ui64 ShardId;
    TShardInfoWithId(const ui64 shardId, TShardInfo&& base)
        : TShardInfo(std::move(base))
        , ShardId(shardId)
    {

    }
};

struct TPhysicalShardReadSettings {
    bool Sorted = true;
    bool Reverse = false;
    ui64 ItemsLimit = 0;
    NKikimr::NMiniKQL::TType* ResultType = nullptr;
};

TSerializedTableRange MakeKeyRange(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyKeyRange& range, const TStageInfo& stageInfo, const NMiniKQL::THolderFactory& holderFactory,
    const NMiniKQL::TTypeEnvironment& typeEnv);

TVector<TSerializedPointOrRange> FillReadRanges(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyOpReadOlapRanges& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::TTypeEnvironment& typeEnv);

TVector<TSerializedPointOrRange> FillReadRanges(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyOpReadRanges& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::TTypeEnvironment& typeEnv);

THashMap<ui64, TShardInfo> PrunePartitions(const NKqpProto::TKqpPhyOpReadRange& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv, bool& isFullScan);

THashMap<ui64, TShardInfo> PrunePartitions(const NKqpProto::TKqpPhyOpReadRanges& readRanges, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv, bool& isFullScan);

THashMap<ui64, TShardInfo> PrunePartitions(const NKqpProto::TKqpPhyOpLookup& lookup, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv, bool& isFullScan);

std::pair<ui64, TShardInfo> MakeVirtualTablePartition(const NKqpProto::TKqpReadRangesSource& source, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

THashMap<ui64, TShardInfo> PrunePartitions(const NKqpProto::TKqpReadRangesSource& source, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv, bool& isFullScan);

ui64 ExtractItemsLimit(const TStageInfo& stageInfo, const NKqpProto::TKqpPhyValue& protoItemsLimit,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

// Returns the list of ColumnShards that can store rows from the specified range
// NOTE: Unlike OLTP tables that store data in DataShards, data in OLAP tables is not range
// partitioned and multiple ColumnShards store data from the same key range
THashMap<ui64, TShardInfo> PrunePartitions(const NKqpProto::TKqpPhyOpReadOlapRanges& readRanges, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv, bool& isFullScan);

THashMap<ui64, TShardInfo> PrunePartitions(const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv, bool& isFullScan);

THashMap<ui64, TShardInfo> PruneEffectPartitions(const NKqpProto::TKqpPhyOpUpsertRows& effect, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

THashMap<ui64, TShardInfo> PruneEffectPartitions(const NKqpProto::TKqpPhyOpDeleteRows& effect, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

THashMap<ui64, TShardInfo> PruneEffectPartitions(const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

TPhysicalShardReadSettings ExtractReadSettings(
    const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

} // namespace NKikimr::NKqp
