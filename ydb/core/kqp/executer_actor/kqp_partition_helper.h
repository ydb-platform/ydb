#pragma once

#include "kqp_tasks_graph.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

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

using TShardIdToInfoMap = THashMap<ui64 /* shardId */, TShardInfo>;

class TShardInfoWithId: public TShardInfo {
public:
    ui64 ShardId;
    TShardInfoWithId(const ui64 shardId, TShardInfo&& base)
        : TShardInfo(std::move(base))
        , ShardId(shardId) {

    }
};

struct TPhysicalShardReadSettings: public NYql::TSortingOperator<NYql::ERequestSorting::ASC> {
    ui64 ItemsLimit = 0;
    NKikimr::NMiniKQL::TType* ResultType = nullptr;
};

struct TPartitionPrunerConfig {
    TMaybe<TSerializedTableRange> BatchOperationRange;
};

class TPartitionPruner {
public:
    TPartitionPruner(const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv, TPartitionPrunerConfig config = {});

    TShardIdToInfoMap Prune(const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo, bool& isFullScan);

    const TShardIdToInfoMap& Prune(const NKqpProto::TKqpReadRangesSource& source, const TStageInfo& stageInfo, bool& isFullScan);

    TShardIdToInfoMap PruneEffect(const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo);

    std::pair<ui64, TShardInfo> MakeVirtualTablePartition(const NKqpProto::TKqpReadRangesSource& source, const TStageInfo& stageInfo);

private:
    const NMiniKQL::THolderFactory* HolderFactory;
    const NMiniKQL::TTypeEnvironment* TypeEnv;
    const TPartitionPrunerConfig Config;

    THashMap<NYql::NDq::TStageId, std::pair<TShardIdToInfoMap, bool /* isFullScan*/ >> SourceScanStageIdToParititions;
};

TSerializedTableRange MakeKeyRange(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyKeyRange& range, const TStageInfo& stageInfo, const NMiniKQL::THolderFactory& holderFactory,
    const NMiniKQL::TTypeEnvironment& typeEnv);

TVector<TSerializedPointOrRange> FillRangesFromParameter(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyParamValue& rangesParam, const TStageInfo& stageInfo,
    const NMiniKQL::TTypeEnvironment& typeEnv);

TVector<TSerializedPointOrRange> FillReadRanges(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyOpReadOlapRanges& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::TTypeEnvironment& typeEnv);

TVector<TSerializedPointOrRange> FillReadRanges(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyOpReadRanges& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::TTypeEnvironment& typeEnv);

TShardIdToInfoMap PrunePartitions(const NKqpProto::TKqpPhyOpReadRange& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
    const TPartitionPrunerConfig& prunerConfig, bool& isFullScan);

TShardIdToInfoMap PrunePartitions(const NKqpProto::TKqpPhyOpReadRanges& readRanges, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
    const TPartitionPrunerConfig& prunerConfig, bool& isFullScan);

std::pair<ui64, TShardInfo> MakeVirtualTablePartition(const NKqpProto::TKqpReadRangesSource& source, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

TShardIdToInfoMap PrunePartitions(const NKqpProto::TKqpReadRangesSource& source, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
    const TPartitionPrunerConfig& prunerConfig, bool& isFullScan);

NUdf::TUnboxedValue ExtractPhyValue(const TStageInfo& stageInfo, const NKqpProto::TKqpPhyValue& protoItemsLimit,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
    const NUdf::TUnboxedValue& defaultValue);

// Returns the list of ColumnShards that can store rows from the specified range
// NOTE: Unlike OLTP tables that store data in DataShards, data in OLAP tables is not range
// partitioned and multiple ColumnShards store data from the same key range
TShardIdToInfoMap PrunePartitions(const NKqpProto::TKqpPhyOpReadOlapRanges& readRanges, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
    const TPartitionPrunerConfig& prunerConfig, bool& isFullScan);

TShardIdToInfoMap PrunePartitions(const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
    const TPartitionPrunerConfig& prunerConfig, bool& isFullScan);

TShardIdToInfoMap PruneEffectPartitions(const NKqpProto::TKqpPhyOpUpsertRows& effect, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
    const TPartitionPrunerConfig& prunerConfig);

TShardIdToInfoMap PruneEffectPartitions(const NKqpProto::TKqpPhyOpDeleteRows& effect, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
    const TPartitionPrunerConfig& prunerConfig);

TShardIdToInfoMap PruneEffectPartitions(const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv,
    const TPartitionPrunerConfig& prunerConfig);

TPhysicalShardReadSettings ExtractReadSettings(
    const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

} // namespace NKikimr::NKqp
