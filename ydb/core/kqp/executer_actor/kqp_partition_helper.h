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

TSerializedTableRange MakeKeyRange(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyKeyRange& range, const TStageInfo& stageInfo, const NMiniKQL::THolderFactory& holderFactory,
    const NMiniKQL::TTypeEnvironment& typeEnv);

TVector<TSerializedPointOrRange> FillReadRanges(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyOpReadOlapRanges& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::TTypeEnvironment& typeEnv);

TVector<TSerializedPointOrRange> FillReadRanges(const TVector<NScheme::TTypeInfo>& keyColumnTypes,
    const NKqpProto::TKqpPhyOpReadRanges& readRange, const TStageInfo& stageInfo,
    const NMiniKQL::TTypeEnvironment& typeEnv);

std::pair<ui64, TShardInfo> MakeVirtualTablePartition(const NKqpProto::TKqpReadRangesSource& source, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

THashMap<ui64, TShardInfo> PrunePartitions(const NKqpProto::TKqpReadRangesSource& source, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv, bool& isFullScan);

ui64 ExtractItemsLimit(const TStageInfo& stageInfo, const NKqpProto::TKqpPhyValue& protoItemsLimit,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

TPhysicalShardReadSettings ExtractReadSettings(
    const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo,
    const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv);

// Returns true if parallel point read is possible for the given partitions
// for EnableParallelPointReadConsolidation settings
bool IsParallelPointReadPossible(const THashMap<ui64, TShardInfo>& partitions);

struct TPartitionPrunerConfig {
};

class TPartitionPruner {
public:
    TPartitionPruner(const NMiniKQL::THolderFactory& holderFactory, const NMiniKQL::TTypeEnvironment& typeEnv, TPartitionPrunerConfig config = {});

    THashMap<ui64, TShardInfo> Prune(const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo, bool& isFullScan);

    THashMap<ui64, TShardInfo> Prune(const NKqpProto::TKqpReadRangesSource& source, const TStageInfo& stageInfo, bool& isFullScan);

    THashMap<ui64, TShardInfo> PruneEffect(const NKqpProto::TKqpPhyTableOperation& operation, const TStageInfo& stageInfo);

private:
    const NMiniKQL::THolderFactory* HolderFactory;
    const NMiniKQL::TTypeEnvironment* TypeEnv;
    const TPartitionPrunerConfig Config;
};

} // namespace NKikimr::NKqp
