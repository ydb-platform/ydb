#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/sharding/sharding.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {
class ISSEntity;
class ISSEntityEvolution;
class TEntityInitializationContext;
class TEvolutionInitializationContext;
}

namespace NKikimr::NSchemeShard {

struct TColumnTableInfo {
public:
    using TPtr = std::shared_ptr<TColumnTableInfo>;

    ui64 AlterVersion = 0;
    TPtr AlterData;

    TPathId GetOlapStorePathIdVerified() const {
        AFL_VERIFY(!IsStandalone());
        return PathIdFromPathId(Description.GetColumnStorePathId());
    }

    std::shared_ptr<NSharding::IShardingBase> GetShardingVerified(const TOlapSchema& olapSchema) const {
        return NSharding::IShardingBase::BuildFromProto(olapSchema, Description.GetSharding()).DetachResult();
    }

    std::set<ui64> GetShardIdsSet() const {
        return std::set<ui64>(Description.GetSharding().GetColumnShards().begin(), Description.GetSharding().GetColumnShards().end());
    }

    const auto& GetColumnShards() const {
        return Description.GetSharding().GetColumnShards();
    }

    void SetColumnShards(const std::vector<ui64>& columnShards) {
        AFL_VERIFY(GetColumnShards().empty())("original", Description.DebugString());
        AFL_VERIFY(columnShards.size());

        Description.MutableSharding()->MutableColumnShards()->Clear();
        Description.MutableSharding()->MutableColumnShards()->Reserve(columnShards.size());
        for (ui64 columnShard : columnShards) {
            Description.MutableSharding()->AddColumnShards(columnShard);
        }
    }

    NKikimrSchemeOp::TColumnTableDescription Description;
    TMaybe<NKikimrSchemeOp::TColumnStoreSharding> StandaloneSharding;
    TMaybe<NKikimrSchemeOp::TAlterColumnTable> AlterBody;

    TAggregatedStats Stats;

    TColumnTableInfo() = default;
    TColumnTableInfo(ui64 alterVersion, const NKikimrSchemeOp::TColumnTableDescription& description,
        TMaybe<NKikimrSchemeOp::TColumnStoreSharding>&& standaloneSharding,
        TMaybe<NKikimrSchemeOp::TAlterColumnTable>&& alterBody = Nothing());

    const NKikimrSchemeOp::TColumnStoreSharding& GetStandaloneShardingVerified() const {
        AFL_VERIFY(!!StandaloneSharding);
        return *StandaloneSharding;
    }

    const auto& GetOwnedColumnShardsVerified() const {
        AFL_VERIFY(IsStandalone());
        return StandaloneSharding->GetColumnShards();
    }

    std::vector<TShardIdx> BuildOwnedColumnShardsVerified() const {
        std::vector<TShardIdx> result;
        for (auto&& i : GetOwnedColumnShardsVerified()) {
            result.emplace_back(TShardIdx::BuildFromProto(i).DetachResult());
        }
        return result;
    }

    void SetOlapStorePathId(const TPathId& pathId) {
        Description.MutableColumnStorePathId()->SetOwnerId(pathId.OwnerId);
        Description.MutableColumnStorePathId()->SetLocalId(pathId.LocalPathId);
    }

    static TColumnTableInfo::TPtr BuildTableWithAlter(const TColumnTableInfo& initialTable, const NKikimrSchemeOp::TAlterColumnTable& alterBody);

    bool IsStandalone() const {
        return !!StandaloneSharding;
    }

    const TAggregatedStats& GetStats() const {
        return Stats;
    }

    void UpdateShardStats(const TShardIdx shardIdx, const TPartitionStats& newStats) {
        Stats.Aggregated.PartCount = GetColumnShards().size();
        Stats.PartitionStats[shardIdx]; // insert if none
        Stats.UpdateShardStats(shardIdx, newStats);
    }

    void UpdateTableStats(const TPathId& pathId, const TPartitionStats& newStats) {
        Stats.UpdateTableStats(pathId, newStats);
    }

    TConclusion<std::shared_ptr<NOlap::NAlter::ISSEntity>> BuildEntity(const TPathId& pathId, const NOlap::NAlter::TEntityInitializationContext& iContext) const;

    TConclusion<std::shared_ptr<NOlap::NAlter::ISSEntityEvolution>> BuildEvolution(const TPathId& pathId, const NOlap::NAlter::TEvolutionInitializationContext& iContext) const;
};

}