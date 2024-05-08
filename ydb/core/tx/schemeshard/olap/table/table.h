#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {
class ISSEntity;
class ISSEntityEvolution;
class TEntityInitializationContext;
class TEvolutionInitializationContext;
}

namespace NKikimr::NSchemeShard {

struct TColumnTableInfo {
private:
    std::optional<NOlap::NAlter::TEvolutions> Evolutions;
public:
    using TPtr = std::shared_ptr<TColumnTableInfo>;

    ui64 AlterVersion = 0;

    bool IsInModification() const {
        return !!Evolutions;
    }

    std::set<ui64> GetShardIdsSet() const {
        return std::set<ui64>(Description.GetSharding().GetColumnShards().begin(), Description.GetSharding().GetColumnShards().end());
    }

    void CleanEvolutions() {
        AFL_VERIFY(!!Evolutions);
        AFL_VERIFY(!Evolutions->HasEvolutions());
        Evolutions.reset();
    }

    void SetEvolutions(std::optional<NOlap::NAlter::TEvolutions>&& evolutions) {
        AFL_VERIFY(!!Evolutions);
        Evolutions = std::move(evolutions);
    }

    ui32 GetShardsCount() const {
        return Description.GetSharding().GetColumnShards().size();
    }

    ui64 GetTabletId(const ui32 idx) const {
        AFL_VERIFY(idx < GetShardsCount());
        return Description.GetSharding().GetColumnShards()[idx];
    }

    TPathId GetOlapStorePathIdVerified() const {
        AFL_VERIFY(!IsStandalone());
        return PathIdFromPathId(Description.GetColumnStorePathId());
    }

    const auto& GetColumnShards() const {
        return Description.GetSharding().GetColumnShards();
    }

    void SetColumnShards(const std::vector<ui64>& columnShards) {
        AFL_VERIFY(GetColumnShards().empty())("original", Description.DebugString());
        AFL_VERIFY(columnShards.size());
        Description.MutableSharding()->SetVersion(1);

        Description.MutableSharding()->MutableColumnShards()->Clear();
        Description.MutableSharding()->MutableColumnShards()->Reserve(columnShards.size());
        for (ui64 columnShard : columnShards) {
            Description.MutableSharding()->AddColumnShards(columnShard);
        }
    }

    const NKikimrSchemeOp::TAlterColumnTable& GetAlterBodyVerified() const {
        AFL_VERIFY(!!Evolutions);
        AFL_VERIFY(Evolutions->GetAlterProtoOriginal().HasAlterColumnTable());
        return Evolutions->GetAlterProtoOriginal().GetAlterColumnTable();
    }

    const NOlap::NAlter::TEvolutions& GetEvolutionsVerified() const {
        AFL_VERIFY(!!Evolutions);
        return *Evolutions;
    }

    NOlap::NAlter::TEvolutions& GetEvolutionsVerified() {
        AFL_VERIFY(!!Evolutions);
        return *Evolutions;
    }

    std::shared_ptr<NOlap::NAlter::ISSEntityEvolution> GetCurrentEvolution() const {
        return GetEvolutionsVerified().GetCurrentEvolution();
    }

    std::shared_ptr<NOlap::NAlter::ISSEntityEvolution> ExtractCurrentEvolution() {
        return GetEvolutionsVerified().ExtractCurrentEvolution();
    }

    NKikimrSchemeOp::TColumnTableDescription Description;
    TMaybe<NKikimrSchemeOp::TColumnStoreSharding> StandaloneSharding;

    TAggregatedStats Stats;

    TColumnTableInfo() = default;
    TColumnTableInfo(ui64 alterVersion, NKikimrSchemeOp::TColumnTableDescription&& description,
        TMaybe<NKikimrSchemeOp::TColumnStoreSharding>&& standaloneSharding);

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
};

}