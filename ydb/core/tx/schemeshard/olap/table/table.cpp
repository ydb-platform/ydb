#include "table.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/standalone/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/object.h>
#include <ydb/core/tx/columnshard/common/protos/snapshot.pb.h>

namespace NKikimr::NSchemeShard {

TPathId TColumnTableInfo::GetOlapStorePathIdVerified() const {
    AFL_VERIFY(!IsStandalone());
    return TPathId::FromProto(Description.GetColumnStorePathId());
}

std::shared_ptr<NSharding::IShardingBase> TColumnTableInfo::GetShardingVerified(const TOlapSchema& olapSchema) const {
    return NSharding::IShardingBase::BuildFromProto(olapSchema, Description.GetSharding()).DetachResult();
}

std::set<ui64> TColumnTableInfo::GetShardIdsSet() const {
    return std::set<ui64>(Description.GetSharding().GetColumnShards().begin(), Description.GetSharding().GetColumnShards().end());
}

const google::protobuf::RepeatedField<arc_ui64>& TColumnTableInfo::GetColumnShards() const {
    return Description.GetSharding().GetColumnShards();
}

void TColumnTableInfo::SetColumnShards(const std::vector<ui64>& columnShards) {
    AFL_VERIFY(GetColumnShards().empty())("original", Description.DebugString());
    AFL_VERIFY(columnShards.size());

    Description.MutableSharding()->MutableColumnShards()->Clear();
    Description.MutableSharding()->MutableColumnShards()->Reserve(columnShards.size());
    for (ui64 columnShard : columnShards) {
        Description.MutableSharding()->AddColumnShards(columnShard);
    }
}

THashSet<TString> TColumnTableInfo::GetUsedTiers() const {
    THashSet<TString> tiers;
    for (const auto& tier : Description.GetTtlSettings().GetEnabled().GetTiers()) {
        if (tier.HasEvictToExternalStorage()) {
            tiers.emplace(tier.GetEvictToExternalStorage().GetStorage());
        }
    }
    return tiers;
}

TColumnTableInfo::TColumnTableInfo(
    ui64 alterVersion,
    const NKikimrSchemeOp::TColumnTableDescription& description,
    TMaybe<NKikimrSchemeOp::TColumnStoreSharding>&& standaloneSharding,
    TMaybe<NKikimrSchemeOp::TAlterColumnTable>&& alterBody)
    : AlterVersion(alterVersion)
    , Description(description)
    , StandaloneSharding(std::move(standaloneSharding))
    , AlterBody(std::move(alterBody))
{
}

const NKikimrSchemeOp::TColumnStoreSharding& TColumnTableInfo::GetStandaloneShardingVerified() const {
    AFL_VERIFY(!!StandaloneSharding);
    return *StandaloneSharding;
}

const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TShardIdx>& TColumnTableInfo::GetOwnedColumnShardsVerified() const {
    AFL_VERIFY(IsStandalone());
    return StandaloneSharding->GetColumnShards();
}

std::vector<TShardIdx> TColumnTableInfo::BuildOwnedColumnShardsVerified() const {
    std::vector<TShardIdx> result;
    for (auto&& i : GetOwnedColumnShardsVerified()) {
        result.emplace_back(TShardIdx::BuildFromProto(i).DetachResult());
    }
    return result;
}

void TColumnTableInfo::SetOlapStorePathId(const TPathId& pathId) {
    Description.MutableColumnStorePathId()->SetOwnerId(pathId.OwnerId);
    Description.MutableColumnStorePathId()->SetLocalId(pathId.LocalPathId);
}

TColumnTableInfo::TPtr TColumnTableInfo::BuildTableWithAlter(const TColumnTableInfo& initialTable, const NKikimrSchemeOp::TAlterColumnTable& alterBody) {
    TColumnTableInfo::TPtr alterData = std::make_shared<TColumnTableInfo>(initialTable);
    alterData->AlterBody.ConstructInPlace(alterBody);
    ++alterData->AlterVersion;
    return alterData;
}

void TColumnTableInfo::UpdateShardStats(TDiskSpaceUsageDelta* diskSpaceUsageDelta, const TShardIdx shardIdx, const TPartitionStats& newStats, TInstant now) {
    Stats.Aggregated.PartCount = GetColumnShards().size();
    Stats.PartitionStats[shardIdx]; // insert if none
    Stats.UpdateShardStats(diskSpaceUsageDelta, shardIdx, newStats, now);
}

void TColumnTableInfo::UpdateTableStats(const TShardIdx shardIdx, const TPathId& pathId, const TPartitionStats& newStats, TInstant now) {
    Stats.TableStats[pathId].Aggregated.PartCount = GetColumnShards().size();
    Stats.UpdateTableStats(shardIdx, pathId, newStats, now);
}

TConclusion<std::shared_ptr<NOlap::NAlter::ISSEntity>> TColumnTableInfo::BuildEntity(const TPathId& pathId, const NOlap::NAlter::TEntityInitializationContext& iContext) const {
    std::shared_ptr<NOlap::NAlter::ISSEntity> result;
    if (IsStandalone()) {
        result = std::make_shared<NOlap::NAlter::TStandaloneTable>(pathId);
    } else {
        result = std::make_shared<NOlap::NAlter::TInStoreTable>(pathId);
    }
    auto initConclusion = result->Initialize(iContext);
    if (initConclusion.IsFail()) {
        return initConclusion;
    }
    return result;
}

}