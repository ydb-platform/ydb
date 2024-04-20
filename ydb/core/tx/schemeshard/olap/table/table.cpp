#include "table.h"

namespace NKikimr::NSchemeShard {

TColumnTableInfo::TColumnTableInfo(
    ui64 alterVersion,
    NKikimrSchemeOp::TColumnTableDescription&& description,
    NKikimrSchemeOp::TColumnTableSharding&& sharding,
    TMaybe<NKikimrSchemeOp::TColumnStoreSharding>&& standaloneSharding,
    TMaybe<NKikimrSchemeOp::TAlterColumnTable>&& alterBody)
    : AlterVersion(alterVersion)
    , Description(std::move(description))
    , Sharding(std::move(sharding))
    , StandaloneSharding(std::move(standaloneSharding))
    , AlterBody(std::move(alterBody)) {
    if (Description.HasColumnStorePathId()) {
        OlapStorePathId = TPathId(
            TOwnerId(Description.GetColumnStorePathId().GetOwnerId()),
            TLocalPathId(Description.GetColumnStorePathId().GetLocalId()));
    }

    if (Description.HasSchema()) {
        TOlapSchema schema;
        schema.ParseFromLocalDB(Description.GetSchema());
    }

    ColumnShards.reserve(Sharding.GetColumnShards().size());
    for (ui64 columnShard : Sharding.GetColumnShards()) {
        ColumnShards.push_back(columnShard);
    }

    if (StandaloneSharding) {
        OwnedColumnShards.reserve(StandaloneSharding->GetColumnShards().size());
        for (const auto& shardIdx : StandaloneSharding->GetColumnShards()) {
            OwnedColumnShards.push_back(TShardIdx(
                TOwnerId(shardIdx.GetOwnerId()),
                TLocalShardIdx(shardIdx.GetLocalId())));
        }
    }
}

TColumnTableInfo::TPtr TColumnTableInfo::BuildTableWithAlter(const TColumnTableInfo& initialTable, const NKikimrSchemeOp::TAlterColumnTable& alterBody) {
    TColumnTableInfo::TPtr alterData = std::make_shared<TColumnTableInfo>(initialTable);
    alterData->AlterBody.ConstructInPlace(alterBody);
    ++alterData->AlterVersion;
    return alterData;
}

}