#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

namespace NKikimr::NSchemeShard {

struct TColumnTableInfo {
private:
    YDB_READONLY_DEF(std::vector<ui64>, ColumnShards);
public:
    using TPtr = std::shared_ptr<TColumnTableInfo>;

    ui64 AlterVersion = 0;
    TPtr AlterData;

    void SetColumnShards(std::vector<ui64>&& columnShards) {
        AFL_VERIFY(ColumnShards.empty());
        ColumnShards = std::move(columnShards);

        Sharding.SetVersion(1);

        Sharding.MutableColumnShards()->Clear();
        Sharding.MutableColumnShards()->Reserve(ColumnShards.size());
        for (ui64 columnShard : ColumnShards) {
            Sharding.AddColumnShards(columnShard);
        }
    }

    NKikimrSchemeOp::TColumnTableDescription Description;
    NKikimrSchemeOp::TColumnTableSharding Sharding;
    TMaybe<NKikimrSchemeOp::TColumnStoreSharding> StandaloneSharding;
    TMaybe<NKikimrSchemeOp::TAlterColumnTable> AlterBody;

    TMaybe<TPathId> OlapStorePathId; // PathId of the table store

    std::vector<TShardIdx> OwnedColumnShards;
    TAggregatedStats Stats;

    TColumnTableInfo() = default;
    TColumnTableInfo(ui64 alterVersion, NKikimrSchemeOp::TColumnTableDescription&& description,
        NKikimrSchemeOp::TColumnTableSharding&& sharding,
        TMaybe<NKikimrSchemeOp::TColumnStoreSharding>&& standaloneSharding,
        TMaybe<NKikimrSchemeOp::TAlterColumnTable>&& alterBody = Nothing());

    void SetOlapStorePathId(const TPathId& pathId) {
        OlapStorePathId = pathId;
        Description.MutableColumnStorePathId()->SetOwnerId(pathId.OwnerId);
        Description.MutableColumnStorePathId()->SetLocalId(pathId.LocalPathId);
    }

    static TColumnTableInfo::TPtr BuildTableWithAlter(const TColumnTableInfo& initialTable, const NKikimrSchemeOp::TAlterColumnTable& alterBody);

    bool IsStandalone() const {
        return !OwnedColumnShards.empty();
    }

    const TAggregatedStats& GetStats() const {
        return Stats;
    }

    void UpdateShardStats(const TShardIdx shardIdx, const TPartitionStats& newStats) {
        Stats.Aggregated.PartCount = ColumnShards.size();
        Stats.PartitionStats[shardIdx]; // insert if none
        Stats.UpdateShardStats(shardIdx, newStats);
    }

    void UpdateTableStats(const TPathId& pathId, const TPartitionStats& newStats) {
        Stats.UpdateTableStats(pathId, newStats);
    }
};

}