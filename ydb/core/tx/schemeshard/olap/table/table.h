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

    TPathId GetOlapStorePathIdVerified() const;

    std::shared_ptr<NSharding::IShardingBase> GetShardingVerified(const TOlapSchema& olapSchema) const;

    std::set<ui64> GetShardIdsSet() const;

    const google::protobuf::RepeatedField<arc_ui64>& GetColumnShards() const;

    void SetColumnShards(const std::vector<ui64>& columnShards);

    THashSet<TString> GetUsedTiers() const;

    NKikimrSchemeOp::TColumnTableDescription Description;
    TMaybe<NKikimrSchemeOp::TColumnStoreSharding> StandaloneSharding;
    TMaybe<NKikimrSchemeOp::TAlterColumnTable> AlterBody;
    NKikimrSchemeOp::TBackupTask BackupSettings;
    TMap<TTxId, TTableInfo::TBackupRestoreResult> BackupHistory;

    TAggregatedStats Stats;

    TColumnTableInfo() = default;
    TColumnTableInfo(ui64 alterVersion, const NKikimrSchemeOp::TColumnTableDescription& description,
        TMaybe<NKikimrSchemeOp::TColumnStoreSharding>&& standaloneSharding,
        TMaybe<NKikimrSchemeOp::TAlterColumnTable>&& alterBody = Nothing());

    const NKikimrSchemeOp::TColumnStoreSharding& GetStandaloneShardingVerified() const;

    const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TShardIdx>& GetOwnedColumnShardsVerified() const;

    std::vector<TShardIdx> BuildOwnedColumnShardsVerified() const;

    void SetOlapStorePathId(const TPathId& pathId);

    static TColumnTableInfo::TPtr BuildTableWithAlter(const TColumnTableInfo& initialTable, const NKikimrSchemeOp::TAlterColumnTable& alterBody);

    bool IsStandalone() const {
        return !!StandaloneSharding;
    }

    const TAggregatedStats& GetStats() const {
        return Stats;
    }

    void UpdateShardStats(TDiskSpaceUsageDelta* diskSpaceUsageDelta, const TShardIdx shardIdx, const TPartitionStats& newStats, TInstant now);

    void UpdateTableStats(const TShardIdx shardIdx, const TPathId& pathId, const TPartitionStats& newStats, TInstant now);

    TConclusion<std::shared_ptr<NOlap::NAlter::ISSEntity>> BuildEntity(const TPathId& pathId, const NOlap::NAlter::TEntityInitializationContext& iContext) const;

    TConclusion<std::shared_ptr<NOlap::NAlter::ISSEntityEvolution>> BuildEvolution(const TPathId& pathId, const NOlap::NAlter::TEvolutionInitializationContext& iContext) const;
};

}
