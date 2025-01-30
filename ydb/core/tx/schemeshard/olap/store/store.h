#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/layout/layout.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <util/system/types.h>

namespace NKikimr::NSchemeShard {

struct TOlapStoreInfo {
private:
    TString Name;
    ui64 NextSchemaPresetId = 1;
    ui64 NextTtlSettingsPresetId = 1;
    NKikimrSchemeOp::TColumnStorageConfig StorageConfig;
    NKikimrSchemeOp::TColumnStoreDescription Description;
    ui64 AlterVersion = 0;
public:
    using TPtr = std::shared_ptr<TOlapStoreInfo>;

    class TLayoutInfo {
    private:
        YDB_ACCESSOR_DEF(std::vector<ui64>, TabletIds);
        YDB_READONLY(bool, IsNewGroup, false);
    public:
        TLayoutInfo(std::vector<ui64>&& ids, const bool isNewGroup)
            : TabletIds(std::move(ids))
            , IsNewGroup(isNewGroup)
        {

        }
    };

    class ILayoutPolicy {
    protected:
        virtual TConclusion<TLayoutInfo> DoLayout(const TColumnTablesLayout& currentLayout, const ui32 shardsCount) const = 0;
    public:
        using TPtr = std::shared_ptr<ILayoutPolicy>;
        virtual ~ILayoutPolicy() = default;
        TConclusion<TLayoutInfo> Layout(const TColumnTablesLayout& currentLayout, const ui32 shardsCount) const;
    };

    class TMinimalTablesCountLayout: public ILayoutPolicy {
    protected:
        virtual TConclusion<TLayoutInfo> DoLayout(const TColumnTablesLayout& currentLayout, const ui32 shardsCount) const override;
    };

    class TIdentityGroupsLayout: public ILayoutPolicy {
    protected:
        virtual TConclusion<TLayoutInfo> DoLayout(const TColumnTablesLayout& currentLayout, const ui32 shardsCount) const override;
    };

    TPtr AlterData;

    const NKikimrSchemeOp::TColumnStoreDescription& GetDescription() const {
        return Description;
    }

    NKikimrSchemeOp::TColumnStoreSharding Sharding;
    TMaybe<NKikimrSchemeOp::TAlterColumnStore> AlterBody;

    TVector<TShardIdx> ColumnShards;

    THashMap<ui32, TOlapStoreSchemaPreset> SchemaPresets;
    THashMap<TString, ui32> SchemaPresetByName;

    THashSet<TPathId> ColumnTables;
    THashSet<TPathId> ColumnTablesUnderOperation;
    TAggregatedStats Stats;

    TOlapStoreInfo() = default;
    TOlapStoreInfo(ui64 alterVersion,
        NKikimrSchemeOp::TColumnStoreSharding&& sharding,
        TMaybe<NKikimrSchemeOp::TAlterColumnStore>&& alterBody = Nothing());

    static TOlapStoreInfo::TPtr BuildStoreWithAlter(const TOlapStoreInfo& initialStore, const NKikimrSchemeOp::TAlterColumnStore& alterBody);

    const NKikimrSchemeOp::TColumnStorageConfig& GetStorageConfig() const {
        return StorageConfig;
    }

    const TVector<TShardIdx>& GetColumnShards() const {
        return ColumnShards;
    }

    ui64 GetAlterVersion() const {
        return AlterVersion;
    }

    void ApplySharding(const TVector<TShardIdx>& shardsIndexes) {
        Y_ABORT_UNLESS(ColumnShards.size() == shardsIndexes.size());
        Sharding.ClearColumnShards();
        for (ui64 i = 0; i < ColumnShards.size(); ++i) {
            const auto& idx = shardsIndexes[i];
            ColumnShards[i] = idx;
            auto* shardInfoProto = Sharding.AddColumnShards();
            shardInfoProto->SetOwnerId(idx.GetOwnerId());
            shardInfoProto->SetLocalId(idx.GetLocalId().GetValue());
        }
    }
    void SerializeDescription(NKikimrSchemeOp::TColumnStoreDescription& descriptionProto) const;
    void ParseFromLocalDB(const NKikimrSchemeOp::TColumnStoreDescription& descriptionProto);
    bool ParseFromRequest(const NKikimrSchemeOp::TColumnStoreDescription& descriptionProto, IErrorCollector& errors);
    bool UpdatePreset(const TString& presetName, const TOlapSchemaUpdate& schemaUpdate, IErrorCollector& errors);

    const TAggregatedStats& GetStats() const {
        return Stats;
    }

    ILayoutPolicy::TPtr GetTablesLayoutPolicy() const;

    void UpdateShardStats(TShardIdx shardIdx, const TPartitionStats& newStats) {
        Stats.Aggregated.PartCount = ColumnShards.size();
        Stats.PartitionStats[shardIdx]; // insert if none
        Stats.UpdateShardStats(shardIdx, newStats);
    }
};

}