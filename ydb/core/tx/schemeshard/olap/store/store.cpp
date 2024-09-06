#include "store.h"

#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NSchemeShard {

TConclusion<TOlapStoreInfo::TLayoutInfo> TOlapStoreInfo::ILayoutPolicy::Layout(const TColumnTablesLayout& currentLayout, const ui32 shardsCount) const {
    auto result = DoLayout(currentLayout, shardsCount);
    if (result.IsFail()) {
        return result;
    }
    AFL_VERIFY(result->GetTabletIds().size() == shardsCount);
    return result;
}

TConclusion<TOlapStoreInfo::TLayoutInfo> TOlapStoreInfo::TIdentityGroupsLayout::DoLayout(const TColumnTablesLayout& currentLayout, const ui32 shardsCount) const {
    for (auto&& i : currentLayout.GetGroups()) {
        if (i.GetTableIds().Size() == 0 && i.GetShardIds().size() >= shardsCount) {
            return TOlapStoreInfo::TLayoutInfo(std::vector<ui64>(i.GetShardIds().begin(), std::next(i.GetShardIds().begin(), shardsCount)), true);
        }
        if (i.GetShardIds().size() != shardsCount) {
            continue;
        }
        return TOlapStoreInfo::TLayoutInfo(std::vector<ui64>(i.GetShardIds().begin(), i.GetShardIds().end()), false);
    }
    return TConclusionStatus::Fail("cannot find appropriate group for " + ::ToString(shardsCount) + " shards");
}

TConclusion<TOlapStoreInfo::TLayoutInfo> TOlapStoreInfo::TMinimalTablesCountLayout::DoLayout(const TColumnTablesLayout& currentLayout, const ui32 shardsCount) const {
    bool isNewGroup = true;
    std::vector<ui64> resultLocal;
    for (auto&& i : currentLayout.GetGroups()) {
        if (i.GetTableIds().Size() > 0) {
            isNewGroup = false;
        }
        for (auto&& s : i.GetShardIds()) {
            resultLocal.emplace_back(s);
            if (resultLocal.size() == shardsCount) {
                return TOlapStoreInfo::TLayoutInfo(std::move(resultLocal), isNewGroup);
            }
        }
    }
    return TConclusionStatus::Fail("cannot find appropriate group for " + ::ToString(shardsCount) + " shards");
}

TOlapStoreInfo::TOlapStoreInfo(
    ui64 alterVersion,
    NKikimrSchemeOp::TColumnStoreSharding&& sharding,
    TMaybe<NKikimrSchemeOp::TAlterColumnStore>&& alterBody)
    : AlterVersion(alterVersion)
    , Sharding(std::move(sharding))
    , AlterBody(std::move(alterBody)) {
    for (const auto& shardIdx : Sharding.GetColumnShards()) {
        ColumnShards.push_back(TShardIdx(
            TOwnerId(shardIdx.GetOwnerId()),
            TLocalShardIdx(shardIdx.GetLocalId())));
    }
}

TOlapStoreInfo::TPtr TOlapStoreInfo::BuildStoreWithAlter(const TOlapStoreInfo& initialStore, const NKikimrSchemeOp::TAlterColumnStore& alterBody) {
    TOlapStoreInfo::TPtr alterData = std::make_shared<TOlapStoreInfo>(initialStore);
    alterData->AlterVersion++;
    alterData->AlterBody.ConstructInPlace(alterBody);
    return alterData;
}

void TOlapStoreInfo::SerializeDescription(NKikimrSchemeOp::TColumnStoreDescription& descriptionProto) const {
    descriptionProto.SetName(Name);
    descriptionProto.SetColumnShardCount(ColumnShards.size());
    descriptionProto.SetNextSchemaPresetId(NextSchemaPresetId);
    descriptionProto.SetNextTtlSettingsPresetId(NextTtlSettingsPresetId);

    for (const auto& [name, preset] : SchemaPresets) {
        Y_UNUSED(name);
        auto presetProto = descriptionProto.AddSchemaPresets();
        preset.Serialize(*presetProto);
    }
}

void TOlapStoreInfo::ParseFromLocalDB(const NKikimrSchemeOp::TColumnStoreDescription& descriptionProto) {
    StorageConfig = descriptionProto.GetStorageConfig();
    NextTtlSettingsPresetId = descriptionProto.GetNextTtlSettingsPresetId();
    NextSchemaPresetId = descriptionProto.GetNextSchemaPresetId();
    Name = descriptionProto.GetName();

    size_t schemaPresetIndex = 0;
    for (const auto& presetProto : descriptionProto.GetSchemaPresets()) {
        Y_ABORT_UNLESS(!SchemaPresets.contains(presetProto.GetId()));
        auto& preset = SchemaPresets[presetProto.GetId()];
        preset.ParseFromLocalDB(presetProto);
        preset.SetProtoIndex(schemaPresetIndex++);
        SchemaPresetByName[preset.GetName()] = preset.GetId();
    }
    SerializeDescription(Description);
}

bool TOlapStoreInfo::UpdatePreset(const TString& presetName, const TOlapSchemaUpdate& schemaUpdate, IErrorCollector& errors) {
    const ui32 presetId = SchemaPresetByName.at(presetName);
    auto& currentPreset = SchemaPresets.at(presetId);
    if (!currentPreset.Update(schemaUpdate, errors)) {
        return false;
    }

    NKikimrSchemeOp::TColumnTableSchemaPreset schemaUpdateProto;
    currentPreset.Serialize(schemaUpdateProto);

    auto mutablePresetProto = Description.MutableSchemaPresets(currentPreset.GetProtoIndex());
    *mutablePresetProto = schemaUpdateProto;
    return true;
}

bool TOlapStoreInfo::ParseFromRequest(const NKikimrSchemeOp::TColumnStoreDescription& descriptionProto, IErrorCollector& errors) {
    AlterVersion = 1;
    if (descriptionProto.GetRESERVED_MetaShardCount() != 0) {
        errors.AddError("trying to create OLAP store with meta shards (not supported yet)");
        return false;
    }

    if (!descriptionProto.HasColumnShardCount()) {
        errors.AddError("trying to create OLAP store without shards number specified");
        return false;
    }

    if (descriptionProto.GetColumnShardCount() == 0) {
        errors.AddError("trying to create OLAP store without zero shards");
        return false;
    }

    for (auto& presetProto : descriptionProto.GetRESERVED_TtlSettingsPresets()) {
        Y_UNUSED(presetProto);
        errors.AddError("TTL presets are not supported");
        return false;
    }

    Name = descriptionProto.GetName();
    StorageConfig = descriptionProto.GetStorageConfig();
    // Make it easier by having data channel count always specified internally
    if (!StorageConfig.HasDataChannelCount()) {
        StorageConfig.SetDataChannelCount(64);
    }

    size_t protoIndex = 0;
    for (const auto& presetProto : descriptionProto.GetSchemaPresets()) {
        TOlapStoreSchemaPreset preset;
        if (!preset.ParseFromRequest(presetProto, errors)) {
            return false;
        }
        if (SchemaPresets.contains(NextSchemaPresetId) || SchemaPresetByName.contains(preset.GetName())) {
            errors.AddError(TStringBuilder() << "Duplicate schema preset " << NextSchemaPresetId << " with name '" << preset.GetName() << "'");
            return false;
        }
        preset.SetId(NextSchemaPresetId++);
        preset.SetProtoIndex(protoIndex++);

        TOlapSchemaUpdate schemaDiff;
        if (!schemaDiff.Parse(presetProto.GetSchema(), errors)) {
            return false;
        }

        if (!preset.Update(schemaDiff, errors)) {
            return false;
        }

        SchemaPresetByName[preset.GetName()] = preset.GetId();
        SchemaPresets[preset.GetId()] = std::move(preset);
    }

    if (!SchemaPresetByName.contains("default") || SchemaPresets.size() > 1) {
        errors.AddError("A single schema preset named 'default' is required");
        return false;
    }

    ColumnShards.resize(descriptionProto.GetColumnShardCount());
    SerializeDescription(Description);
    return true;
}

TOlapStoreInfo::ILayoutPolicy::TPtr TOlapStoreInfo::GetTablesLayoutPolicy() const {
    ILayoutPolicy::TPtr result;
    if (AppData()->ColumnShardConfig.GetTablesStorageLayoutPolicy().HasMinimalTables()) {
        result = std::make_shared<TMinimalTablesCountLayout>();
    } else if (AppData()->ColumnShardConfig.GetTablesStorageLayoutPolicy().HasIdentityGroups()) {
        result = std::make_shared<TIdentityGroupsLayout>();
    } else {
        result = std::make_shared<TMinimalTablesCountLayout>();
    }
    return result;
}

}
