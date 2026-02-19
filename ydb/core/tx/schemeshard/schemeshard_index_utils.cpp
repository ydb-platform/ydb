#include "schemeshard_info_types.h"
#include "schemeshard_index_utils.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/persqueue/public/utils.h>

namespace NKikimr {
namespace NTableIndex {

TIndexObjectCounts GetIndexObjectCounts(const NKikimrSchemeOp::TIndexCreationConfig& indexDesc) {
    TIndexObjectCounts res;
    res.IndexTableCount = 1;
    res.SequenceCount = 0;
    res.IndexTableShards = 0;
    res.ShardsPerPath = 1;
    switch (GetIndexType(indexDesc)) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
            const bool prefixVectorIndex = indexDesc.GetKeyColumnNames().size() > 1;
            res.IndexTableCount = (prefixVectorIndex ? 3 : 2);
            res.SequenceCount = (prefixVectorIndex ? 1 : 0);
            break;
        }
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain: {
            res.IndexTableCount = 1;
            break;
        }
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance: {
            res.IndexTableCount = 4;
            break;
        }
        default:
            Y_DEBUG_ABORT_S(NTableIndex::InvalidIndexType(indexDesc.GetType()));
            break;
    }
    if (indexDesc.IndexImplTableDescriptionsSize() == res.IndexTableCount) {
        for (const auto& indexTableDesc: indexDesc.GetIndexImplTableDescriptions()) {
            auto implShards = NSchemeShard::TTableInfo::ShardsToCreate(indexTableDesc);
            res.IndexTableShards += implShards;
            if (res.ShardsPerPath < implShards) {
                res.ShardsPerPath = implShards;
            }
        }
    } else {
        res.IndexTableShards = res.IndexTableCount;
    }
    return res;
}

TTableColumns ExtractInfo(const NKikimrSchemeOp::TTableDescription &tableDescr) {
    TTableColumns result;
    for (auto& column: tableDescr.GetColumns()) {
        result.Columns.insert(column.GetName());
    }
    for (auto& keyName: tableDescr.GetKeyColumnNames()) {
        result.Keys.push_back(keyName);
    }
    return result;
}

TIndexColumns ExtractInfo(const NKikimrSchemeOp::TIndexCreationConfig &indexDesc) {
    TIndexColumns result;
    for (auto& keyName: indexDesc.GetKeyColumnNames()) {
        result.KeyColumns.push_back(keyName);
    }
    for (auto& keyName: indexDesc.GetDataColumnNames()) {
        result.DataColumns.push_back(keyName);
    }
    return result;
}

TTableColumns ExtractInfo(const NSchemeShard::TTableInfo::TPtr &tableInfo) {
    TTableColumns result;
    for (auto& item: tableInfo->Columns) {
        const auto& column = item.second;
        if (column.IsDropped()) {
            continue;
        }

        result.Columns.insert(item.second.Name);
    }

    for (auto& keyId: tableInfo->KeyColumnIds) {
        const auto& keyColumn = tableInfo->Columns.at(keyId);
        if (keyColumn.IsDropped()) {
            continue;
        }

        Y_ABORT_UNLESS(result.Columns.contains(keyColumn.Name));
        result.Keys.push_back(keyColumn.Name);
    }

    return result;
}

namespace {

NKikimrSchemeOp::TPartitionConfig PartitionConfigForIndexes(
        const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
        const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    // KIKIMR-6687
    NKikimrSchemeOp::TPartitionConfig result;

    if (baseTablePartitionConfig.HasNamedCompactionPolicy()) {
        result.SetNamedCompactionPolicy(baseTablePartitionConfig.GetNamedCompactionPolicy());
    }
    if (baseTablePartitionConfig.HasCompactionPolicy()) {
        result.MutableCompactionPolicy()->CopyFrom(baseTablePartitionConfig.GetCompactionPolicy());
    }
    // skip optional uint64 FollowerCount = 3;
    if (baseTablePartitionConfig.HasExecutorCacheSize()) {
        result.SetExecutorCacheSize(baseTablePartitionConfig.GetExecutorCacheSize());
    }
    // skip     optional bool AllowFollowerPromotion = 5 [default = true];
    if (baseTablePartitionConfig.HasTxReadSizeLimit()) {
        result.SetTxReadSizeLimit(baseTablePartitionConfig.GetTxReadSizeLimit());
    }
    // skip optional uint32 CrossDataCenterFollowerCount = 8;
    if (baseTablePartitionConfig.HasChannelProfileId()) {
        result.SetChannelProfileId(baseTablePartitionConfig.GetChannelProfileId());
    }

    if (indexTableDesc.GetPartitionConfig().HasPartitioningPolicy()) {
        result.MutablePartitioningPolicy()->CopyFrom(indexTableDesc.GetPartitionConfig().GetPartitioningPolicy());
    } else {
        result.MutablePartitioningPolicy()->SetSizeToSplit(2_GB);
        result.MutablePartitioningPolicy()->SetMinPartitionsCount(1);
    }
    if (baseTablePartitionConfig.HasPipelineConfig()) {
        result.MutablePipelineConfig()->CopyFrom(baseTablePartitionConfig.GetPipelineConfig());
    }
    if (baseTablePartitionConfig.ColumnFamiliesSize()) {
        // Indexes don't need column families unless it's the default column family
        for (const auto& family : baseTablePartitionConfig.GetColumnFamilies()) {
            const bool isDefaultFamily = (
                (!family.HasId() && !family.HasName()) ||
                (family.HasId() && family.GetId() == 0) ||
                (family.HasName() && family.GetName() == "default"));
            if (isDefaultFamily) {
                result.AddColumnFamilies()->CopyFrom(family);
            }
        }
    }
    if (baseTablePartitionConfig.HasResourceProfile()) {
        result.SetResourceProfile(baseTablePartitionConfig.GetResourceProfile());
    }
    if (baseTablePartitionConfig.HasDisableStatisticsCalculation()) {
        result.SetDisableStatisticsCalculation(baseTablePartitionConfig.GetDisableStatisticsCalculation());
    }
    if (baseTablePartitionConfig.HasEnableFilterByKey()) {
        result.SetEnableFilterByKey(baseTablePartitionConfig.GetEnableFilterByKey());
    }
    if (baseTablePartitionConfig.HasExecutorFastLogPolicy()) {
        result.SetExecutorFastLogPolicy(baseTablePartitionConfig.GetExecutorFastLogPolicy());
    }
    if (baseTablePartitionConfig.HasEnableEraseCache()) {
        result.SetEnableEraseCache(baseTablePartitionConfig.GetEnableEraseCache());
    }
    if (baseTablePartitionConfig.HasEraseCacheMinRows()) {
        result.SetEraseCacheMinRows(baseTablePartitionConfig.GetEraseCacheMinRows());
    }
    if (baseTablePartitionConfig.HasEraseCacheMaxBytes()) {
        result.SetEraseCacheMaxBytes(baseTablePartitionConfig.GetEraseCacheMaxBytes());
    }
    if (baseTablePartitionConfig.HasKeepSnapshotTimeout()) {
        result.SetKeepSnapshotTimeout(baseTablePartitionConfig.GetKeepSnapshotTimeout());
    }
    if (indexTableDesc.GetPartitionConfig().FollowerGroupsSize()) {
        result.MutableFollowerGroups()->CopyFrom(indexTableDesc.GetPartitionConfig().GetFollowerGroups());
    }
    // skip repeated NKikimrStorageSettings.TStorageRoom StorageRooms = 17;

    return result;
}

void SetImplTablePartitionConfig(
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    NKikimrSchemeOp::TTableDescription& tableDescription)
{
    if (indexTableDesc.HasUniformPartitionsCount()) {
        tableDescription.SetUniformPartitionsCount(indexTableDesc.GetUniformPartitionsCount());
    }

    if (indexTableDesc.SplitBoundarySize()) {
        tableDescription.MutableSplitBoundary()->CopyFrom(indexTableDesc.GetSplitBoundary());
    }

    *tableDescription.MutablePartitionConfig() = PartitionConfigForIndexes(baseTablePartitionConfig, indexTableDesc);
}

void FillIndexImplTableColumns(
    const auto& baseTableColumns,
    std::span<const TString> keys,
    const THashSet<TString>& columns,
    NKikimrSchemeOp::TTableDescription& implTableDesc)
{
    // The function that calls this may have already added some columns
    // and we want to add new columns after those that have already been added
    const auto was = implTableDesc.ColumnsSize();

    THashMap<TString, ui32> implKeyToImplColumn;
    for (ui32 keyId = 0; keyId < keys.size(); ++keyId) {
        implKeyToImplColumn[keys[keyId]] = keyId;
    }

    // We want data columns order in index table same as in indexed table,
    // so we use counter to keep this order in the std::sort
    // Counter starts with Max/2 to avoid intersection with key columns counter
    for (ui32 i = Max<ui32>() / 2; auto& columnIt: baseTableColumns) {
        NKikimrSchemeOp::TColumnDescription* column = nullptr;
        using TColumn = std::decay_t<decltype(columnIt)>;
        if constexpr (std::is_same_v<TColumn, std::pair<const ui32, NSchemeShard::TTableInfo::TColumn>>) {
            const auto& columnInfo = columnIt.second;
            if (!columnInfo.IsDropped() && columns.contains(columnInfo.Name)) {
                column = implTableDesc.AddColumns();
                column->SetName(columnInfo.Name);
                column->SetType(NScheme::TypeName(columnInfo.PType, columnInfo.PTypeMod));
                column->SetNotNull(columnInfo.NotNull);
            }
        } else if constexpr (std::is_same_v<TColumn, NKikimrSchemeOp::TColumnDescription>) {
            if (columns.contains(columnIt.GetName())) {
                column = implTableDesc.AddColumns();
                *column = columnIt;
                column->ClearFamily();
                column->ClearFamilyName();
                column->ClearDefaultValue();
            }
        } else {
            static_assert(dependent_false<TColumn>::value);
        }
        if (column) {
            ui32 order = i++;
            if (const auto* id = implKeyToImplColumn.FindPtr(column->GetName())) {
                order = *id;
            }
            column->SetId(order);
        }
    }

    std::sort(implTableDesc.MutableColumns()->begin() + was,
              implTableDesc.MutableColumns()->end(),
              [] (auto& left, auto& right) {
                  return left.GetId() < right.GetId();
              });

    for (auto& column: *implTableDesc.MutableColumns()) {
        column.ClearId();
    }

    for (const auto& keyName: keys) {
        implTableDesc.AddKeyColumnNames(keyName);
    }
}

bool GetIsRestore(const NSchemeShard::TTableInfo::TPtr& tableInfo) {
    return tableInfo->IsRestore;
}

const auto& GetPartitionConfig(const NSchemeShard::TTableInfo::TPtr& tableInfo) {
    return tableInfo->PartitionConfig();
}

const auto& GetColumns(const NSchemeShard::TTableInfo::TPtr& tableInfo) {
    return tableInfo->Columns;
}

bool GetIsRestore(const NKikimrSchemeOp::TTableDescription& tableDescr) {
    return tableDescr.GetIsRestore();
}

const auto& GetPartitionConfig(const NKikimrSchemeOp::TTableDescription& tableDescr) {
    return tableDescr.GetPartitionConfig();
}

const auto& GetColumns(const NKikimrSchemeOp::TTableDescription& tableDescr) {
    return tableDescr.GetColumns();
}

auto CalcImplTableDescImpl(
    const auto& baseTable,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    NKikimrSchemeOp::TTableDescription implTableDesc;
    implTableDesc.SetName(NTableIndex::ImplTable);
    implTableDesc.SetIsRestore(GetIsRestore(baseTable));
    SetImplTablePartitionConfig(GetPartitionConfig(baseTable), indexTableDesc, implTableDesc);
    FillIndexImplTableColumns(GetColumns(baseTable), implTableColumns.Keys, implTableColumns.Columns, implTableDesc);
    if (indexTableDesc.HasReplicationConfig()) {
        implTableDesc.MutableReplicationConfig()->CopyFrom(indexTableDesc.GetReplicationConfig());
    }

    return implTableDesc;
}

auto CalcVectorKmeansTreePostingImplTableDescImpl(
    const auto& baseTable,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    std::string_view suffix,
    bool withForeign)
{
    auto tableColumns = ExtractInfo(baseTable);
    THashSet<TString> indexColumns = indexDataColumns;
    for (const auto & keyColumn: tableColumns.Keys) {
        indexColumns.insert(keyColumn);
    }

    NKikimrSchemeOp::TTableDescription implTableDesc;
    implTableDesc.SetName(TString::Join(NKMeans::PostingTable, suffix));
    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);
    {
        auto col = implTableDesc.AddColumns();
        col->SetName(NKMeans::ParentColumn);
        col->SetType(NTableIndex::NKMeans::ClusterIdTypeName);
        col->SetTypeId(NSchemeShard::ClusterIdTypeId);
        col->SetNotNull(true);
    }
    if (withForeign) {
        auto col = implTableDesc.AddColumns();
        col->SetName(NKMeans::IsForeignColumn);
        col->SetType(NTableIndex::NKMeans::IsForeignTypeName);
        col->SetTypeId(NTableIndex::NKMeans::IsForeignType);
        col->SetNotNull(true);
    }
    implTableDesc.AddKeyColumnNames(NKMeans::ParentColumn);
    FillIndexImplTableColumns(GetColumns(baseTable), tableColumns.Keys, indexColumns, implTableDesc);

    implTableDesc.SetSystemColumnNamesAllowed(true);

    return implTableDesc;
}

auto CalcVectorKmeansTreePrefixImplTableDescImpl(
    const THashSet<TString>& indexKeyColumns,
    const auto& baseTable,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc
) {
    NKikimrSchemeOp::TTableDescription implTableDesc;
    implTableDesc.SetName(NKMeans::PrefixTable);
    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);
    auto keys = implTableColumns.Keys;
    std::erase_if(keys, [&](const auto& key) { return !indexKeyColumns.contains(key); });
    FillIndexImplTableColumns(GetColumns(baseTable), keys, indexKeyColumns, implTableDesc);
    {
        auto idColumn = implTableDesc.AddColumns();
        idColumn->SetName(NKMeans::IdColumn);
        idColumn->SetType(NTableIndex::NKMeans::ClusterIdTypeName);
        idColumn->SetTypeId(NSchemeShard::ClusterIdTypeId);
        idColumn->SetNotNull(true);
        idColumn->SetDefaultFromSequence(NKMeans::IdColumnSequence);
    }
    implTableDesc.AddKeyColumnNames(NKMeans::IdColumn);

    implTableDesc.SetSystemColumnNamesAllowed(true);

    return implTableDesc;
}

auto CalcVectorKmeansTreeBuildOverlapTableDescImpl(
    const auto& baseTable,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    std::string_view suffix)
{
    auto tableColumns = ExtractInfo(baseTable);
    THashSet<TString> indexColumns = indexDataColumns;
    for (const auto & keyColumn: tableColumns.Keys) {
        indexColumns.insert(keyColumn);
    }

    NKikimrSchemeOp::TTableDescription implTableDesc;
    implTableDesc.SetName(TString::Join(NKMeans::PostingTable, suffix));
    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);

    {
        auto col = implTableDesc.AddColumns();
        col->SetName(NKMeans::ParentColumn);
        col->SetType(NTableIndex::NKMeans::ClusterIdTypeName);
        col->SetTypeId(NSchemeShard::ClusterIdTypeId);
        col->SetNotNull(true);
    }
    {
        auto col = implTableDesc.AddColumns();
        col->SetName(NKMeans::DistanceColumn);
        col->SetType(NTableIndex::NKMeans::DistanceTypeName);
        col->SetTypeId(NTableIndex::NKMeans::DistanceType);
        col->SetNotNull(true);
    }
    {
        auto col = implTableDesc.AddColumns();
        col->SetName(NKMeans::IsForeignColumn);
        col->SetType(NTableIndex::NKMeans::IsForeignTypeName);
        col->SetTypeId(NTableIndex::NKMeans::IsForeignType);
        col->SetNotNull(true);
    }

    FillIndexImplTableColumns(GetColumns(baseTable), tableColumns.Keys, indexColumns, implTableDesc);
    // ParentColumn in the end of the key
    implTableDesc.AddKeyColumnNames(NKMeans::ParentColumn);

    implTableDesc.SetSystemColumnNamesAllowed(true);

    return implTableDesc;
}

auto CalcFulltextImplTableDescImpl(
    const auto& baseTable,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc,
    bool withFreq)
{
    auto tableColumns = ExtractInfo(baseTable);
    THashSet<TString> indexColumns;
    if (!withFreq) {
        indexColumns = indexDataColumns;
    }
    for (const auto & keyColumn: tableColumns.Keys) {
        indexColumns.insert(keyColumn);
    }

    TColumnTypes baseColumnTypes;
    TString error;
    Y_ENSURE(ExtractTypes(baseTable, baseColumnTypes, error), error);
    Y_ENSURE(indexDesc.GetSettings().columns().size() == 1);
    auto textColumnInfo = baseColumnTypes.at(indexDesc.GetSettings().columns().at(0).column());

    NKikimrSchemeOp::TTableDescription implTableDesc;
    implTableDesc.SetName(NTableIndex::ImplTable);
    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);
    {
        auto tokenColumn = implTableDesc.AddColumns();
        tokenColumn->SetName(NFulltext::TokenColumn);
        tokenColumn->SetType(NScheme::TypeName(textColumnInfo.GetTypeId()));
        tokenColumn->SetTypeId(textColumnInfo.GetTypeId());
        tokenColumn->SetNotNull(true);
    }
    implTableDesc.AddKeyColumnNames(NFulltext::TokenColumn);
    FillIndexImplTableColumns(GetColumns(baseTable), tableColumns.Keys, indexColumns, implTableDesc);
    if (withFreq) {
        auto col = implTableDesc.AddColumns();
        col->SetName(NFulltext::FreqColumn);
        col->SetType(NFulltext::TokenCountTypeName);
        col->SetTypeId(NFulltext::TokenCountType);
        col->SetNotNull(true);
    }

    implTableDesc.SetSystemColumnNamesAllowed(true);

    return implTableDesc;
}

auto CalcFulltextDocsImplTableDescImpl(
    const auto& baseTable,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    auto tableColumns = ExtractInfo(baseTable);
    THashSet<TString> indexColumns = indexDataColumns;
    for (const auto & keyColumn: tableColumns.Keys) {
        indexColumns.insert(keyColumn);
    }

    NKikimrSchemeOp::TTableDescription implTableDesc;
    implTableDesc.SetName(NTableIndex::NFulltext::DocsTable);
    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);
    FillIndexImplTableColumns(GetColumns(baseTable), tableColumns.Keys, indexColumns, implTableDesc);
    {
        auto col = implTableDesc.AddColumns();
        col->SetName(NFulltext::DocLengthColumn);
        col->SetType(NFulltext::TokenCountTypeName);
        col->SetTypeId(NFulltext::TokenCountType);
        col->SetNotNull(true);
    }

    implTableDesc.SetSystemColumnNamesAllowed(true);

    return implTableDesc;
}

auto CalcFulltextDictImplTableDescImpl(
    const auto& baseTable,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc)
{
    auto tableColumns = ExtractInfo(baseTable);

    TColumnTypes baseColumnTypes;
    TString error;
    Y_ENSURE(ExtractTypes(baseTable, baseColumnTypes, error), error);
    Y_ENSURE(indexDesc.GetSettings().columns().size() == 1);
    auto textColumnInfo = baseColumnTypes.at(indexDesc.GetSettings().columns().at(0).column());

    NKikimrSchemeOp::TTableDescription implTableDesc;
    implTableDesc.SetName(NTableIndex::NFulltext::DictTable);
    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);
    {
        auto col = implTableDesc.AddColumns();
        col->SetName(NFulltext::TokenColumn);
        col->SetType(NScheme::TypeName(textColumnInfo.GetTypeId()));
        col->SetTypeId(textColumnInfo.GetTypeId());
        col->SetNotNull(true);
    }
    implTableDesc.AddKeyColumnNames(NFulltext::TokenColumn);
    {
        auto col = implTableDesc.AddColumns();
        col->SetName(NFulltext::FreqColumn);
        col->SetType(NFulltext::DocCountTypeName);
        col->SetTypeId(NFulltext::DocCountType);
        col->SetNotNull(true);
    }

    implTableDesc.SetSystemColumnNamesAllowed(true);

    return implTableDesc;
}

auto CalcFulltextStatsImplTableDescImpl(
    const auto& baseTable,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    auto tableColumns = ExtractInfo(baseTable);

    NKikimrSchemeOp::TTableDescription implTableDesc;
    implTableDesc.SetName(NTableIndex::NFulltext::StatsTable);
    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);
    {
        auto col = implTableDesc.AddColumns();
        col->SetName(NFulltext::IdColumn);
        col->SetType("Uint32");
        col->SetTypeId(Ydb::Type::UINT32);
        col->SetNotNull(true);
    }
    {
        auto col = implTableDesc.AddColumns();
        col->SetName(NFulltext::DocCountColumn);
        col->SetType(NFulltext::DocCountTypeName);
        col->SetTypeId(NFulltext::DocCountType);
        col->SetNotNull(true);
    }
    {
        auto col = implTableDesc.AddColumns();
        col->SetName(NFulltext::SumDocLengthColumn);
        col->SetType(NFulltext::DocCountTypeName);
        col->SetTypeId(NFulltext::DocCountType);
        col->SetNotNull(true);
    }
    implTableDesc.AddKeyColumnNames(NFulltext::IdColumn);

    implTableDesc.SetSystemColumnNamesAllowed(true);

    return implTableDesc;
}

}

void FillIndexTableColumns(
    const TMap<ui32, NSchemeShard::TTableInfo::TColumn>& baseTableColumns,
    std::span<const TString> keys,
    const THashSet<TString>& columns,
    NKikimrSchemeOp::TTableDescription& implTableDesc) {
    FillIndexImplTableColumns(baseTableColumns, keys, columns, implTableDesc);
}

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    return CalcImplTableDescImpl(baseTableInfo, implTableColumns, indexTableDesc);
}

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    return CalcImplTableDescImpl(baseTableDescr, implTableColumns, indexTableDesc);
}

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreeLevelImplTableDesc(
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    NKikimrSchemeOp::TTableDescription implTableDesc;

    implTableDesc.SetName(NKMeans::LevelTable);

    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);

    {
        auto parentColumn = implTableDesc.AddColumns();
        parentColumn->SetName(NKMeans::ParentColumn);
        parentColumn->SetType(NTableIndex::NKMeans::ClusterIdTypeName);
        parentColumn->SetTypeId(NSchemeShard::ClusterIdTypeId);
        parentColumn->SetNotNull(true);
    }
    {
        auto idColumn = implTableDesc.AddColumns();
        idColumn->SetName(NKMeans::IdColumn);
        idColumn->SetType(NTableIndex::NKMeans::ClusterIdTypeName);
        idColumn->SetTypeId(NSchemeShard::ClusterIdTypeId);
        idColumn->SetNotNull(true);
    }
    {
        auto centroidColumn = implTableDesc.AddColumns();
        centroidColumn->SetName(NKMeans::CentroidColumn);
        centroidColumn->SetType("String");
        centroidColumn->SetTypeId(NScheme::NTypeIds::String);
        centroidColumn->SetNotNull(true);
    }

    implTableDesc.AddKeyColumnNames(NKMeans::ParentColumn);
    implTableDesc.AddKeyColumnNames(NKMeans::IdColumn);

    implTableDesc.SetSystemColumnNamesAllowed(true);

    return implTableDesc;
}

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePostingImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    std::string_view suffix,
    bool withForeign)
{
    return CalcVectorKmeansTreePostingImplTableDescImpl(baseTableInfo, baseTablePartitionConfig, indexDataColumns, indexTableDesc, suffix, withForeign);
}

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePostingImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    std::string_view suffix)
{
    return CalcVectorKmeansTreePostingImplTableDescImpl(baseTableDescr, baseTablePartitionConfig, indexDataColumns, indexTableDesc, suffix, false);
}

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePrefixImplTableDesc(
    const THashSet<TString>& indexKeyColumns,
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    return CalcVectorKmeansTreePrefixImplTableDescImpl(indexKeyColumns, baseTableInfo, baseTablePartitionConfig, implTableColumns, indexTableDesc);
}

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePrefixImplTableDesc(
    const THashSet<TString>& indexKeyColumns,
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    return CalcVectorKmeansTreePrefixImplTableDescImpl(indexKeyColumns, baseTableDescr, baseTablePartitionConfig, implTableColumns, indexTableDesc);
}

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreeBuildOverlapTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    std::string_view suffix)
{
    return CalcVectorKmeansTreeBuildOverlapTableDescImpl(baseTableInfo, baseTablePartitionConfig, indexDataColumns, indexTableDesc, suffix);
}

NKikimrSchemeOp::TTableDescription CalcFulltextImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc,
    bool withFreq)
{
    return CalcFulltextImplTableDescImpl(baseTableInfo, baseTablePartitionConfig, indexDataColumns, indexTableDesc, indexDesc, withFreq);
}

NKikimrSchemeOp::TTableDescription CalcFulltextImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc,
    bool withFreq)
{
    return CalcFulltextImplTableDescImpl(baseTableDescr, baseTablePartitionConfig, indexDataColumns, indexTableDesc, indexDesc, withFreq);
}

NKikimrSchemeOp::TTableDescription CalcFulltextDocsImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    return CalcFulltextDocsImplTableDescImpl(baseTableInfo, baseTablePartitionConfig, indexDataColumns, indexTableDesc);
}

NKikimrSchemeOp::TTableDescription CalcFulltextDocsImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    return CalcFulltextDocsImplTableDescImpl(baseTableDescr, baseTablePartitionConfig, indexDataColumns, indexTableDesc);
}

NKikimrSchemeOp::TTableDescription CalcFulltextDictImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc)
{
    return CalcFulltextDictImplTableDescImpl(baseTableInfo, baseTablePartitionConfig, indexTableDesc, indexDesc);
}

NKikimrSchemeOp::TTableDescription CalcFulltextDictImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc)
{
    return CalcFulltextDictImplTableDescImpl(baseTableDescr, baseTablePartitionConfig, indexTableDesc, indexDesc);
}

NKikimrSchemeOp::TTableDescription CalcFulltextStatsImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    return CalcFulltextStatsImplTableDescImpl(baseTableInfo, baseTablePartitionConfig, indexTableDesc);
}

NKikimrSchemeOp::TTableDescription CalcFulltextStatsImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    return CalcFulltextStatsImplTableDescImpl(baseTableDescr, baseTablePartitionConfig, indexTableDesc);
}

bool ExtractTypes(const NKikimrSchemeOp::TTableDescription& baseTableDescr, TColumnTypes& columnTypes, TString& explain) {
    const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;
    Y_ABORT_UNLESS(typeRegistry);

    for (auto& column: baseTableDescr.GetColumns()) {
        auto& columnName = column.GetName();
        auto typeName = NMiniKQL::AdaptLegacyYqlType(column.GetType());

        NScheme::TTypeInfo typeInfo;
        if (!GetTypeInfo(typeRegistry->GetType(typeName), column.GetTypeInfo(), typeName, columnName, typeInfo, explain)) {
            return false;
        }

        columnTypes[columnName] = typeInfo;
    }

    return true;
}

bool ExtractTypes(const NSchemeShard::TTableInfo::TPtr& baseTableInfo, TColumnTypes& columnsTypes, TString& explain) {
    Y_UNUSED(explain);

    for (const auto& [_, column] : baseTableInfo->Columns) {
        columnsTypes[column.Name] = column.PType;
    }

    return true;
}

bool IsCompatibleKeyTypes(
    const TColumnTypes& baseTableColumnTypes,
    const TTableColumns& implTableColumns,
    bool uniformTable,
    TString& explain)
{
    const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;
    Y_ABORT_UNLESS(typeRegistry);

    for (auto& keyName: implTableColumns.Keys) {
        Y_ABORT_UNLESS(baseTableColumnTypes.contains(keyName));
        auto typeInfo = baseTableColumnTypes.at(keyName);

        if (typeInfo.GetTypeId() == NScheme::NTypeIds::Uuid) {
            if (!AppData()->FeatureFlags.GetEnableUuidAsPrimaryKey()) {
                explain += TStringBuilder() << "Uuid as primary key is forbiden by configuration: " << keyName;
                return false;
            }
        }

        if (uniformTable) {
            switch (typeInfo.GetTypeId()) {
            case NScheme::NTypeIds::Uint32:
            case NScheme::NTypeIds::Uint64:
                break;
            default:
                explain += TStringBuilder() << "Column '" << keyName << "' has wrong key type "
                                            << NScheme::TypeName(typeInfo) << " for being key of table with uniform partitioning";
                return false;
            }
        }

        if (!NKikimr::IsAllowedKeyType(typeInfo)) {
            explain += TStringBuilder() << "Column '" << keyName << "' has wrong key type " << NScheme::TypeName(typeInfo) << " for being key";
            return false;
        }
    }

    return true;
}

}

}
