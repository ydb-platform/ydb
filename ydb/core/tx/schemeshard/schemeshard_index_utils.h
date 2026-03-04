#pragma once

#include "schemeshard_info_types.h"
#include "schemeshard_types.h"

#include <ydb/core/base/fulltext.h>
#include <ydb/core/base/table_index.h>

#include <yql/essentials/minikql/mkql_type_ops.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>


namespace NKikimr {

namespace NTableIndex {

struct TIndexObjectCounts {
    ui32 IndexTableCount = 0;
    ui32 SequenceCount = 0;
    ui32 IndexTableShards = 0;
    ui32 ShardsPerPath = 0;
};

TIndexObjectCounts GetIndexObjectCounts(const NKikimrSchemeOp::TIndexCreationConfig& indexDesc);

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDesc,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreeLevelImplTableDesc(
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePostingImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    std::string_view suffix = {},
    bool withForeign = false);

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePostingImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    std::string_view suffix = {});

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePrefixImplTableDesc(
    const THashSet<TString>& indexKeyColumns,
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePrefixImplTableDesc(
    const THashSet<TString>& indexKeyColumns,
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreeBuildOverlapTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    std::string_view suffix = {});

NKikimrSchemeOp::TTableDescription CalcFulltextImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc,
    bool withFreq);

NKikimrSchemeOp::TTableDescription CalcFulltextImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc,
    bool withFreq);

NKikimrSchemeOp::TTableDescription CalcFulltextDocsImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcFulltextDocsImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcFulltextDictImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc);

NKikimrSchemeOp::TTableDescription CalcFulltextDictImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc);

NKikimrSchemeOp::TTableDescription CalcFulltextStatsImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcFulltextStatsImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

TTableColumns ExtractInfo(const NSchemeShard::TTableInfo::TPtr& tableInfo);
TTableColumns ExtractInfo(const NKikimrSchemeOp::TTableDescription& tableDesc);
TIndexColumns ExtractInfo(const NKikimrSchemeOp::TIndexCreationConfig& indexDesc);

void FillIndexTableColumns(
    const TMap<ui32, NSchemeShard::TTableInfo::TColumn>& baseTableColumns,
    std::span<const TString> keys,
    const THashSet<TString>& columns,
    NKikimrSchemeOp::TTableDescription& implTableDesc);

using TColumnTypes = THashMap<TString, NScheme::TTypeInfo>;

bool ExtractTypes(const NSchemeShard::TTableInfo::TPtr& baseTableInfo, TColumnTypes& columnsTypes, TString& explain);
bool ExtractTypes(const NKikimrSchemeOp::TTableDescription& baseTableDesc, TColumnTypes& columnsTypes, TString& explain);

bool IsCompatibleKeyTypes(
    const TColumnTypes& baseTableColumnsTypes,
    const TTableColumns& implTableColumns,
    bool uniformTable,
    TString& explain);

template <typename TTableDesc>
bool CommonCheck(const TTableDesc& tableDesc, const NKikimrSchemeOp::TIndexCreationConfig& indexDesc,
        const NSchemeShard::TSchemeLimits& schemeLimits, bool uniformTable,
        TTableColumns& implTableColumns, NKikimrScheme::EStatus& status, TString& error)
{
    const TTableColumns baseTableColumns = ExtractInfo(tableDesc);
    const TIndexColumns indexKeys = ExtractInfo(indexDesc);

    if (indexKeys.KeyColumns.empty()) {
        status = NKikimrScheme::EStatus::StatusInvalidParameter;
        error = "No key columns in index creation config";
        return false;
    }

    if (!indexKeys.DataColumns.empty() && !AppData()->FeatureFlags.GetEnableDataColumnForIndexTable()) {
        status = NKikimrScheme::EStatus::StatusPreconditionFailed;
        error = "It is not allowed to create index with data column";
        return false;
    }

    if (!IsCompatibleIndex(GetIndexType(indexDesc), baseTableColumns, indexKeys, error)) {
        status = NKikimrScheme::EStatus::StatusInvalidParameter;
        return false;
    }

    TColumnTypes baseColumnTypes;
    if (!ExtractTypes(tableDesc, baseColumnTypes, error)) {
        status = NKikimrScheme::EStatus::StatusInvalidParameter;
        return false;
    }

    implTableColumns = CalcTableImplDescription(GetIndexType(indexDesc), baseTableColumns, indexKeys);

    switch (GetIndexType(indexDesc)) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            if (!IsCompatibleKeyTypes(baseColumnTypes, implTableColumns, uniformTable, error)) {
                status = NKikimrScheme::EStatus::StatusInvalidParameter;
                return false;
            }
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
            // We have already checked this in IsCompatibleIndex
            Y_ABORT_UNLESS(indexKeys.KeyColumns.size() >= 1);

            if (indexKeys.KeyColumns.size() > 1 && !IsCompatibleKeyTypes(baseColumnTypes, implTableColumns, uniformTable, error)) {
                status = NKikimrScheme::EStatus::StatusInvalidParameter;
                return false;
            }

            const TString& embeddingColumnName = indexKeys.KeyColumns.back();
            Y_ABORT_UNLESS(baseColumnTypes.contains(embeddingColumnName));
            auto typeInfo = baseColumnTypes.at(embeddingColumnName);

            if (typeInfo.GetTypeId() != NScheme::NTypeIds::String) {
                status = NKikimrScheme::EStatus::StatusInvalidParameter;
                error = TStringBuilder() << "Embedding column '" << embeddingColumnName << "' expected type 'String' but got " << NScheme::TypeName(typeInfo);
                return false;
            }
            break;
        }
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance: {
            // We have already checked this in IsCompatibleIndex
            Y_ABORT_UNLESS(indexKeys.KeyColumns.size() >= 1);

            // Fulltext index only supports tables with a single PK column of type Uint64
            if (baseTableColumns.Keys.size() != 1) {
                status = NKikimrScheme::EStatus::StatusInvalidParameter;
                error = TStringBuilder()
                    << "Fulltext index requires exactly one primary key column of type 'Uint64'"
                    << ", but table has " << baseTableColumns.Keys.size() << " primary key columns";
                return false;
            }

            {
                const TString& pkColumnName = baseTableColumns.Keys[0];
                Y_ABORT_UNLESS(baseColumnTypes.contains(pkColumnName));
                auto pkTypeInfo = baseColumnTypes.at(pkColumnName);
                if (pkTypeInfo.GetTypeId() != NScheme::NTypeIds::Uint64) {
                    status = NKikimrScheme::EStatus::StatusInvalidParameter;
                    error = TStringBuilder()
                        << "Fulltext index requires primary key column '" << pkColumnName
                        << "' to be of type 'Uint64' but got " << NScheme::TypeName(pkTypeInfo);
                    return false;
                }
            }

            // Here we only check that fulltext index columns matches table description
            // the rest will be checked in NFulltext::ValidateSettings (called separately outside of CommonCheck)
            if (!NKikimr::NFulltext::ValidateColumnsMatches(indexKeys.KeyColumns, indexDesc.GetFulltextIndexDescription().GetSettings(), error)) {
                status = NKikimrScheme::EStatus::StatusInvalidParameter;
                return false;
            }

            for (const auto& column : indexDesc.GetFulltextIndexDescription().GetSettings().columns()) {
                if (column.has_analyzers()) {
                    auto typeInfo = baseColumnTypes.at(column.column());
                    if (typeInfo.GetTypeId() != NScheme::NTypeIds::String && typeInfo.GetTypeId() != NScheme::NTypeIds::Utf8) {
                        status = NKikimrScheme::EStatus::StatusInvalidParameter;
                        error = TStringBuilder() << "Fulltext column '" << column.column() << "' expected type 'String' or 'Utf8' but got " << NScheme::TypeName(typeInfo);
                        return false;
                    }
                }
            }

            break;
        }
        default:
            status = NKikimrScheme::EStatus::StatusInvalidParameter;
            error = InvalidIndexType(indexDesc.GetType());
            return false;
    }

    if (implTableColumns.Keys.size() > schemeLimits.MaxTableKeyColumns) {
        status = NKikimrScheme::EStatus::StatusSchemeError;
        error = TStringBuilder()
            << "Too many keys indexed, index table reaches the limit of the maximum key columns count"
            << ": indexing columns: " << indexKeys.KeyColumns.size()
            << ", requested keys columns for index table: " << implTableColumns.Keys.size()
            << ", limit: " << schemeLimits.MaxTableKeyColumns;
        return false;
    }

    return true;
}

template <typename TTableDesc>
bool CommonCheck(const TTableDesc& tableDesc, const NKikimrSchemeOp::TIndexCreationConfig& indexDesc,
        const NSchemeShard::TSchemeLimits& schemeLimits, TString& error)
{
    TTableColumns implTableColumns;
    NKikimrScheme::EStatus status;
    return CommonCheck(tableDesc, indexDesc, schemeLimits, false, implTableColumns, status, error);
}

} // NTableIndex
} // NKikimr
