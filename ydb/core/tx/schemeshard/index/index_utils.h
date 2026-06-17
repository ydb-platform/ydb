#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/schemeshard_types.h>

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

// Fulltext index key columns are ordered [prefix..., text]; the text column is always last.
// Returns the leading prefix columns (empty for a non-prefixed index).
inline TVector<TString> GetFulltextPrefixColumns(const NProtoBuf::RepeatedPtrField<TString>& keyColumnNames) {
    if (keyColumnNames.size() <= 1) {
        return {};
    }
    return TVector<TString>{keyColumnNames.begin(), keyColumnNames.end() - 1};
}

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    ui32 uniqueKeySize);

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDesc,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    ui32 uniqueKeySize);

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
    const NKikimrSchemeOp::EIndexType indexType,
    const TVector<TString>& prefixColumns = {});

NKikimrSchemeOp::TTableDescription CalcFulltextImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc,
    const NKikimrSchemeOp::EIndexType indexType,
    const TVector<TString>& prefixColumns = {});

NKikimrSchemeOp::TTableDescription CalcFulltextCompactImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription* indexDesc,
    const NKikimrSchemeOp::EIndexType indexType,
    const TVector<TString>& prefixColumns = {});

NKikimrSchemeOp::TTableDescription CalcFulltextCompactImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription* indexDesc,
    const NKikimrSchemeOp::EIndexType indexType,
    const TVector<TString>& prefixColumns = {});

NKikimrSchemeOp::TTableDescription CalcFulltextDocsImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc);

NKikimrSchemeOp::TTableDescription CalcFulltextDocsImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const THashSet<TString>& indexDataColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    const NKikimrSchemeOp::TFulltextIndexDescription& indexDesc);

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

// Fulltext and JSON indexes require exactly one Int64/Int32/Uint64/Uint32 primary key column
bool CheckSingleIntegerPrimaryKey(
    const TTableColumns& baseTableColumns,
    const TColumnTypes& baseColumnTypes,
    TStringBuf indexKind,
    TString& error);

// Classification of how a fulltext index build should obtain its document id.
enum class EFulltextRowIdPlan {
    NotApplicable,    // the index is not a fulltext index - nothing to decide
    LegacyIntegerPk,  // no __ydb_row_id column and a single integer PK - use the PK as doc_id (legacy)
    Reuse,            // a valid __ydb_row_id column + Ready unique index on it already exist - reuse them
    Provision,        // a custom (non-single-integer) PK without the full rowid infrastructure - the
                      // schemeshard must auto-provision the missing parts (see NeedColumn/NeedUniqueIndex)
    Error,            // an invalid state (e.g. malformed __ydb_row_id, or a half-built unique index)
};

struct TFulltextRowIdClassification {
    EFulltextRowIdPlan Plan = EFulltextRowIdPlan::NotApplicable;
    bool NeedColumn = false;       // the __ydb_row_id column must be added (and backfilled)
    bool NeedUniqueIndex = false;  // the unique secondary index on __ydb_row_id must be created
};

// Classifies a fulltext index build against the main table's current schema. The rules mirror the
// historical opt-in (a __ydb_row_id Uint64 NOT NULL column plus a Ready single-column GlobalUnique index on
// __ydb_row_id enables rowid mode), but additionally distinguishes the "custom PK, infrastructure missing"
// case so the schemeshard can auto-provision it. See EFulltextRowIdPlan. For a non-fulltext index the
// plan is NotApplicable.
TFulltextRowIdClassification ClassifyFulltextRowId(
    const NSchemeShard::TTableInfo::TPtr& tableInfo,
    const TMap<TString, TPathId>& tableChildren,
    const THashMap<TPathId, NSchemeShard::TTableIndexInfo::TPtr>& indexes,
    const NKikimrSchemeOp::TIndexCreationConfig& indexDesc,
    TString& error);

// Thin wrapper over ClassifyFulltextRowId for callers that only need to enable rowid mode when the
// infrastructure already exists (the Reuse case). On Reuse it sets
// indexDesc.MutableFulltextIndexDescription()->set_use_row_id_as_doc_id(true) and returns true.
// NotApplicable / LegacyIntegerPk are noops that return true (the single-integer-PK validation in
// CommonCheck applies as before). Provision / Error return false with an explanatory error - callers
// on the strict path (e.g. the create-build-index sub-operation composer) treat that as a rejection;
// the auto-provisioning entry point (TTxCreate) uses ClassifyFulltextRowId directly instead.
bool MaybeEnableFulltextRowIdMode(
    const NSchemeShard::TTableInfo::TPtr& tableInfo,
    const TMap<TString, TPathId>& tableChildren,
    const THashMap<TPathId, NSchemeShard::TTableIndexInfo::TPtr>& indexes,
    NKikimrSchemeOp::TIndexCreationConfig& indexDesc,
    TString& error);

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
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance: {
            // We have already checked this in IsCompatibleIndex
            Y_ABORT_UNLESS(indexKeys.KeyColumns.size() >= 1);

            // Fulltext index key columns are [prefix..., text]; more than one key column means the
            // index has prefix columns. Enforce the feature flag server-side so direct scheme
            // operations or older clients cannot bypass the KQP-side check.
            if (indexKeys.KeyColumns.size() > 1 && !AppData()->FeatureFlags.GetEnableFulltextIndexPrefix()) {
                status = NKikimrScheme::EStatus::StatusPreconditionFailed;
                error = "Fulltext index prefix columns support is disabled";
                return false;
            }

            // __ydb_row_id opt-in: when MaybeEnableFulltextRowIdMode() has set the flag,
            // skip the single-integer-PK requirement (the doc_id is __ydb_row_id, not the PK).
            if (!indexDesc.GetFulltextIndexDescription().GetUseRowIdAsDocId()) {
                if (!CheckSingleIntegerPrimaryKey(baseTableColumns, baseColumnTypes, "Fulltext", error)) {
                    status = NKikimrScheme::EStatus::StatusInvalidParameter;
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
        case NKikimrSchemeOp::EIndexTypeGlobalJson:
        case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact: {
            Y_ABORT_UNLESS(indexKeys.KeyColumns.size() >= 1);

            if (!CheckSingleIntegerPrimaryKey(baseTableColumns, baseColumnTypes, "JSON", error)) {
                status = NKikimrScheme::EStatus::StatusInvalidParameter;
                return false;
            }

            if (indexKeys.KeyColumns.size() != 1) {
                status = NKikimrScheme::EStatus::StatusInvalidParameter;
                error = TStringBuilder() << "JSON index requires exactly one key column, but " << indexKeys.KeyColumns.size() << " are requested";
                return false;
            }

            if (!indexKeys.DataColumns.empty()) {
                status = NKikimrScheme::EStatus::StatusInvalidParameter;
                error = TStringBuilder() << "JSON index does not support COVER columns";
                return false;
            }

            for (const auto& column : indexKeys.KeyColumns) {
                auto typeInfo = baseColumnTypes.at(column);
                if (typeInfo.GetTypeId() != NScheme::NTypeIds::Json &&
                    typeInfo.GetTypeId() != NScheme::NTypeIds::JsonDocument)
                {
                    status = NKikimrScheme::EStatus::StatusInvalidParameter;
                    error = TStringBuilder() << "JSON column '" << column <<
                        "' must have type 'Json' or 'JsonDocument' but got " << NScheme::TypeName(typeInfo);
                    return false;
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
