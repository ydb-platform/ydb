#pragma once

#include "schemeshard_info_types.h"
#include "schemeshard_types.h"

#include <ydb/core/base/table_index.h>

#include <yql/essentials/minikql/mkql_type_ops.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>


namespace NKikimr {
namespace NSchemeShard {

inline bool IsValidColumnName(const TString& name, bool allowSystemColumnNames = false) {
    if (!allowSystemColumnNames && name.StartsWith(SYSTEM_COLUMN_PREFIX)) {
        return false;
    }

    for (auto c: name) {
        if (!std::isalnum(c) && c != '_' && c != '-') {
            return false;
        }
    }

    return true;
}

inline NKikimrSchemeOp::TModifyScheme TransactionTemplate(const TString& workingDir, NKikimrSchemeOp::EOperationType type) {
    NKikimrSchemeOp::TModifyScheme tx;
    tx.SetWorkingDir(workingDir);
    tx.SetOperationType(type);

    return tx;
}

class PQGroupReserve {
public:
    PQGroupReserve(const ::NKikimrPQ::TPQTabletConfig& tabletConfig, ui64 partitions);

    ui64 Storage;
    ui64 Throughput;
};

} // NSchemeShard

namespace NTableIndex {

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
    std::string_view suffix = {});

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

    if (!IsCompatibleIndex(indexDesc.GetType(), baseTableColumns, indexKeys, error)) {
        status = NKikimrScheme::EStatus::StatusInvalidParameter;
        return false;
    }

    TColumnTypes baseColumnTypes;
    if (!ExtractTypes(tableDesc, baseColumnTypes, error)) {
        status = NKikimrScheme::EStatus::StatusInvalidParameter;
        return false;
    }

    implTableColumns = CalcTableImplDescription(indexDesc.GetType(), baseTableColumns, indexKeys);

    if (indexDesc.GetType() == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
        //We have already checked this in IsCompatibleIndex
        Y_ABORT_UNLESS(indexKeys.KeyColumns.size() >= 1);

        if (indexKeys.KeyColumns.size() > 1 && !IsCompatibleKeyTypes(baseColumnTypes, implTableColumns, uniformTable, error)) {
            status = NKikimrScheme::EStatus::StatusInvalidParameter;
            return false;
        }

        const TString& indexColumnName = indexKeys.KeyColumns.back();
        Y_ABORT_UNLESS(baseColumnTypes.contains(indexColumnName));
        auto typeInfo = baseColumnTypes.at(indexColumnName);

        if (typeInfo.GetTypeId() != NScheme::NTypeIds::String) {
            status = NKikimrScheme::EStatus::StatusInvalidParameter;
            error = TStringBuilder() << "Index column '" << indexColumnName << "' expected type 'String' but got " << NScheme::TypeName(typeInfo);
            return false;
        }
    } else if (!IsCompatibleKeyTypes(baseColumnTypes, implTableColumns, uniformTable, error)) {
        status = NKikimrScheme::EStatus::StatusInvalidParameter;
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
