#include "table_index.h"

#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimr::NTableIndex {
namespace {

const TString* IsUnique(const TVector<TString>& names, THashSet<TString>& tmp) {
    tmp.clear();
    for (const auto& name : names) {
        if (!tmp.emplace(name).second) {
            return &name;
        }
    }
    return nullptr;
}

const TString* IsContains(const TVector<TString>& names, const THashSet<TString>& columns, bool contains = false) {
    for (const auto& name : names) {
        if (columns.contains(name) == contains) {
            return &name;
        }
    }
    return nullptr;
}

bool Contains(const auto& names, std::string_view str) {
    return std::find(std::begin(names), std::end(names), str) != std::end(names);
}

bool ContainsSystemColumn(const auto& columns) {
    for (const auto& column : columns) {
        if (column.StartsWith(SYSTEM_COLUMN_PREFIX)) {
            return true;
        }
    }
    return false;
}

const TString ImplTables[] = {
    ImplTable,
    NTableVectorKmeansTreeIndex::LevelTable,
    NTableVectorKmeansTreeIndex::PostingTable,
    NTableVectorKmeansTreeIndex::PrefixTable,
    TString{NTableVectorKmeansTreeIndex::PostingTable} + NTableVectorKmeansTreeIndex::BuildSuffix0,
    TString{NTableVectorKmeansTreeIndex::PostingTable} + NTableVectorKmeansTreeIndex::BuildSuffix1,
};

constexpr std::string_view GlobalSecondaryImplTables[] = {
    ImplTable,
};
static_assert(std::is_sorted(std::begin(GlobalSecondaryImplTables), std::end(GlobalSecondaryImplTables)));

constexpr std::string_view GlobalKMeansTreeImplTables[] = {
    NTableVectorKmeansTreeIndex::LevelTable, NTableVectorKmeansTreeIndex::PostingTable,
};
static_assert(std::is_sorted(std::begin(GlobalKMeansTreeImplTables), std::end(GlobalKMeansTreeImplTables)));

constexpr std::string_view PrefixedGlobalKMeansTreeImplTables[] = {
    NTableVectorKmeansTreeIndex::LevelTable, NTableVectorKmeansTreeIndex::PostingTable, NTableVectorKmeansTreeIndex::PrefixTable,
};
static_assert(std::is_sorted(std::begin(PrefixedGlobalKMeansTreeImplTables), std::end(PrefixedGlobalKMeansTreeImplTables)));

}

TTableColumns CalcTableImplDescription(NKikimrSchemeOp::EIndexType type, const TTableColumns& table, const TIndexColumns& index) {
    TTableColumns result;

    const bool isSecondaryIndex = type != NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree;
    std::for_each(index.KeyColumns.begin(), index.KeyColumns.end() - (isSecondaryIndex ? 0 : 1), [&] (const auto& ik) {
        result.Keys.push_back(ik);
        result.Columns.emplace(ik);
    });

    for (const auto& tk : table.Keys) {
        if (result.Columns.emplace(tk).second) {
            result.Keys.push_back(tk);
        }
    }

    for (const auto& dk : index.DataColumns) {
        result.Columns.emplace(dk);
    }

    return result;
}

bool IsCompatibleIndex(NKikimrSchemeOp::EIndexType indexType, const TTableColumns& table, const TIndexColumns& index, TString& explain) {
    if (const auto* broken = IsContains(table.Keys, table.Columns)) {
        explain = TStringBuilder()
                  << "all table key columns should be in table columns, table key column "
                  << *broken << " is missed";
        return false;
    }

    if (const auto* broken = IsContains(index.KeyColumns, table.Columns)) {
        explain = TStringBuilder()
                  << "all index key columns should be in table columns, index key column "
                  << *broken << " is missed";
        return false;
    }

    if (const auto* broken = IsContains(index.DataColumns, table.Columns)) {
        explain = TStringBuilder()
                  << "all index data columns should be in table columns, index data column "
                  << *broken << " is missed";
        return false;
    }

    THashSet<TString> tmp;

    if (const auto* broken = IsUnique(table.Keys, tmp)) {
        explain = TStringBuilder()
                  << "all table key columns should be unique, for example " << *broken;
        return false;
    }

    if (const auto* broken = IsUnique(index.KeyColumns, tmp)) {
        explain = TStringBuilder()
                  << "all index key columns should be unique, for example " << *broken;
        return false;
    }

    if (const auto* broken = IsUnique(index.DataColumns, tmp)) {
        explain = TStringBuilder()
                  << "all index data columns should be unique, for example " << *broken;
        return false;
    }

    const bool isSecondaryIndex = indexType != NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree;

    if (index.KeyColumns.size() < 1) {
        explain = "should be at least single index key column";
        return false;
    }
    if (isSecondaryIndex) {
        if (index.KeyColumns == table.Keys) {
            explain = "index keys shouldn't be table keys";
            return false;
        }
    } else {
        if (ContainsSystemColumn(table.Keys)) {
            explain = TStringBuilder() << "table key column shouldn't have a reserved name";
            return false;
        }
        if (ContainsSystemColumn(index.KeyColumns)) {
            explain = TStringBuilder() << "index key column shouldn't have a reserved name";
            return false;
        }
        if (ContainsSystemColumn(index.DataColumns)) {
            explain = TStringBuilder() << "index data column shouldn't have a reserved name";
            return false;
        }
    }
    tmp.clear();
    tmp.insert(table.Keys.begin(), table.Keys.end());
    if (isSecondaryIndex) {
        tmp.insert(index.KeyColumns.begin(), index.KeyColumns.end());
    } else {
        // Vector indexes allow to add all columns both to index & data
    }
    if (const auto* broken = IsContains(index.DataColumns, tmp, true)) {
        explain = TStringBuilder()
                  << "the same column can't be used as key and data column for one index, for example " << *broken;
        return false;
    }
    return true;
}

std::span<const std::string_view> GetImplTables(NKikimrSchemeOp::EIndexType indexType, std::span<const TString> indexKeys) {
    if (indexType == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
        if (indexKeys.size() == 1) {
            return GlobalKMeansTreeImplTables;
        } else {
            return PrefixedGlobalKMeansTreeImplTables;
        }
    } else {
        return GlobalSecondaryImplTables;
    }
}

bool IsImplTable(std::string_view tableName) {
    return Contains(ImplTables, tableName);
}

bool IsBuildImplTable(std::string_view tableName) {
    // all impl tables that ends with "build" should be used only for index creation and dropped when index build is finished
    return tableName.ends_with(NTableVectorKmeansTreeIndex::BuildSuffix0)
        || tableName.ends_with(NTableVectorKmeansTreeIndex::BuildSuffix1);
}

static constexpr TClusterId PostingParentFlag = (1ull << 63ull);

// Note: if cluster id is too big, something is wrong with cluster enumeration
void EnsureNoPostingParentFlag(TClusterId parent) {
    Y_ENSURE((parent & PostingParentFlag) == 0);
}

TClusterId SetPostingParentFlag(TClusterId parent) {
    EnsureNoPostingParentFlag(parent);
    return (parent | PostingParentFlag);
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvReshuffleKMeansRequest& record) {
    auto copy = record;
    TStringBuilder result;
    // clusters are not human readable and can be large like 100Kb+
    copy.ClearClusters();
    result << copy.ShortDebugString();
    result << " Clusters: " << record.ClustersSize();
    return result;
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvRecomputeKMeansRequest& record) {
    auto copy = record;
    TStringBuilder result;
    // clusters are not human readable and can be large like 100Kb+
    copy.ClearClusters();
    result << copy.ShortDebugString();
    result << " Clusters: " << record.ClustersSize();
    return result;
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvRecomputeKMeansResponse& record) {
    auto copy = record;
    TStringBuilder result;
    // clusters are not human readable and can be large like 100Kb+
    copy.ClearClusters();
    copy.ClearClusterSizes();
    result << copy.ShortDebugString();
    result << " Clusters: " << record.ClustersSize();
    return result;
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvSampleKResponse& record) {
    auto copy = record;
    TStringBuilder result;
    // rows are not human readable and can be large like 100Kb+
    copy.ClearRows();
    result << copy.ShortDebugString();
    result << " Rows: " << record.RowsSize();
    return result;
}

TString ToShortDebugString(const NKikimrTxDataShard::TEvValidateUniqueIndexResponse& record) {
    auto copy = record;
    // keys are not human readable and contain user data
    copy.ClearFirstIndexKey();
    copy.ClearLastIndexKey();
    return copy.ShortDebugString();
}

}
