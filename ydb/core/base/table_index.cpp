#include "table_index.h"

#include <ydb/core/base/table_vector_index.h>

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

constexpr std::string_view ImplTables[] = {ImplTable, NTableVectorKmeansTreeIndex::LevelTable, NTableVectorKmeansTreeIndex::PostingTable};

}

TTableColumns CalcTableImplDescription(NKikimrSchemeOp::EIndexType type, const TTableColumns& table, const TIndexColumns& index) {
    TTableColumns result;

    const bool isSecondaryIndex = type != NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree;
    if (isSecondaryIndex) {
        for (const auto& ik : index.KeyColumns) {
            result.Keys.push_back(ik);
            result.Columns.emplace(ik);
        }
    }

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

    if (isSecondaryIndex) {
        if (index.KeyColumns.size() < 1) {
            explain = "should be at least single index key column";
            return false;
        }
        if (index.KeyColumns == table.Keys) {
            explain = "index keys shouldn't be table keys";
            return false;
        }
    } else {
        if (index.KeyColumns.size() != 1) {
            explain = "only single key column is supported for vector index";
            return false;
        }

        if (Contains(table.Keys, NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn)) {
            explain = TStringBuilder() << "table key column shouldn't have a reserved name: " << NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn;
            return false;
        }
        if (Contains(index.KeyColumns, NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn)) {
            // This isn't really needed, but it will be really strange to have column with such name but different meaning
            explain = TStringBuilder() << "index key column shouldn't have a reserved name: " << NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn;
            return false;
        }
        if (Contains(index.DataColumns, NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn)) {
            explain = TStringBuilder() << "index data column shouldn't have a reserved name: " << NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn;
            return false;
        }
    }
    tmp.clear();
    tmp.insert(table.Keys.begin(), table.Keys.end());
    if (isSecondaryIndex) {
        tmp.insert(index.KeyColumns.begin(), index.KeyColumns.end());
    }
    if (const auto* broken = IsContains(index.DataColumns, tmp, true)) {
        explain = TStringBuilder()
                  << "the same column can't be used as key and data column for one index, for example " << *broken;
        return false;
    }
    return true;
}

TVector<TString> GetImplTables(NKikimrSchemeOp::EIndexType indexType) {
    if (indexType == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
        return { NTableVectorKmeansTreeIndex::LevelTable, NTableVectorKmeansTreeIndex::PostingTable };
    } else {
        return { ImplTable };
    }
}

bool IsImplTable(std::string_view tableName) {
    return Contains(ImplTables, tableName);
}

bool IsTmpImplTable(std::string_view tableName) {
    // all impl tables that ends with "tmp" should be used only for index creation and dropped when index build is finished
    return tableName.ends_with("tmp");
}

}
