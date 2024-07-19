#include "table_index.h"

TVector<TString>::const_iterator IsUniq(const TVector<TString>& names) {
    THashSet<TString> tmp;

    for (auto it = names.begin(); it != names.end(); ++it) {
        bool inserted = tmp.insert(*it).second;
        if (!inserted) {
            return it;
        }
    }

    return names.end();
}

bool Contains(const TVector<TString>& names, TString str) {
    return std::find(names.begin(), names.end(), str) != names.end();
}

namespace NKikimr {
namespace NTableIndex {

TTableColumns CalcTableImplDescription(const NKikimrSchemeOp::EIndexType indexType, const TTableColumns& table, const TIndexColumns& index) {
    TTableColumns result;

    if (indexType == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
        result.Keys.push_back(NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn);
        result.Columns.insert(NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn);
    } else {
        for (const auto& ik: index.KeyColumns) {
            result.Keys.push_back(ik);
            result.Columns.insert(ik);
        }
    }

    for (const auto& tk: table.Keys) {
        if (!result.Columns.contains(tk)) {
            result.Keys.push_back(tk);
            result.Columns.insert(tk);
        }
    }

    for (const auto& dk: index.DataColumns) {
        result.Columns.insert(dk);
    }

    return result;
}

bool IsCompatibleIndex(const NKikimrSchemeOp::EIndexType indexType, const TTableColumns& table, const TIndexColumns& index, TString& explain) {
    const bool isVectorIndex = indexType == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree;

    {
        auto brokenAt = IsUniq(table.Keys);
        if (brokenAt != table.Keys.end()) {
            explain = TStringBuilder()
                    << "all table keys should be uniq, for example " << *brokenAt;
            return false;
        }
    }

    {
        auto brokenAt = IsUniq(index.KeyColumns);
        if (brokenAt != index.KeyColumns.end()) {
            explain = TStringBuilder()
                    << "all index keys should be uniq, for example " << *brokenAt;
            return false;
        }
    }

    {
        auto brokenAt = IsUniq(index.DataColumns);
        if (brokenAt != index.DataColumns.end()) {
            explain = TStringBuilder()
                    << "all data columns should be uniq, for example " << *brokenAt;
            return false;
        }
    }

    if (isVectorIndex) {
        if (index.KeyColumns.size() != 1) {
            explain = "Only single key column is supported for vector index";
            return false;
        }

        if (Contains(index.KeyColumns, NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn)) {
            explain = TStringBuilder() << "Key column should not have a reserved name: " << NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn;
            return false;
        }

        if (Contains(index.DataColumns, NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn)) {
            explain = TStringBuilder() << "Data column should not have a reserved name: " << NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn;
            return false;
        }
    }

    THashSet<TString> indexKeys;

    for (const auto& tableKeyName: table.Keys) {
        indexKeys.insert(tableKeyName);
        if (!table.Columns.contains(tableKeyName)) {
            explain = TStringBuilder()
                    << "all table keys should be in table columns too"
                    << ", table key " << tableKeyName << " is missed";
            return false;
        }
    }

    for (const auto& indexKeyName: index.KeyColumns) {
        if (!isVectorIndex)
            indexKeys.insert(indexKeyName);
        if (!table.Columns.contains(indexKeyName)) {
            explain = TStringBuilder()
                    << "all index keys should be in table columns"
                    << ", index key " << indexKeyName << " is missed";
            return false;
        }
    }

    if (index.KeyColumns == table.Keys && !isVectorIndex) {
        explain = TStringBuilder()
                    << "table and index keys are the same";
        return false;
    }

    for (const auto& dataName: index.DataColumns) {
        if (indexKeys.contains(dataName)) {
            explain = TStringBuilder()
                    << "The same column can't be used as key column and data column for one index";
            return false;
        }
        if (!table.Columns.contains(dataName)) {
            explain = TStringBuilder()
                    << "all index data columns should be in table columns"
                    << ", data columns " << dataName << " is missed";
            return false;
        }
    }

    return true;
}

bool IsImplTable(std::string_view tableName) {
    return std::find(std::begin(ImplTables), std::end(ImplTables), tableName) != std::end(ImplTables);
}

}
}
