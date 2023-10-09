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

namespace NKikimr {
namespace NTableIndex {

TTableColumns CalcTableImplDescription(const TTableColumns& table, const TIndexColumns& index) {
    {
        TString explain;
        Y_ABORT_UNLESS(IsCompatibleIndex(table, index, explain), "explain is %s", explain.c_str());
    }

    TTableColumns result;

    for (const auto& ik: index.KeyColumns) {
        result.Keys.push_back(ik);
        result.Columns.insert(ik);
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

bool IsCompatibleIndex(const TTableColumns& table, const TIndexColumns& index, TString& explain) {
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
        indexKeys.insert(indexKeyName);
        if (!table.Columns.contains(indexKeyName)) {
            explain = TStringBuilder()
                    << "all index keys should be in table columns"
                    << ", index key " << indexKeyName << " is missed";
            return false;
        }
    }

    if (index.KeyColumns == table.Keys) {
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

}
}
