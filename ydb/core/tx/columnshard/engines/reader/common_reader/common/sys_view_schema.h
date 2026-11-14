#pragma once

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NOlap::NReader::NCommon {

template <class TSysViewSchema>
NArrow::TSimpleRow BuildSysViewPKRow(const TString& data) {
    static const auto schema = []() {
        NTable::TScheme::TTableSchema table;
        NIceDb::NHelpers::TStaticSchemaFiller<TSysViewSchema>::Fill(table);
        std::vector<std::pair<TString, NScheme::TTypeInfo>> keyColumns;
        for (const auto id : table.KeyColumns) {
            const auto it = table.Columns.find(id);
            AFL_VERIFY(it != table.Columns.end());
            keyColumns.emplace_back(it->second.Name, it->second.PType);
        }

        return std::make_pair(NArrow::MakeArrowSchema(keyColumns).ValueOrDie(), NArrow::ColumnTypes(keyColumns));
    }();

    return NArrow::TSimpleRow(data, schema.first, schema.second);
}

}   // namespace NKikimr::NOlap::NReader::NCommon
