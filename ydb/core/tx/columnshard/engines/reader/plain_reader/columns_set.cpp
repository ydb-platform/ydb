#include "columns_set.h"
#include <util/string/join.h>

namespace NKikimr::NOlap::NPlainReader {

TString TColumnsSet::DebugString() const {
    return TStringBuilder() << "("
        << "column_ids=" << JoinSeq(",", ColumnIds) << ";"
        << "column_names=" << JoinSeq(",", ColumnNames) << ";"
        << ");";
}

}
