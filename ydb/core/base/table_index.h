#pragma once

#include "table_vector_index.h"
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NTableIndex {

struct TTableColumns {
    THashSet<TString> Columns;
    TVector<TString> Keys;
};

struct TIndexColumns {
    TVector<TString> KeyColumns;
    TVector<TString> DataColumns;
};

inline constexpr const char* ImplTable = "indexImplTable";
inline constexpr std::string_view ImplTables[] = {ImplTable, NTableVectorKmeansTreeIndex::LevelTable, NTableVectorKmeansTreeIndex::PostingTable};

bool IsCompatibleIndex(const NKikimrSchemeOp::EIndexType indexType, const TTableColumns& table, const TIndexColumns& index, TString& explain);
TTableColumns CalcTableImplDescription(const NKikimrSchemeOp::EIndexType indexType, const TTableColumns& table, const TIndexColumns& index);

bool IsImplTable(std::string_view tableName);

}
}
