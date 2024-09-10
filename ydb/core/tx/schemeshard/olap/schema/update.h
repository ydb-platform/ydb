#pragma once
#include <ydb/core/tx/schemeshard/olap/column_family/update.h>
#include <ydb/core/tx/schemeshard/olap/columns/update.h>
#include <ydb/core/tx/schemeshard/olap/indexes/update.h>
#include <ydb/core/tx/schemeshard/olap/options/update.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {

using ColumnFamiliesByName = THashMap<TString, TOlapColumnFamlilyAdd>;

class TOlapSchemaUpdate {
    YDB_READONLY_DEF(TOlapColumnsUpdate, Columns);
    YDB_READONLY_DEF(TOlapIndexesUpdate, Indexes);
    YDB_READONLY_DEF(TOlapOptionsUpdate, Options);
    YDB_READONLY_DEF(TOlapColumnFamiliesUpdate, ColumnFamilies)
    YDB_READONLY_OPT(NKikimrSchemeOp::EColumnTableEngine, Engine);
    YDB_READONLY_DEF(ColumnFamiliesByName, CurrentColumnFamilies);

public:
    void PutCurrentFamilies(const std::vector<TOlapColumnFamlilyAdd>& currentColumnFamilies);
    bool Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys = false);
    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors);
};
}
