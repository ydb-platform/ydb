#pragma once
#include <ydb/core/tx/schemeshard/olap/column_families/update.h>
#include <ydb/core/tx/schemeshard/olap/columns/update.h>
#include <ydb/core/tx/schemeshard/olap/indexes/update.h>
#include <ydb/core/tx/schemeshard/olap/options/update.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {

class TOlapSchemaUpdate {
    YDB_READONLY_DEF(TOlapColumnsUpdate, Columns);
    YDB_READONLY_DEF(TOlapIndexesUpdate, Indexes);
    YDB_READONLY_DEF(TOlapOptionsUpdate, Options);
    YDB_READONLY_DEF(TOlapColumnFamiliesUpdate, ColumnFamilies);

public:
    bool Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys);
    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors);
};
}
