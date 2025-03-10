#include "schema.h"

namespace NKikimr::NSchemeShard {

bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys) {
    if (!ColumnFamilies.Parse(tableSchema, errors)) {
        return false;
    }

    if (!Columns.Parse(tableSchema, errors, allowNullKeys)) {
        return false;
    }

    return true;
}

bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
    if (!ColumnFamilies.Parse(alterRequest, errors)) {
        return false;
    }

    if (!Columns.Parse(alterRequest, errors)) {
        return false;
    }

    if (!Indexes.Parse(alterRequest, errors)) {
        return false;
    }

    if (!Options.Parse(alterRequest, errors)) {
        return false;
    }

    return true;
}
}
