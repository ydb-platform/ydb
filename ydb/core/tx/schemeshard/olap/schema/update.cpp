#include "schema.h"

namespace NKikimr::NSchemeShard {

    bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys) {
        if (tableSchema.HasEngine()) {
            Engine = tableSchema.GetEngine();
        }

        if (!Columns.Parse(tableSchema, errors, allowNullKeys)) {
            return false;
        }

        return true;
    }

    bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
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
