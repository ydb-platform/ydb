#include "schema.h"

namespace NKikimr::NSchemeShard {

void TOlapSchemaUpdate::PutCurrentFamilies(const std::vector<TOlapColumnFamlilyAdd>& currentColumnFamilies) {
    for (const auto& family : currentColumnFamilies) {
        CurrentColumnFamilies[family.GetName()] = family;
    }
}

    bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys) {
        if (tableSchema.HasEngine()) {
            Engine = tableSchema.GetEngine();
        }

        if (!ColumnFamilies.Parse(tableSchema, errors)) {
            return false;
        }

        if (!Columns.Parse(tableSchema, errors, allowNullKeys)) {
            return false;
        }

        return true;
    }

    bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
        if (!ColumnFamilies.Parse(CurrentColumnFamilies, alterRequest, errors)) {
            return false;
        }

        if (!Columns.Parse(CurrentColumnFamilies, alterRequest, errors)) {
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
