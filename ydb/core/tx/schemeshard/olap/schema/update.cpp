#include "schema.h"

namespace NKikimr::NSchemeShard {

void TOlapSchemaUpdate::PutCurrentFamilies(const TOlapColumnFamiliesDescription& currentColumnFamilies) {
    CurrentColumnFamilies = currentColumnFamilies;
}

    bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys) {
        if (tableSchema.HasEngine()) {
            Engine = tableSchema.GetEngine();
        }

        TOlapColumnFamiliesUpdate columnFamiliesUpdate;
        if (!columnFamiliesUpdate.Parse(tableSchema, errors)) {
            return false;
        }
        if (!CurrentColumnFamilies.ApplyUpdate(columnFamiliesUpdate, errors)) {
            return false;
        }

        if (!Columns.Parse(CurrentColumnFamilies, tableSchema, errors, allowNullKeys)) {
            return false;
        }

        return true;
    }

    bool TOlapSchemaUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
        TOlapColumnFamiliesUpdate columnFamiliesUpdate;
        if (!columnFamiliesUpdate.Parse(alterRequest, errors)) {
            return false;
        }

        if (!CurrentColumnFamilies.ApplyUpdate(columnFamiliesUpdate, errors)) {
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
