#include "add_column.h"
#include <util/string/type.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>

namespace NKikimr::NKqp {

TConclusionStatus TAddColumnOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    {
        auto fValue = features.Extract("NAME");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find  alter parameter NAME");
        }
        ColumnName = *fValue;
    }
    StorageId = features.Extract("STORAGE_ID");
    if (StorageId && !*StorageId) {
        return TConclusionStatus::Fail("STORAGE_ID cannot be empty string");
    }
    if (StorageId && !NKikimr::NOlap::NBlobOperations::IsValidColumnStorageId(*StorageId)) {
        return TConclusionStatus::Fail("Unsupported STORAGE_ID '" + *StorageId
            + "': only '__DEFAULT', '__MEMORY' and '__LOCAL_METADATA' are allowed for column tables");
    }
    {
        auto fValue = features.Extract("TYPE");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find alter parameter TYPE");
        }
        ColumnType = *fValue;
    }
    {
        auto fValue = features.Extract("NOT_NULL");
        if (!!fValue) {
            NotNull = IsTrue(*fValue);
        }
    }
    return TConclusionStatus::Success();
}

void TAddColumnOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    auto column = schemaData.AddAddColumns();
    column->SetName(ColumnName);
    column->SetType(ColumnType);
    if (StorageId) {
        column->SetStorageId(*StorageId);
    }
    column->SetNotNull(NotNull);
}

}
