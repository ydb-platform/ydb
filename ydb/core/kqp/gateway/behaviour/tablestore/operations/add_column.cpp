#include "add_column.h"
#include <util/string/type.h>

namespace NKikimr::NKqp {

NMetadata::NModifications::TObjectOperatorResult TAddColumnOperation::DoDeserialize(const NYql::TObjectSettingsImpl::TFeatures& features) {
    {
        auto it = features.find("NAME");
        if (it == features.end()) {
            return NMetadata::NModifications::TObjectOperatorResult("can't find  alter parameter NAME");
        }
        ColumnName = it->second;
    }
    {
        auto it = features.find("TYPE");
        if (it == features.end()) {
            return NMetadata::NModifications::TObjectOperatorResult("can't find alter parameter TYPE");
        }
        ColumnType = it->second;
    }
    {
        auto it = features.find("NOT_NULL");
        if (it != features.end()) {
            NotNull = IsTrue(it->second);
        }
    }
    return NMetadata::NModifications::TObjectOperatorResult(true);
}

void TAddColumnOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchemaPreset& presetProto) const {
    auto schemaData = presetProto.MutableAlterSchema();
    auto column = schemaData->AddColumns();
    column->SetName(ColumnName);
    column->SetType(ColumnType);
    column->SetNotNull(NotNull);
}

}
