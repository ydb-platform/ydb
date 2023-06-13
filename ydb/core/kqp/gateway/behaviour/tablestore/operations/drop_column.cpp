#include "drop_column.h"
#include <util/string/type.h>

namespace NKikimr::NKqp {

TConclusionStatus TDropColumnOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    {
        auto fValue = features.Extract("NAME");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find  alter parameter NAME");
        }
        ColumnName = *fValue;
    }
    return TConclusionStatus::Success();
}

void TDropColumnOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchemaPreset& presetProto) const {
    auto schemaData = presetProto.MutableAlterSchema();
    auto column = schemaData->AddDropColumns();
    column->SetName(ColumnName);
}

}
