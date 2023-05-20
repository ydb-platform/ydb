#include "alter_column.h"

namespace NKikimr::NKqp::NColumnshard {

NKikimr::NMetadata::NModifications::TObjectOperatorResult TAlterColumnOperation::DoDeserialize(const NYql::TObjectSettingsImpl::TFeatures& features) {
    {
        auto it = features.find("NAME");
        if (it == features.end()) {
            return NMetadata::NModifications::TObjectOperatorResult("can't find alter parameter NAME");
        }
        ColumnName = it->second;
    }
    {
        auto it = features.find("LOW_CARDINALITY");
        if (it != features.end()) {
            bool value;
            if (!TryFromString<bool>(it->second, value)) {
                return NMetadata::NModifications::TObjectOperatorResult("cannot parse LOW_CARDINALITY as bool");
            }
            LowCardinality = value;
        }
    }
    auto result = CompressionDiff.DeserializeFromRequestFeatures(features);
    if (!result) {
        return NMetadata::NModifications::TObjectOperatorResult(result.GetErrorMessage());
    }
    return NMetadata::NModifications::TObjectOperatorResult(true);
}

void TAlterColumnOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchemaPreset& presetProto) const {
    auto schemaData = presetProto.MutableAlterSchema();
    auto* column = schemaData->AddAlterColumns();
    column->SetName(ColumnName);
    *column->MutableCompression() = CompressionDiff.SerializeToProto();
    if (LowCardinality) {
        column->SetLowCardinality(*LowCardinality);
    }
}

}
