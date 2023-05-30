#include "alter_column.h"

namespace NKikimr::NKqp::NColumnshard {

TConclusionStatus TAlterColumnOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    {
        auto fValue = features.Extract("NAME");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find alter parameter NAME");
        }
        ColumnName = *fValue;
    }
    {
        auto result = DictionaryEncodingDiff.DeserializeFromRequestFeatures(features);
        if (!result) {
            return TConclusionStatus::Fail(result.GetErrorMessage());
        }
    }
    {
        auto result = CompressionDiff.DeserializeFromRequestFeatures(features);
        if (!result) {
            return TConclusionStatus::Fail(result.GetErrorMessage());
        }
    }
    return TConclusionStatus::Success();
}

void TAlterColumnOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchemaPreset& presetProto) const {
    auto schemaData = presetProto.MutableAlterSchema();
    auto* column = schemaData->AddAlterColumns();
    column->SetName(ColumnName);
    *column->MutableCompression() = CompressionDiff.SerializeToProto();
    *column->MutableDictionaryEncoding() = DictionaryEncodingDiff.SerializeToProto();
}

}
