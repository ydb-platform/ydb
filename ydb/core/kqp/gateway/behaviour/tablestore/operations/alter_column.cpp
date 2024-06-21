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
    StorageId = features.Extract("STORAGE_ID");
    if (StorageId && !*StorageId) {
        return TConclusionStatus::Fail("STORAGE_ID cannot be empty string");
    }
    {
        auto result = DictionaryEncodingDiff.DeserializeFromRequestFeatures(features);
        if (!result) {
            return TConclusionStatus::Fail(result.GetErrorMessage());
        }
    }
    {
        auto status = Serializer.DeserializeFromRequest(features);
        if (!status) {
            return status;
        }
    }
    return TConclusionStatus::Success();
}

void TAlterColumnOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    auto* column = schemaData.AddAlterColumns();
    column->SetName(ColumnName);
    if (StorageId && !!*StorageId) {
        column->SetStorageId(*StorageId);
    }
    if (!!Serializer) {
        Serializer.SerializeToProto(*column->MutableSerializer());
    }
    *column->MutableDictionaryEncoding() = DictionaryEncodingDiff.SerializeToProto();
}

}
