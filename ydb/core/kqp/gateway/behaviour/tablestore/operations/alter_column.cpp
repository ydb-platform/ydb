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
    DefaultValue = features.Extract("DEFAULT_VALUE");

    StorageId = features.Extract("STORAGE_ID");
    if (StorageId && !*StorageId) {
        return TConclusionStatus::Fail("STORAGE_ID cannot be empty string");
    }
    {
        auto status = AccessorConstructor.DeserializeFromRequest(features);
        if (status.IsFail()) {
            return status;
        }
    }
    {
        auto result = DictionaryEncodingDiff.DeserializeFromRequestFeatures(features);
        if (result.IsFail()) {
            return result;
        }
    }
    {
        auto status = Serializer.DeserializeFromRequest(features);
        if (status.IsFail()) {
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
    if (!!AccessorConstructor) {
        *column->MutableDataAccessorConstructor() = AccessorConstructor.SerializeToProto();
    }
    *column->MutableDictionaryEncoding() = DictionaryEncodingDiff.SerializeToProto();
    if (DefaultValue) {
        column->SetDefaultValue(*DefaultValue);
    }
}

}
