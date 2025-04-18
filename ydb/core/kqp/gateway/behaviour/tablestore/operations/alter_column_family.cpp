#include "alter_column_family.h"

namespace NKikimr::NKqp::NColumnshard {

TConclusionStatus TAlterColumnFamilyOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    {
        auto fValue = features.Extract("NAME");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find alter parameter NAME");
        }
        ColumnFamilyName = *fValue;
    }

    {
        auto status = AccessorConstructor.DeserializeFromRequest(features);
        if (status.IsFail()) {
            return status;
        }
    }

    return TConclusionStatus::Success();
}

void TAlterColumnFamilyOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    auto* columnFamily = schemaData.AddAlterColumnFamily();
    columnFamily->SetName(ColumnFamilyName);
    if (!!AccessorConstructor) {
        *columnFamily->MutableDataAccessorConstructor() = AccessorConstructor.SerializeToProto();
    }
}

}
