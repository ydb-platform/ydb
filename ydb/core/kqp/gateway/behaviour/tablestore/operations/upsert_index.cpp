#include "upsert_index.h"
#include <util/string/type.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp {

TConclusionStatus TUpsertIndexOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    {
        auto fValue = features.Extract("NAME");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find  alter parameter NAME");
        }
        IndexName = *fValue;
    }
    StorageId = features.Extract("STORAGE_ID");
    if (StorageId && !*StorageId) {
        return TConclusionStatus::Fail("STORAGE_ID cannot be empty string");
    }
    TString indexType;
    {
        auto fValue = features.Extract("TYPE");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find alter parameter TYPE");
        }
        indexType = *fValue;
    }
    {
        auto fValue = features.Extract("FEATURES");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find alter parameter FEATURES");
        }
        if (!IndexMetaConstructor.Initialize(indexType)) {
            return TConclusionStatus::Fail("can't initialize index meta object for type \"" + indexType + "\"");
        }
        NJson::TJsonValue jsonData;
        if (!NJson::ReadJsonFastTree(*fValue, &jsonData)) {
            return TConclusionStatus::Fail("incorrect json in request FEATURES parameter");
        }
        auto result = IndexMetaConstructor->DeserializeFromJson(jsonData);
        if (result.IsFail()) {
            return result;
        }
    }
    return TConclusionStatus::Success();
}

void TUpsertIndexOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    auto* indexProto = schemaData.AddUpsertIndexes();
    if (StorageId) {
        indexProto->SetStorageId(*StorageId);
    }
    indexProto->SetName(IndexName);
    IndexMetaConstructor.SerializeToProto(*indexProto);
}

}
