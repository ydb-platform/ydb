#include "upsert_stat.h"
#include <util/string/type.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp {

TConclusionStatus TUpsertStatOperation::DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) {
    {
        auto fValue = features.Extract("NAME");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find  alter parameter NAME");
        }
        Name = *fValue;
    }
    TString type;
    {
        auto fValue = features.Extract("TYPE");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find alter parameter TYPE");
        }
        type = *fValue;
    }
    {
        auto fValue = features.Extract("FEATURES");
        if (!fValue) {
            return TConclusionStatus::Fail("can't find alter parameter FEATURES");
        }
        if (!Constructor.Initialize(type)) {
            return TConclusionStatus::Fail("can't initialize stat constructor object for type \"" + type + "\"");
        }
        NJson::TJsonValue jsonData;
        if (!NJson::ReadJsonFastTree(*fValue, &jsonData)) {
            return TConclusionStatus::Fail("incorrect json in request FEATURES parameter");
        }
        auto result = Constructor->DeserializeFromJson(jsonData);
        if (result.IsFail()) {
            return result;
        }
    }
    return TConclusionStatus::Success();
}

void TUpsertStatOperation::DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const {
    auto* proto = schemaData.AddUpsertStatistics();
    proto->SetName(Name);
    Constructor.SerializeToProto(*proto);
}

}
