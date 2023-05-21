#include "abstract.h"
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

namespace NKikimr::NKqp {

TConclusionStatus ITableStoreOperation::Deserialize(const NYql::TObjectSettingsImpl& settings) {
    std::pair<TString, TString> pathPair;
    {
        TString error;
        if (!NYql::IKikimrGateway::TrySplitTablePath(settings.GetObjectId(), pathPair, error)) {
            return TConclusionStatus::Fail(error);
        }
        WorkingDir = pathPair.first;
        StoreName = pathPair.second;
    }
    {
        auto presetName = settings.GetFeaturesExtractor().Extract("PRESET_NAME");
        if (presetName) {
            PresetName = *presetName;
        }
    }
    auto serializationResult = DoDeserialize(settings.GetFeaturesExtractor());
    if (!serializationResult) {
        return serializationResult;
    }
    if (!settings.GetFeaturesExtractor().IsFinished()) {
        return TConclusionStatus::Fail("there are undefined parameters: " + settings.GetFeaturesExtractor().GetRemainedParamsString());
    }
    return TConclusionStatus::Success();
}

void ITableStoreOperation::SerializeScheme(NKikimrSchemeOp::TModifyScheme& scheme) const {
    scheme.SetWorkingDir(WorkingDir);
    scheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnStore);

    NKikimrSchemeOp::TAlterColumnStore* alter = scheme.MutableAlterColumnStore();
    alter->SetName(StoreName);
    auto schemaPresetObject = alter->AddAlterSchemaPresets();
    schemaPresetObject->SetName(PresetName);
    return DoSerializeScheme(*schemaPresetObject);
}

}
