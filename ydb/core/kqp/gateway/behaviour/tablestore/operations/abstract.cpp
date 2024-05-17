#include "abstract.h"
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

namespace NKikimr::NKqp {

TConclusionStatus ITableStoreOperation::Deserialize(const NYql::TObjectSettingsImpl& settings) {
    std::pair<TString, TString> pathPair;
    {
        TString error;
        if (!NSchemeHelpers::TrySplitTablePath(settings.GetObjectId(), pathPair, error)) {
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

void ITableStoreOperation::DoSerializeScheme(NKikimrSchemeOp::TModifyScheme& scheme, const bool isStandalone) const {
    if (isStandalone) {
        scheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnTable);
        NKikimrSchemeOp::TAlterColumnTable* alter = scheme.MutableAlterColumnTable();
        alter->SetName(StoreName);
        auto schemaObject = alter->MutableAlterSchema();
        return DoSerializeScheme(*schemaObject);
    } else {
        scheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnStore);
        NKikimrSchemeOp::TAlterColumnStore* alter = scheme.MutableAlterColumnStore();
        alter->SetName(StoreName);
        auto schemaPresetObject = alter->AddAlterSchemaPresets();
        schemaPresetObject->SetName(PresetName);
        return DoSerializeScheme(*(schemaPresetObject->MutableAlterSchema()));
    }
}

void ITableStoreOperation::SerializeScheme(NKikimrSchemeOp::TModifyScheme& scheme, const bool isStandalone) const {
    scheme.SetWorkingDir(WorkingDir);
    DoSerializeScheme(scheme, isStandalone);
}

}
