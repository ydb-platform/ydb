#include "abstract.h"
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

namespace NKikimr::NKqp {

NKikimr::NMetadata::NModifications::TObjectOperatorResult ITableStoreOperation::Deserialize(const NYql::TObjectSettingsImpl& settings) {
    std::pair<TString, TString> pathPair;
    {
        TString error;
        if (!NYql::IKikimrGateway::TrySplitTablePath(settings.GetObjectId(), pathPair, error)) {
            return NMetadata::NModifications::TObjectOperatorResult(error);
        }
        WorkingDir = pathPair.first;
        StoreName = pathPair.second;
    }
    {
        auto it = settings.GetFeatures().find("PRESET_NAME");
        if (it != settings.GetFeatures().end()) {
            PresetName = it->second;
        }
    }
    return DoDeserialize(settings.GetFeatures());
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
