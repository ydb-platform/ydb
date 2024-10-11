#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/scheme_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRulesManager: public NMetadata::NModifications::TSchemeObjectOperationsManager {
protected:
    void DoPreprocessSettings(
        const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context, IPreprocessingController::TPtr controller) const override;
    TOperationParsingResult DoBuildPatchFromSettings(
        const NYql::TObjectSettingsImpl& settings, NSchemeShard::TSchemeShard& context) const override;
    TConclusion<TObjectDependencies> DoValidateOperation(
        const TString& objectId, const NMetadata::NModifications::TBaseObject::TPtr& object, EActivityType activity, NSchemeShard::TSchemeShard& context) const override;
};

}
