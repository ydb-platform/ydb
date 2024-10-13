#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/scheme_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRulesManager: public NMetadata::NModifications::TSchemeObjectOperationsManager {
private:
    static TConclusion<NKikimrSchemeOp::TTieringIntervals> ConvertIntervalsToProto(const NJson::TJsonValue& jsonInfo);

    inline static const TString KeyDescription = "description";
    inline static const TString KeyDefaultColumn = "defaultColumn";

protected:
    void DoBuildRequestFromSettings(const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context,
        IBuildRequestController::TPtr controller) const override;

    TString GetStorageDirectory() const override;
};
}
