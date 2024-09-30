#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/scheme_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTiersManager: public NMetadata::NModifications::TSchemeObjectOperationsManager {
public:
    TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings, NSchemeShard::TSchemeShard& context) const override;

    void DoPreprocessSettings(
        const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context, IPreprocessingController::TPtr controller) const override;
};

}
