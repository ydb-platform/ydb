#pragma once

#include "behaviour.h"

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NKqp {

class TTableStoreManager: public NMetadata::NModifications::IOperationsManager {
protected:
    NThreading::TFuture<TYqlConclusionStatus> DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        NMetadata::IClassBehaviour::TPtr manager, TInternalModificationContext& context) const override;
public:
    using NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
};

}
