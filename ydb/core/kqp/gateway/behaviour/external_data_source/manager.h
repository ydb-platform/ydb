#pragma once

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NKqp {

class TExternalDataSourceManager: public NMetadata::NModifications::IOperationsManager {
    NThreading::TFuture<TYqlConclusionStatus> CreateExternalDataSource(const NYql::TObjectSettingsImpl& settings,
                                                                       TInternalModificationContext& context) const;

    NThreading::TFuture<TYqlConclusionStatus> DropExternalDataSource(const NYql::TObjectSettingsImpl& settings,
                                                                     TInternalModificationContext& context) const;

protected:
    NThreading::TFuture<TYqlConclusionStatus> DoModify(const NYql::TObjectSettingsImpl& settings,
                                                       const ui32 nodeId,
                                                       NMetadata::IClassBehaviour::TPtr manager,
                                                       TInternalModificationContext& context) const override;

public:
    using NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
};

}
