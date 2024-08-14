#pragma once

#include "object.h"

#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/abstract/kqp_common.h>


namespace NKikimr::NKqp {

class TResourcePoolClassifierBehaviour : public NMetadata::TClassBehaviour<TResourcePoolClassifierConfig> {
    static TFactory::TRegistrator<TResourcePoolClassifierBehaviour> Registrator;

protected:
    virtual NMetadata::NInitializer::IInitializationBehaviour::TPtr ConstructInitializer() const override;
    virtual NMetadata::NModifications::IOperationsManager::TPtr ConstructOperationsManager() const override;
    virtual TString GetInternalStorageTablePath() const override;

public:
    virtual TString GetTypeId() const override;

    static IClassBehaviour::TPtr GetInstance();
};

}  // namespace NKikimr::NKqp
