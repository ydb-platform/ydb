#pragma once
#include "secret.h"
#include "initializer.h"
#include <ydb/services/metadata/manager/common.h>

namespace NKikimr::NMetadata::NSecret {

class TSecretBehaviour: public IClassBehaviour {
private:
    static TFactory::TRegistrator<TSecretBehaviour> Registrator;
protected:
    virtual NInitializer::IInitializationBehaviour::TPtr ConstructInitializer() const override;
    virtual NModifications::IOperationsManager::TPtr ConstructOperationsManager() const override;
    virtual TString GetInternalStorageTablePath() const override;

public:
    virtual TString GetTypeId() const override;
};

}
