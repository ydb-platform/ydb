#pragma once
#include "secret.h"
#include "initializer.h"
#include <ydb/services/metadata/manager/common.h>

namespace NKikimr::NMetadata::NSecret {

class TSecretBehaviour: public TClassBehaviour<TSecret> {
private:
    static TFactory::TRegistrator<TSecretBehaviour> Registrator;
protected:
    virtual NInitializer::IInitializationBehaviour::TPtr ConstructInitializer() const override;
    virtual NModifications::IOperationsManager::TPtr ConstructOperationsManager() const override;
    virtual TString GetInternalStorageTablePath() const override;

public:
    TSecretBehaviour() = default;
    static IClassBehaviour::TPtr GetInstance();

    virtual TString GetTypeId() const override;
};

}
