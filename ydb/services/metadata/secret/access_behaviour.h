#pragma once
#include "access.h"

#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/manager/abstract.h>
#include <ydb/services/metadata/manager/common.h>

namespace NKikimr::NMetadata::NSecret {

class TAccessBehaviour: public IClassBehaviour {
private:
    static TFactory::TRegistrator<TAccessBehaviour> Registrator;
protected:
    virtual NInitializer::IInitializationBehaviour::TPtr ConstructInitializer() const override;
    virtual NModifications::IOperationsManager::TPtr ConstructOperationsManager() const override;
    virtual TString GetInternalStorageTablePath() const override;

public:
    virtual TString GetTypeId() const override;
};

}
