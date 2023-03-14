#pragma once
#include "object.h"

#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/manager/abstract.h>
#include <ydb/services/metadata/manager/common.h>

namespace NKikimr::NMetadata::NCSIndex {

class TBehaviour: public TClassBehaviour<TObject> {
private:
    static TFactory::TRegistrator<TBehaviour> Registrator;
protected:
    virtual NInitializer::IInitializationBehaviour::TPtr ConstructInitializer() const override;
    virtual NModifications::IOperationsManager::TPtr ConstructOperationsManager() const override;
    virtual TString GetInternalStorageTablePath() const override;

public:
    TBehaviour() = default;
    virtual TString GetTypeId() const override;
    static IClassBehaviour::TPtr GetInstance();
};

}
