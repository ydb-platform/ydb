#pragma once
#include "udf_module.h"
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NUdfStore {

class TUdfModuleBehaviour: public NMetadata::TClassBehaviour<TUdfModule> {
    virtual NMetadata::NInitializer::IInitializationBehaviour::TPtr ConstructInitializer() const override {
        return {};
    }
    virtual NMetadata::NModifications::IOperationsManager::TPtr ConstructOperationsManager() const override {
        return {};
    }
    virtual TString GetInternalStorageTablePath() const override {
        return "udf_store/modules";
    }
    virtual TString GetTypeId() const override {
        return "UdfModule";
    }

public:
    TUdfModuleBehaviour() = default;
    static IClassBehaviour::TPtr GetInstance() {
        static std::shared_ptr<TUdfModuleBehaviour> result = std::make_shared<TUdfModuleBehaviour>();
        return result;
    }
};

} // namespace NKikimr::NUdfStore
