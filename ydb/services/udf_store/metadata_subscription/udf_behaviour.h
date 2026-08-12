#pragma once
#include "udf_meta.h"
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NUdfStore {

class TUdfBehaviour: public NMetadata::TClassBehaviour<TUdfMeta> {
    virtual NMetadata::NInitializer::IInitializationBehaviour::TPtr ConstructInitializer() const override {
        return {};
    }
    virtual NMetadata::NModifications::IOperationsManager::TPtr ConstructOperationsManager() const override {
        return {};
    }
    virtual TString GetInternalStorageTablePath() const override {
        return "udf_store/meta";
    }
    virtual TString GetTypeId() const override {
        return "UdfMeta";
    }

public:
    TUdfBehaviour() = default;
    static IClassBehaviour::TPtr GetInstance() {
        static std::shared_ptr<TUdfBehaviour> result = std::make_shared<TUdfBehaviour>();
        return result;
    }

};

} // namespace NKikimr::NUdfStore
