#pragma once

#include "library_source.h"

#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NUdfStore {

class TLibraryBehaviour: public NMetadata::TClassBehaviour<TUdfLibrarySource> {
    virtual NMetadata::NInitializer::IInitializationBehaviour::TPtr ConstructInitializer() const override {
        return {};
    }
    virtual NMetadata::NModifications::IOperationsManager::TPtr ConstructOperationsManager() const override {
        return {};
    }
    virtual TString GetInternalStorageTablePath() const override {
        return "udf_store/library_source";
    }
    virtual TString GetTypeId() const override {
        return "UdfLibrarySource";
    }

public:
    TLibraryBehaviour() = default;
    static NMetadata::IClassBehaviour::TPtr GetInstance() {
        static std::shared_ptr<TLibraryBehaviour> result = std::make_shared<TLibraryBehaviour>();
        return result;
    }
};

} // namespace NKikimr::NUdfStore
