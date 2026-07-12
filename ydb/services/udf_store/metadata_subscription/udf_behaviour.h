#pragma once
#include "udf_meta.h"
#include "initializer.h"
#include "manager.h"
#include <ydb/services/metadata/manager/common.h>

namespace NKikimr::NUdfStore {

class TUdfBehaviour: public TClassBehaviour<TUdfMeta> {
private:
protected:
    virtual NInitializer::IInitializationBehaviour::TPtr ConstructInitializer() const override {
        return std::make_shared<TUdfInitializer>();
    }
    virtual NModifications::IOperationsManager::TPtr ConstructOperationsManager() const override {
        return std::make_shared<TUdfManager>();
    }
    virtual TString GetInternalStorageTablePath() const override {
        return "udf_store/meta";
    }

public:
    TUdfBehaviour() = default;
    static IClassBehaviour::TPtr GetInstance() {
        static std::shared_ptr<TUdfBehaviour> result = std::make_shared<TUdfBehaviour>();
        return result;
    }

    virtual TString GetTypeId() const override {
        return TUdfMeta::GetTypeId();
    }
};

} // namespace NKikimr::NUdfStore
