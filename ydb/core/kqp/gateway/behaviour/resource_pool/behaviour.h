#pragma once

#include <ydb/services/metadata/abstract/kqp_common.h>


namespace NKikimr::NKqp {

class TResourcePoolConfig {
public:
    static TString GetTypeId() {
        return "RESOURCE_POOL";
    }
};

class TResourcePoolBehaviour: public NMetadata::TClassBehaviour<TResourcePoolConfig> {
    static TFactory::TRegistrator<TResourcePoolBehaviour> Registrator;

protected:
    virtual std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override;
    virtual std::shared_ptr<NMetadata::NModifications::IOperationsManager> ConstructOperationsManager() const override;
    virtual TString GetInternalStorageTablePath() const override;

public:
    virtual TString GetTypeId() const override;
};

}  // namespace NKikimr::NKqp
