#pragma once
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NKqp {

class TTableStoreConfig {
public:
    static TString GetTypeId() {
        return "TABLESTORE";
    }
};

class TTableStoreBehaviour: public NMetadata::TClassBehaviour<TTableStoreConfig> {
private:
    static TFactory::TRegistrator<TTableStoreBehaviour> Registrator;
protected:
    virtual std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override;
    virtual std::shared_ptr<NMetadata::NModifications::IOperationsManager> ConstructOperationsManager() const override;

    virtual TString GetInternalStorageTablePath() const override;
    virtual TString GetTypeId() const override;
};

}
