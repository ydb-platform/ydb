#pragma once
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NKqp {

class TExternalDataSourceConfig {
public:
    static TString GetTypeId() {
        return "EXTERNAL_DATA_SOURCE";
    }
};

class TExternalDataSourceBehaviour: public NMetadata::TClassBehaviour<TExternalDataSourceConfig> {
private:
    static TFactory::TRegistrator<TExternalDataSourceBehaviour> Registrator;
protected:
    virtual std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override;
    virtual std::shared_ptr<NMetadata::NModifications::IOperationsManager> ConstructOperationsManager() const override;

    virtual TString GetInternalStorageTablePath() const override;
    virtual TString GetTypeId() const override;
};

}
