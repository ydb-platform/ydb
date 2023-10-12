#pragma once
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NKqp {

class TOlapTableConfig {
public:
    static TString GetTypeId() {
        return "TABLE";
    }
};

class TOlapTableBehaviour: public NMetadata::TClassBehaviour<TOlapTableBehaviour> {
private:
    static TFactory::TRegistrator<TOlapTableBehaviour> Registrator;
protected:
    virtual std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override;
    virtual std::shared_ptr<NMetadata::NModifications::IOperationsManager> ConstructOperationsManager() const override;

    virtual TString GetInternalStorageTablePath() const override;
    virtual TString GetTypeId() const override;
};

}
