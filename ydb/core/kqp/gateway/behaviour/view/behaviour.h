#pragma once

#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NKqp {

struct TViewConfig {
    static TString GetTypeId() {
        return "VIEW";
    }
};

class TViewBehaviour: public NMetadata::TClassBehaviour<TViewConfig> {
private:
    static TFactory::TRegistrator<TViewBehaviour> Registrator;

protected:
    std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override;
    std::shared_ptr<NMetadata::NModifications::IOperationsManager> ConstructOperationsManager() const override;

    TString GetInternalStorageTablePath() const override;
    TString GetTypeId() const override;
};

}
