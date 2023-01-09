#pragma once

#include "object.h"
#include <ydb/services/metadata/abstract/kqp_common.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRuleBehaviour: public NMetadata::TClassBehaviour<TTieringRule> {
private:
    static TFactory::TRegistrator<TTieringRuleBehaviour> Registrator;
protected:
    virtual std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override;
    virtual std::shared_ptr<NMetadata::NModifications::IOperationsManager> ConstructOperationsManager() const override;

    virtual TString GetInternalStorageTablePath() const override;
    virtual TString GetTypeId() const override;

};

}
