#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRulesManager: public NMetadata::TGenericOperationsManager<TTieringRule> {
private:
    static TFactory::TRegistrator<TTieringRulesManager> Registrator;
protected:
    virtual NMetadata::IInitializationBehaviour::TPtr DoGetInitializationBehaviour() const override;
};

}
