#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTiersManager: public NMetadata::TGenericOperationsManager<TTierConfig> {
private:
    static TFactory::TRegistrator<TTiersManager> Registrator;
protected:
    virtual NMetadata::IInitializationBehaviour::TPtr DoGetInitializationBehaviour() const override;
public:
};

}
