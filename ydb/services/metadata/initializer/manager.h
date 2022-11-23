#pragma once

#include "object.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/manager/generic_manager.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadataInitializer {

class TManager: public NMetadata::TGenericOperationsManager<TDBInitialization> {
private:
    static TFactory::TRegistrator<TManager> Registrator;
protected:
    virtual NMetadata::IInitializationBehaviour::TPtr DoGetInitializationBehaviour() const override;
public:
};

}
