#pragma once
#include "snapshot.h"
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/manager/generic_manager.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NSecret {

class TSecretManager: public TGenericOperationsManager<TSecret> {
private:
    static TFactory::TRegistrator<TSecretManager> Registrator;
protected:
    virtual IInitializationBehaviour::TPtr DoGetInitializationBehaviour() const override;
public:
};

class TAccessManager: public TGenericOperationsManager<TAccess> {
private:
    static TFactory::TRegistrator<TAccessManager> Registrator;
protected:
    virtual IInitializationBehaviour::TPtr DoGetInitializationBehaviour() const override;
public:
};

}
