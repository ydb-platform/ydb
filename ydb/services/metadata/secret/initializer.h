#pragma once

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NSecret {

class TSecretInitializer: public NInitializer::IInitializationBehaviour {
protected:
    virtual void DoPrepare(NInitializer::IInitializerInput::TPtr controller) const override;
public:
};

class TAccessInitializer: public NInitializer::IInitializationBehaviour {
protected:
    virtual void DoPrepare(NInitializer::IInitializerInput::TPtr controller) const override;
public:
};

}
