#pragma once

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NSecret {

class TSecretInitializer: public IInitializationBehaviour {
protected:
    virtual void DoPrepare(NMetadataInitializer::IInitializerInput::TPtr controller) const override;
public:
};

class TAccessInitializer: public IInitializationBehaviour {
protected:
    virtual void DoPrepare(NMetadataInitializer::IInitializerInput::TPtr controller) const override;
public:
};

}
