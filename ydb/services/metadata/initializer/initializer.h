#pragma once

#include "snapshot.h"
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadataInitializer {

class TInitializer: public NMetadata::IInitializationBehaviour {
protected:
    virtual void DoPrepare(NMetadataInitializer::IInitializerInput::TPtr controller) const override;
public:
};

}
