#pragma once

#include "snapshot.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NMetadata::NInitializer {

class TInitializer: public IInitializationBehaviour {
protected:
    virtual void DoPrepare(IInitializerInput::TPtr controller) const override;

public:
};

}   // namespace NKikimr::NMetadata::NInitializer
