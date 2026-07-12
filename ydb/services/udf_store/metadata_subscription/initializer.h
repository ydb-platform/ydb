#pragma once

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NUdfStore {

using namespace NMetadata;

class TUdfInitializer: public NInitializer::IInitializationBehaviour {
protected:
    virtual void DoPrepare(NInitializer::IInitializerInput::TPtr controller) const override {
        controller->OnPreparationFinished({});

    }
public:
};

} // namespace NKikimr::NUdfStore
