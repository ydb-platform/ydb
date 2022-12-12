#pragma once
#include "config.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NBackgroundTasks {

class TBGTasksInitializer: public NMetadata::NInitializer::IInitializationBehaviour {
private:
    const TConfig Config;
protected:
    virtual void DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const override;
public:
    TBGTasksInitializer(const TConfig& config)
        : Config(config)
    {

    }
};

}
