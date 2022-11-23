#pragma once
#include "config.h"

#include <ydb/services/metadata/abstract/common.h>

namespace NKikimr::NBackgroundTasks {

class TBGTasksInitializer: public NMetadata::IInitializationBehaviour {
private:
    const TConfig Config;
protected:
    virtual void DoPrepare(NMetadataInitializer::IInitializerInput::TPtr controller) const override;
public:
    TBGTasksInitializer(const TConfig& config)
        : Config(config)
    {

    }
};

}
