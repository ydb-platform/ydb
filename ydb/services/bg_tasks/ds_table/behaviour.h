#pragma once
#include "config.h"
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NBackgroundTasks {

class TBehaviour: public NMetadata::IClassBehaviour {
private:
    const TConfig Config;
public:
    virtual TString GetInternalStorageTablePath() const override {
        return Config.GetTablePath();
    }
    virtual std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override;
    virtual std::shared_ptr<NMetadata::NModifications::IOperationsManager> GetOperationsManager() const override;
    TBehaviour(const TConfig& config)
        : Config(config) {

    }
    virtual TString GetTypeId() const override {
        return "bg_tasks";
    }
};

}
