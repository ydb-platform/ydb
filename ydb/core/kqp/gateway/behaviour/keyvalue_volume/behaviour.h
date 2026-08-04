#pragma once

#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/abstract/kqp_common.h>

namespace NKikimr::NKqp {

class TKeyValueVolumeConfig {
public:
    static TString GetTypeId() {
        return "KEY_VALUE_VOLUME";
    }
};

class TKeyValueVolumeBehaviour: public NMetadata::TClassBehaviour<TKeyValueVolumeConfig> {
private:
    static TFactory::TRegistrator<TKeyValueVolumeBehaviour> Registrator;

protected:
    virtual std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override;
    virtual std::shared_ptr<NMetadata::NModifications::IOperationsManager> ConstructOperationsManager() const override;

    virtual TString GetInternalStorageTablePath() const override;
    virtual TString GetTypeId() const override;
};

}   // namespace NKikimr::NKqp
