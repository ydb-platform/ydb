#pragma once

#include "object.h"

#include <ydb/services/metadata/abstract/kqp_common.h>

namespace NKikimr::NKqp {

class TStreamingQueryBehaviour : public NMetadata::TClassBehaviour<TStreamingQueryConfig> {
    static TFactory::TRegistrator<TStreamingQueryBehaviour> Registrator;

protected:
    std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> ConstructInitializer() const override;

    TString GetInternalStorageTablePath() const override;

public:
    std::shared_ptr<NMetadata::NModifications::IOperationsManager> ConstructOperationsManager() const override;

    TString GetTypeId() const override;

    static IClassBehaviour::TPtr GetInstance();
};

}  // namespace NKikimr::NKqp
