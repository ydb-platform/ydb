#pragma once

#include "object.h"
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/common.h>

namespace NKikimr::NMetadata::NInitializer {

class TDBObjectBehaviour: public TClassBehaviour<TDBInitialization> {
protected:
    virtual IInitializationBehaviour::TPtr ConstructInitializer() const override;
    virtual std::shared_ptr<NModifications::IOperationsManager> ConstructOperationsManager() const override;

    virtual TString GetInternalStorageTablePath() const override;
    virtual TString GetInternalStorageHistoryTablePath() const override {
        return "";
    }

public:
    virtual TString GetTypeId() const override;
    static std::shared_ptr<TDBObjectBehaviour> GetInstance();
};

}
