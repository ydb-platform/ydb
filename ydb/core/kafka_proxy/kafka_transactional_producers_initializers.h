#pragma once

#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NGRpcProxy::V1 {

using TInitBehaviourPtr = std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour>;
using TClassBehaviourPtr = std::shared_ptr<NMetadata::IClassBehaviour>;

class TTransactionalProducersInitializer : public NMetadata::NInitializer::IInitializationBehaviour {
public:
    static TInitBehaviourPtr GetInstant() {
        static TInitBehaviourPtr res{new TTransactionalProducersInitializer()};
        return res;
    }

protected:
    virtual void DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const override;

private:
    TTransactionalProducersInitializer() = default;
};

class TTransactionalProducersInitManager : public NMetadata::IClassBehaviour {
protected:
    virtual TString GetInternalStorageTablePath() const override {
        return "kafka_transactional_producers";
    }

    TInitBehaviourPtr ConstructInitializer() const override {
        return TTransactionalProducersInitializer::GetInstant();
    }

public:
    std::shared_ptr<NMetadata::NModifications::IOperationsManager> GetOperationsManager() const override {
        return nullptr;
    }

    static TClassBehaviourPtr GetInstant() {
        static TClassBehaviourPtr res{new TTransactionalProducersInitManager()};
        return res;
    }

    virtual TString GetTypeId() const override {
        return TypeName<TTransactionalProducersInitManager>();
    }

private:
    TTransactionalProducersInitManager() = default;
};

}
