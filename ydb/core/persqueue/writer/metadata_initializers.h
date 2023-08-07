#pragma once

#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NGRpcProxy::V1 {

using TInitBehaviourPtr = std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour>;
using TClassBehaviourPtr = std::shared_ptr<NMetadata::IClassBehaviour>;

class TSrcIdMetaInitializer : public NMetadata::NInitializer::IInitializationBehaviour {
public:
    static TInitBehaviourPtr GetInstant() {
        static TInitBehaviourPtr res{new TSrcIdMetaInitializer()};
        return res;
    }

protected:
    virtual void DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const override;

private:
    TSrcIdMetaInitializer() = default;
};

class TSrcIdMetaInitManager : public NMetadata::IClassBehaviour {
protected:
    virtual TString GetInternalStorageTablePath() const override {
        return "TopicPartitionsMapping";
    }

    TInitBehaviourPtr ConstructInitializer() const override {
        return TSrcIdMetaInitializer::GetInstant();
    }

public:
    std::shared_ptr<NMetadata::NModifications::IOperationsManager> GetOperationsManager() const override {
        return nullptr;
    }

    static TClassBehaviourPtr GetInstant() {
        static TClassBehaviourPtr res{new TSrcIdMetaInitManager()};
        return res;
    }

    virtual TString GetTypeId() const override {
        return TypeName<TSrcIdMetaInitManager>();
    }

private:
    TSrcIdMetaInitManager() = default;
};

} // namespace NKikimr::NGRpcProxy::V1
