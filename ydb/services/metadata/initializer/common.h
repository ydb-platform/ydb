#pragma once
#include <ydb/services/metadata/request/config.h>
#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NMetadataInitializer {

class ITableModifier {
protected:
    virtual bool DoExecute(const TActorId& resultCallbackId, const NInternal::NRequest::TConfig& config) const = 0;
public:
    using TPtr = std::shared_ptr<ITableModifier>;
    virtual ~ITableModifier() = default;
    bool Execute(const TActorId& resultCallbackId, const NInternal::NRequest::TConfig& config) const {
        return DoExecute(resultCallbackId, config);
    }
};

template <class TDialogPolicy>
class TGenericTableModifier: public ITableModifier {
private:
    YDB_READONLY_DEF(typename TDialogPolicy::TRequest, Request);
protected:
    virtual bool DoExecute(const TActorId& resultCallbackId, const NInternal::NRequest::TConfig& config) const override {
        TActivationContext::ActorSystem()->Register(new NInternal::NRequest::TYDBRequest<TDialogPolicy>(Request, resultCallbackId, config));
        return true;
    }
public:
    TGenericTableModifier(typename TDialogPolicy::TRequest& request)
        : Request(request) {

    }
};

class IController {
public:
    using TPtr = std::shared_ptr<IController>;
    virtual void PreparationFinished(const TVector<ITableModifier::TPtr>& modifiers) const = 0;
    virtual void PreparationProblem(const TString& errorMessage) const = 0;
    virtual ~IController() = default;
};

}
