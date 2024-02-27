#pragma once

#include "counters.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_proxy/control_plane_proxy.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NFq::NPrivate {

using namespace NActors;
using namespace NThreading;
using namespace NYdb;

template<typename TDerived>
class TPlainBaseActor : public TActorBootstrapped<TDerived> {
public:
    using TBase = TActorBootstrapped<TDerived>;
    using TBase::Become;
    using TBase::SelfId;
    using TBase::Send;

public:
    TPlainBaseActor(const TActorId& successActorId,
                    const TActorId& errorActorId,
                    TDuration requestTimeout,
                    const TRequestCommonCountersPtr& counters)
        : Counters(counters)
        , SuccessActorId(successActorId)
        , ErrorActorId(errorActorId)
        , RequestTimeout(std::move(requestTimeout)) { }

    void Bootstrap() {
        CPP_LOG_T("TBaseActor Bootstrap started. Actor id: " << SelfId());
        Become(&TDerived::StateFunc, RequestTimeout, new NActors::TEvents::TEvWakeup());
        Counters->InFly->Inc();
        BootstrapImpl();
    }

    template<class THandler>
    void SendRequestToSender(TAutoPtr<THandler> handle) {
        Counters->Ok->Inc();
        Send(handle->Forward(SuccessActorId));
        PassAway();
    }

    void SendRequestToSender(IEventBase* event) {
        Counters->Ok->Inc();
        Send(SuccessActorId, event);
        PassAway();
    }

    void SendErrorMessageToSender(IEventBase* event,
                                  NActors::TActorIdentity::TEventFlags flags = 0,
                                  ui64 cookie                                = 0) {
        Counters->Error->Inc();
        Send(ErrorActorId, event, flags, cookie);
        PassAway();
    }

    void HandleTimeout() {
        CPP_LOG_W("TBaseActor Timeout occurred. Actor id: "
                  << SelfId());
        Counters->Timeout->Inc();
        SendErrorMessageToSender(MakeTimeoutEventImpl(
            MakeErrorIssue(TIssuesIds::TIMEOUT,
                           "Timeout occurred. Try repeating the request later")));
    }

    void PassAway() override {
        Counters->InFly->Dec();
        TBase::PassAway();
    }

protected:
    virtual void BootstrapImpl() = 0;
    virtual IEventBase* MakeTimeoutEventImpl(NYql::TIssue issue) = 0;

protected:
    const TRequestCommonCountersPtr Counters;
    const TActorId SuccessActorId;
    const TActorId ErrorActorId;
    const TDuration RequestTimeout;
};

template<typename T>
struct TBaseActorTypeTag {
    using TRequest  = typename T::TRequest;
    using TResponse = typename T::TResponse;
};

template<typename TDerived>
class TBaseActor : public TPlainBaseActor<TDerived> {
public:
    using TBase = TPlainBaseActor<TDerived>;
    using TBase::Become;
    using TBase::PassAway;
    using TBase::SelfId;
    using TBase::Send;

    using TEventRequestPtr = typename TBaseActorTypeTag<TDerived>::TRequest::TPtr;
    using TEventResponse   = typename TBaseActorTypeTag<TDerived>::TResponse;

public:
    TBaseActor(const TActorId& proxyActorId,
               const TEventRequestPtr request,
               TDuration requestTimeout,
               const TRequestCommonCountersPtr& counters)
        : TPlainBaseActor<TDerived>(proxyActorId,
                                    request->Sender,
                                    std::move(requestTimeout),
                                    counters)
        , Request(std::move(request)) { }

    void SendErrorMessageToSender(const NYql::TIssue& issue) {
        TBase::SendErrorMessageToSender(new TEventResponse({issue}, {}),
                                        0,
                                        Request->Cookie);
    }

    void HandleError(const TString& message, EStatus status, const NYql::TIssues& issues) {
        TString errorMessage = TStringBuilder{} << message << ". Status " << status;
        HandleError(errorMessage, issues);
    }

    void HandleError(const TString& message, const NYql::TIssues& issues) {
        CPP_LOG_E(message);
        NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, message);
        for (auto& subIssue : issues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
        }
        SendErrorMessageToSender(std::move(issue));
    }

    void SendRequestToSender() {
        TBase::SendRequestToSender(Request);
    }

protected:
    IEventBase* MakeTimeoutEventImpl(NYql::TIssue issue) final {
        return new TEventResponse({std::move(issue)}, {});
    };

protected:
    const TEventRequestPtr Request;
};

} // namespace NFq::NPrivate
