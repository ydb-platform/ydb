#pragma once

#include "counters.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_proxy/control_plane_proxy.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NFq {
namespace NPrivate {

using namespace NActors;
using namespace NThreading;
using namespace NYdb;

template<typename T>
struct TBaseActorTypeTag {
    using TRequest  = typename T::TRequest;
    using TResponse = typename T::TResponse;
};

template<typename TDerived>
class TBaseActor : public NActors::TActorBootstrapped<TDerived> {
public:
    using TBase = NActors::TActorBootstrapped<TDerived>;
    using TBase::Become;
    using TBase::PassAway;
    using TBase::SelfId;
    using TBase::Send;

    using TEventRequestPtr = typename TBaseActorTypeTag<TDerived>::TRequest::TPtr;
    using TEventResponse   = typename TBaseActorTypeTag<TDerived>::TResponse;

public:
    TBaseActor(const TActorId& sender,
               const TEventRequestPtr request,
               TDuration requestTimeout,
               const NPrivate::TRequestCommonCountersPtr& counters)
        : Request(std::move(request))
        , Counters(counters)
        , Sender(sender)
        , RequestTimeout(requestTimeout) { }

    void Bootstrap() {
        CPP_LOG_T("TBaseActor Bootstrap started. Actor id: " << SelfId());
        Become(&TDerived::StateFunc, RequestTimeout, new NActors::TEvents::TEvWakeup());
        Counters->InFly->Inc();
        BootstrapImpl();
    }

    void SendErrorMessageToSender(const NYql::TIssue& issue) {
        Counters->Error->Inc();
        NYql::TIssues issues;
        issues.AddIssue(issue);
        Send(Sender, new TEventResponse(issues, {}), 0, Request->Cookie);
        PassAway();
    }

    void SendRequestToSender() {
        Counters->Ok->Inc();
        Send(Request->Forward(ControlPlaneProxyActorId()));
        PassAway();
    }

    void HandleTimeout() {
        CPP_LOG_D("TBaseActor Timeout occurred. Actor id: "
                  << SelfId());
        Counters->Timeout->Inc();
        SendErrorMessageToSender(MakeErrorIssue(
            TIssuesIds::TIMEOUT,
            "Timeout occurred. Try repeating the request later"));
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

private:
    virtual void BootstrapImpl() = 0;

protected:
    const TEventRequestPtr Request;
    const NPrivate::TRequestCommonCountersPtr Counters;
    const TActorId Sender;
    const TDuration RequestTimeout;
};

} // namespace NPrivate
} // namespace NFq
