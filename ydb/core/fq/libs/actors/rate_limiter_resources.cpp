#include "proxy_private.h"
#include <util/datetime/base.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/yson/node/node_io.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <google/protobuf/util/time_util.h>

#define C_LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivateCreateRateLimiterResource - QueryId: " << OperationId  << ", Owner: " << OwnerId  << ", " << stream)
#define C_LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivateCreateRateLimiterResource - QueryId: " << OperationId  << ", Owner: " << OwnerId  << ", "<< stream)

#define D_LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivateDeleteRateLimiterResource - QueryId: " << OperationId  << ", Owner: " << OwnerId  << ", " << stream)
#define D_LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivateDeleteRateLimiterResource - QueryId: " << OperationId  << ", Owner: " << OwnerId  << ", "<< stream)

namespace NFq {

using namespace NActors;
using namespace NMonitoring;

namespace {

class TCreateRateLimiterResourceRequestActor
    : public NActors::TActorBootstrapped<TCreateRateLimiterResourceRequestActor>
{
public:
    TCreateRateLimiterResourceRequestActor(
        const NActors::TActorId& sender,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TAutoPtr<TEvents::TEvCreateRateLimiterResourceRequest> ev,
        TDynamicCounterPtr counters)
        : Sender(sender)
        , TimeProvider(timeProvider)
        , Ev(std::move(ev))
        , Counters(std::move(counters->GetSubgroup("subsystem", "private_api")->GetSubgroup("subcomponent", "CreateRateLimiterResource")))
        , LifetimeDuration(Counters->GetHistogram("LifetimeDurationMs",  ExponentialHistogram(10, 2, 50)))
        , RequestedMBytes(Counters->GetHistogram("RequestedMB",  ExponentialHistogram(6, 2, 3)))
        , StartTime(TInstant::Now())
    {}

    static constexpr char ActorName[] = "YQ_PRIVATE_CREATE_RATE_LIMITER_RESOURCE";

    void PassAway() final {
        LifetimeDuration->Collect((TInstant::Now() - StartTime).MilliSeconds());
        NActors::IActor::PassAway();
    }

    void Fail(const TString& message, Ydb::StatusIds::StatusCode reqStatus = Ydb::StatusIds::INTERNAL_ERROR) {
        Issues.AddIssue(message);
        const auto codeStr = Ydb::StatusIds_StatusCode_Name(reqStatus);
        C_LOG_E(TStringBuilder()
            << "Failed with code: " << codeStr
            << " Details: " << Issues.ToString());
        auto res = MakeHolder<TEvents::TEvCreateRateLimiterResourceResponse>();
        res->Status = reqStatus;
        res->Issues.AddIssues(Issues);
        Send(Sender, res.Release());
        PassAway();
    }

    void Bootstrap() {
        Become(&TCreateRateLimiterResourceRequestActor::StateFunc);
        const auto& req = Ev->Record;
        OperationId = req.query_id().value();
        OwnerId = req.owner_id();
        C_LOG_D("Request CP::CreateRateLimiterResource with size: " << req.ByteSize() << " bytes");
        RequestedMBytes->Collect(req.ByteSize() / 1024 / 1024);
        try {
            auto event = CreateControlPlaneEvent();
            Send(NFq::ControlPlaneStorageServiceActorId(), event.release());
        } catch (const std::exception& err) {
            const auto msg = TStringBuilder() << "CreateRateLimiterResource Boostrap Error: " << CurrentExceptionMessage();
            Fail(msg);
        }
    }

private:
    STRICT_STFUNC(
        StateFunc,
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        hFunc(NFq::TEvControlPlaneStorage::TEvCreateRateLimiterResourceResponse, HandleResponse)
    )

    std::unique_ptr<NFq::TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest> CreateControlPlaneEvent() {
        auto request = Ev->Record;
        return std::make_unique<NFq::TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest>(std::move(request));
    }

    void HandleResponse(NFq::TEvControlPlaneStorage::TEvCreateRateLimiterResourceResponse::TPtr& ev) {
        C_LOG_D("Got CP::CreateRateLimiterResourceResponse");
        const auto& issues = ev->Get()->Issues;
        if (issues) {
            Issues.AddIssues(issues);
            Fail("ControlPlane CreateRateLimiterResourceError", Ydb::StatusIds::GENERIC_ERROR);
            return;
        }

        auto response = MakeHolder<TEvents::TEvCreateRateLimiterResourceResponse>();
        response->Status = Ydb::StatusIds::SUCCESS;
        response->Record.ConstructInPlace(ev->Get()->Record);
        Send(Sender, response.Release());
        PassAway();
    }

private:
    const TActorId Sender;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TAutoPtr<TEvents::TEvCreateRateLimiterResourceRequest> Ev;
    TDynamicCounterPtr Counters;
    const THistogramPtr LifetimeDuration;
    const THistogramPtr RequestedMBytes;
    const TInstant StartTime;

    TString OperationId;
    TString OwnerId;
    TString CloudId;
    NYql::TIssues Issues;
};

class TDeleteRateLimiterResourceRequestActor
    : public NActors::TActorBootstrapped<TDeleteRateLimiterResourceRequestActor>
{
public:
    TDeleteRateLimiterResourceRequestActor(
        const NActors::TActorId& sender,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TAutoPtr<TEvents::TEvDeleteRateLimiterResourceRequest> ev,
        TDynamicCounterPtr counters)
        : Sender(sender)
        , TimeProvider(timeProvider)
        , Ev(std::move(ev))
        , Counters(std::move(counters->GetSubgroup("subsystem", "private_api")->GetSubgroup("subcomponent", "DeleteRateLimiterResource")))
        , LifetimeDuration(Counters->GetHistogram("LifetimeDurationMs",  ExponentialHistogram(10, 2, 50)))
        , RequestedMBytes(Counters->GetHistogram("RequestedMB",  ExponentialHistogram(6, 2, 3)))
        , StartTime(TInstant::Now())
    {}

    static constexpr char ActorName[] = "YQ_PRIVATE_CREATE_RATE_LIMITER_RESOURCE";

    void PassAway() final {
        LifetimeDuration->Collect((TInstant::Now() - StartTime).MilliSeconds());
        NActors::IActor::PassAway();
    }

    void Fail(const TString& message, Ydb::StatusIds::StatusCode reqStatus = Ydb::StatusIds::INTERNAL_ERROR) {
        Issues.AddIssue(message);
        const auto codeStr = Ydb::StatusIds_StatusCode_Name(reqStatus);
        D_LOG_E(TStringBuilder()
            << "Failed with code: " << codeStr
            << " Details: " << Issues.ToString());
        auto res = MakeHolder<TEvents::TEvDeleteRateLimiterResourceResponse>();
        res->Status = reqStatus;
        res->Issues.AddIssues(Issues);
        Send(Sender, res.Release());
        PassAway();
    }

    void Bootstrap() {
        Become(&TDeleteRateLimiterResourceRequestActor::StateFunc);
        const auto& req = Ev->Record;
        OperationId = req.query_id().value();
        OwnerId = req.owner_id();
        D_LOG_D("Request CP::DeleteRateLimiterResource with size: " << req.ByteSize() << " bytes");
        RequestedMBytes->Collect(req.ByteSize() / 1024 / 1024);
        try {
            auto event = CreateControlPlaneEvent();
            Send(NFq::ControlPlaneStorageServiceActorId(), event.release());
        } catch (const std::exception& err) {
            const auto msg = TStringBuilder() << "DeleteRateLimiterResource Boostrap Error: " << CurrentExceptionMessage();
            Fail(msg);
        }
    }

private:
    STRICT_STFUNC(
        StateFunc,
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        hFunc(NFq::TEvControlPlaneStorage::TEvDeleteRateLimiterResourceResponse, HandleResponse)
    )

    std::unique_ptr<NFq::TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest> CreateControlPlaneEvent() {
        auto request = Ev->Record;
        return std::make_unique<NFq::TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest>(std::move(request));
    }

    void HandleResponse(NFq::TEvControlPlaneStorage::TEvDeleteRateLimiterResourceResponse::TPtr& ev) {
        D_LOG_D("Got CP::DeleteRateLimiterResourceResponse");
        const auto& issues = ev->Get()->Issues;
        if (issues) {
            Issues.AddIssues(issues);
            Fail("ControlPlane DeleteRateLimiterResourceError", Ydb::StatusIds::GENERIC_ERROR);
            return;
        }

        auto response = MakeHolder<TEvents::TEvDeleteRateLimiterResourceResponse>();
        response->Status = Ydb::StatusIds::SUCCESS;
        response->Record.ConstructInPlace(ev->Get()->Record);
        Send(Sender, response.Release());
        PassAway();
    }

private:
    const TActorId Sender;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TAutoPtr<TEvents::TEvDeleteRateLimiterResourceRequest> Ev;
    TDynamicCounterPtr Counters;
    const THistogramPtr LifetimeDuration;
    const THistogramPtr RequestedMBytes;
    const TInstant StartTime;

    TString OperationId;
    TString OwnerId;
    TString CloudId;
    NYql::TIssues Issues;
};

} // namespace

IActor* CreateCreateRateLimiterResourceRequestActor(
    const NActors::TActorId& sender,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvCreateRateLimiterResourceRequest> ev,
    TDynamicCounterPtr counters) {
    return new TCreateRateLimiterResourceRequestActor(
        sender,
        timeProvider,
        std::move(ev),
        std::move(counters));
}

IActor* CreateDeleteRateLimiterResourceRequestActor(
    const NActors::TActorId& sender,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvDeleteRateLimiterResourceRequest> ev,
    TDynamicCounterPtr counters) {
    return new TDeleteRateLimiterResourceRequestActor(
        sender,
        timeProvider,
        std::move(ev),
        std::move(counters));
}

} /* NFq */
