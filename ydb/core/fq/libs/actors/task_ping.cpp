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

#include <ydb/core/fq/libs/control_plane_config/control_plane_config.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>

#include <google/protobuf/util/time_util.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivatePingTask - QueryId: " << OperationId  << ", Owner: " << OwnerId  << ", " << stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivatePingTask - QueryId: " << OperationId  << ", Owner: " << OwnerId  << ", "<< stream)

namespace NFq {

using namespace NActors;
using namespace NMonitoring;

class TTaskPingRequestActor
    : public NActors::TActorBootstrapped<TTaskPingRequestActor>
{
public:
    TTaskPingRequestActor(
        const NActors::TActorId& sender,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TAutoPtr<TEvents::TEvPingTaskRequest> ev,
        TDynamicCounterPtr counters)
        : Sender(sender)
        , TimeProvider(timeProvider)
        , Ev(std::move(ev))
        , Counters(std::move(counters->GetSubgroup("subsystem", "private_api")->GetSubgroup("subcomponent", "PingTask")))
        , LifetimeDuration(Counters->GetHistogram("LifetimeDurationMs",  ExponentialHistogram(10, 2, 50)))
        , RequestedMBytes(Counters->GetHistogram("RequestedMB",  ExponentialHistogram(6, 2, 3)))
        , StartTime(TInstant::Now())
    {}

    static constexpr char ActorName[] = "YQ_PRIVATE_PING_TASK";

    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        LOG_E("TTaskPingRequestActor::OnUndelivered");
        auto res = MakeHolder<TEvents::TEvPingTaskResponse>();
        res->Status = Ydb::StatusIds::GENERIC_ERROR;
        res->Issues.AddIssue("UNDELIVERED");
        Send(ev->Sender, res.Release());
        PassAway();
    }

    void PassAway() final {
        LifetimeDuration->Collect((TInstant::Now() - StartTime).MilliSeconds());
        NActors::IActor::PassAway();
    }

    void Fail(const TString& message, Ydb::StatusIds::StatusCode reqStatus = Ydb::StatusIds::INTERNAL_ERROR) {
        Issues.AddIssue(message);
        const auto codeStr = Ydb::StatusIds_StatusCode_Name(reqStatus);
        LOG_E(TStringBuilder()
            << "Failed with code: " << codeStr
            << " Details: " << Issues.ToString());
        auto res = MakeHolder<TEvents::TEvPingTaskResponse>();
        res->Status = reqStatus;
        res->Issues.AddIssues(Issues);
        Send(Sender, res.Release());
        PassAway();
    }

    void Bootstrap() {
        Become(&TTaskPingRequestActor::StateFunc);
        const auto& req = Ev->Record;
        OperationId = req.query_id().value();
        OwnerId = req.owner_id();
        TenantName = req.tenant();
        Scope = req.scope();
        Deadline = NProtoInterop::CastFromProto(req.deadline());
        LOG_D("Request CP::PingTask with size: " << req.ByteSize() << " bytes");
        RequestedMBytes->Collect(req.ByteSize() / 1024 / 1024);
        Send(ControlPlaneConfigActorId(), new TEvControlPlaneConfig::TEvGetTenantInfoRequest());
    }

private:
    STRICT_STFUNC(
        StateFunc,
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        hFunc(NFq::TEvControlPlaneStorage::TEvPingTaskResponse, HandleResponse)
        hFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
        hFunc(TEvControlPlaneConfig::TEvGetTenantInfoResponse, Handle);
    )

    std::unique_ptr<NFq::TEvControlPlaneStorage::TEvPingTaskRequest> CreateControlPlaneEvent() {
        auto request = Ev->Record;
        return std::make_unique<NFq::TEvControlPlaneStorage::TEvPingTaskRequest>(std::move(request));
    }

    void HandleResponse(NFq::TEvControlPlaneStorage::TEvPingTaskResponse::TPtr& ev) {
        LOG_D("Got CP::PingTaskResponse");
        const auto& issues = ev->Get()->Issues;
        if (issues) {
            Issues.AddIssues(issues);
            Fail("ControlPlane PingTaskError", Ydb::StatusIds::GENERIC_ERROR);
            return;
        }

        auto response = MakeHolder<TEvents::TEvPingTaskResponse>();
        response->Status = Ydb::StatusIds::SUCCESS;
        response->Record.ConstructInPlace(ev->Get()->Record);
        Send(Sender, response.Release());
        PassAway();
    }

    void Handle(TEvControlPlaneConfig::TEvGetTenantInfoResponse::TPtr& ev) {
        try {
            auto event = CreateControlPlaneEvent();
            event->TenantInfo = ev->Get()->TenantInfo;
            Send(NFq::ControlPlaneStorageServiceActorId(), event.release());
        } catch (const std::exception& err) {
            const auto msg = TStringBuilder() << "PingTask Boostrap Error: " << CurrentExceptionMessage();
            Fail(msg);
        }
    }

private:
    const TActorId Sender;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TAutoPtr<TEvents::TEvPingTaskRequest> Ev;
    TDynamicCounterPtr Counters;
    const THistogramPtr LifetimeDuration;
    const THistogramPtr RequestedMBytes;
    const TInstant StartTime;

    TString OperationId;
    TString OwnerId;
    TString TenantName;
    TString CloudId;
    TString Scope;
    TInstant Deadline;
    NYql::TIssues Issues;
};

IActor* CreatePingTaskRequestActor(
    const NActors::TActorId& sender,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvPingTaskRequest> ev,
    TDynamicCounterPtr counters) {
    return new TTaskPingRequestActor(
        sender,
        timeProvider,
        std::move(ev),
        std::move(counters));
}

} /* NFq */
