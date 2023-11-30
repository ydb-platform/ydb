#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>
#include "proxy_private.h"
#include "proxy.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/yson/node/node_io.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/core/fq/libs/common/entity_id.h>

#include <ydb/core/fq/libs/control_plane_config/control_plane_config.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/library/security/util.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivateGetTask - Owner: " << OwnerId << ", " << "Host: " << Host << ", Tenant: " << Tenant << ", " << stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivateGetTask - Owner: " << OwnerId << ", " << "Host: " << Host << ", Tenant: " << Tenant << ", " << stream)

namespace NFq {

using namespace NActors;
using namespace NMonitoring;

class TGetTaskRequestActor
    : public NActors::TActorBootstrapped<TGetTaskRequestActor>
{
public:
    TGetTaskRequestActor(
        const NActors::TActorId& sender,
        const ::NFq::TSigner::TPtr& signer,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TAutoPtr<TEvents::TEvGetTaskRequest> ev,
        TDynamicCounterPtr counters)
        : Sender(sender)
        , TimeProvider(timeProvider)
        , Ev(std::move(ev))
        , Counters(std::move(counters->GetSubgroup("subsystem", "private_api")->GetSubgroup("subcomponent", "GetTask")))
        , LifetimeDuration(Counters->GetHistogram("LifetimeDurationMs",  ExponentialHistogram(10, 2, 50)))
        , RequestedMBytes(Counters->GetHistogram("RequestedMB",  ExponentialHistogram(6, 2, 3)))
        , StartTime(TInstant::Now())
        , Signer(signer)
    {}

    static constexpr char ActorName[] = "YQ_PRIVATE_GET_TASK";

    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        LOG_E("TGetTaskRequestActor::OnUndelivered");
        auto response = MakeHolder<TEvents::TEvGetTaskResponse>();
        response->Status = Ydb::StatusIds::GENERIC_ERROR;
        response->Issues.AddIssue("UNDELIVERED");
        Send(ev->Sender, response.Release());
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
        auto response = MakeHolder<TEvents::TEvGetTaskResponse>();
        response->Status = reqStatus;
        response->Issues.AddIssues(Issues);
        Send(Sender, response.Release());
        PassAway();
    }

    void Bootstrap() {
        Become(&TGetTaskRequestActor::StateFunc);
        auto request = Ev->Record;
        OwnerId = request.owner_id();
        Host = request.host();
        Tenant = request.tenant();
        LOG_D("Request CP::GetTask with size: " << request.ByteSize() << " bytes");
        RequestedMBytes->Collect(request.ByteSize() / 1024 / 1024);
        Send(ControlPlaneConfigActorId(), new TEvControlPlaneConfig::TEvGetTenantInfoRequest());
    }

private:
    void HandleResponse(NFq::TEvControlPlaneStorage::TEvGetTaskResponse::TPtr& ev) { // YQ
        LOG_D("Got CP::GetTask Response");

        const auto& issues = ev->Get()->Issues;
        if (issues) {
            Issues.AddIssues(issues);
            Fail("ControlPlane::GetTaskError", Ydb::StatusIds::GENERIC_ERROR);
            return;
        }

        auto response = MakeHolder<TEvents::TEvGetTaskResponse>();
        response->Status = Ydb::StatusIds::SUCCESS;
        response->Record.ConstructInPlace();
        auto& record = *response->Record;
        record = ev->Get()->Record;
        try {
            for (auto& task : *record.mutable_tasks()) {
                THashMap<TString, TString> accountIdSignatures;
                for (auto& account : *task.mutable_service_accounts()) {
                    const auto serviceAccountId = account.value();
                    auto& signature = accountIdSignatures[serviceAccountId];
                    if (!signature && Signer) {
                        signature = Signer->SignAccountId(serviceAccountId);
                    }
                    account.set_signature(signature);
                }
            }
            Send(Sender, response.Release());
            PassAway();
        } catch (...) {
            const auto msg = TStringBuilder() << "Can't do GetTask: " << CurrentExceptionMessage();
            Fail(msg);
        }
    }

    void Handle(TEvControlPlaneConfig::TEvGetTenantInfoResponse::TPtr& ev) {
        auto request = Ev->Record;
        auto event = std::make_unique<NFq::TEvControlPlaneStorage::TEvGetTaskRequest>(std::move(request));
        event->TenantInfo = ev->Get()->TenantInfo;
        Send(NFq::ControlPlaneStorageServiceActorId(), event.release());
    }

private:
    STRICT_STFUNC(
        StateFunc,
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        hFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
        hFunc(NFq::TEvControlPlaneStorage::TEvGetTaskResponse, HandleResponse)
        hFunc(TEvControlPlaneConfig::TEvGetTenantInfoResponse, Handle)
    )

    const TActorId Sender;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TAutoPtr<TEvents::TEvGetTaskRequest> Ev;
    TDynamicCounterPtr Counters;
    const THistogramPtr LifetimeDuration;
    const THistogramPtr RequestedMBytes;
    const TInstant StartTime;

    ::NFq::TSigner::TPtr Signer;

    NYql::TIssues Issues;
    TString OwnerId;
    TString Host;
    TString Tenant;
};

IActor* CreateGetTaskRequestActor(
    const NActors::TActorId& sender,
    const ::NFq::TSigner::TPtr& signer,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvGetTaskRequest> ev,
    TDynamicCounterPtr counters) {
    return new TGetTaskRequestActor(
        sender,
        signer,
        timeProvider,
        std::move(ev),
        counters);
}

} /* NFq */
