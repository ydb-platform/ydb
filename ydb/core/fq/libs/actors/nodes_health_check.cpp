#include "proxy_private.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/yson/node/node_io.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <google/protobuf/util/time_util.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_NODES_MANAGER, stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_NODES_MANAGER, stream)

namespace NFq {

using namespace NActors;
using namespace NYql;
using namespace NMonitoring;

class TNodesHealthCheckActor
    : public NActors::TActorBootstrapped<TNodesHealthCheckActor>
{
public:
    TNodesHealthCheckActor(
        const NActors::TActorId& sender,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TAutoPtr<TEvents::TEvNodesHealthCheckRequest> ev,
        TDynamicCounterPtr counters)
        : Sender(sender)
        , TimeProvider(timeProvider)
        , Ev(std::move(ev))
        , Counters(std::move(counters))
        , LifetimeDuration(Counters->GetSubgroup("subsystem", "private_api")->GetHistogram("NodesHealthCheckTask",  ExponentialHistogram(10, 2, 50)))
        , StartTime(TInstant::Now())
    {}

    static constexpr char ActorName[] = "YQ_PRIVATE_NODES_HEALTH_CHECK";

    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        LOG_E("TNodesHealthCheckActor::OnUndelivered");
        auto res = MakeHolder<TEvents::TEvNodesHealthCheckResponse>();
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
        auto res = MakeHolder<TEvents::TEvNodesHealthCheckResponse>();
        res->Status = reqStatus;
        res->Issues.AddIssues(Issues);
        Send(Sender, res.Release());
        PassAway();
    }

    void Bootstrap() {
        Become(&TNodesHealthCheckActor::StateFunc);
        auto& req = Ev->Record;
        Tenant = req.tenant();

        Send(NFq::ControlPlaneStorageServiceActorId(),
            new NFq::TEvControlPlaneStorage::TEvNodesHealthCheckRequest(std::move(req)));
    }

private:
    STRICT_STFUNC(
        StateFunc,
        CFunc(NActors::TEvents::TEvPoison::EventType, Die)
        hFunc(NFq::TEvControlPlaneStorage::TEvNodesHealthCheckResponse, HandleResponse)
        hFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
    )

    void HandleResponse(NFq::TEvControlPlaneStorage::TEvNodesHealthCheckResponse::TPtr& ev) {
        auto res = MakeHolder<TEvents::TEvNodesHealthCheckResponse>();
        try {
            const auto& issues = ev->Get()->Issues;
            if (issues) {
                ythrow yexception() << issues.ToString();
            }
            res->Record.ConstructInPlace();
            res->Status = Ydb::StatusIds::SUCCESS;
            res->Record = ev->Get()->Record;
            Send(Sender, res.Release());
            PassAway();
        } catch (...) {
            const auto msg = TStringBuilder() << "Can't do NodesHealthCheck: " << CurrentExceptionMessage();
            Fail(msg);
        }
    }

private:
    const TActorId Sender;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TAutoPtr<TEvents::TEvNodesHealthCheckRequest> Ev;
    TDynamicCounterPtr Counters;
    const THistogramPtr LifetimeDuration;
    const TInstant StartTime;

    NYql::TIssues Issues;
    TString Tenant;
};

IActor* CreateNodesHealthCheckActor(
    const NActors::TActorId& sender,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvNodesHealthCheckRequest> ev,
    TDynamicCounterPtr counters) {
    return new TNodesHealthCheckActor(
        sender,
        timeProvider,
        std::move(ev),
        std::move(counters));
}

} /* NFq */
