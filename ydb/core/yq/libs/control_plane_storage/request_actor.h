#pragma once
#include "util.h"

#include <ydb/core/yq/libs/control_plane_storage/ydb_control_plane_storage_impl.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/control_plane_storage/schema.h>
#include <ydb/core/yq/libs/db_schema/db_schema.h>
#include <ydb/core/yq/libs/shared_resources/db_exec.h>
#include <ydb/public/api/protos/yq.pb.h>
#include <ydb/public/lib/yq/scope.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <util/datetime/base.h>

namespace NYq {

template <class TRequest, class TResponse, class TDerived>
class TControlPlaneRequestActor : public NActors::TActorBootstrapped<TDerived>,
                                  public TDbRequester
{
public:
    using TResponseEvent = TResponse;
    using TRequestEvent = TRequest;
    using TBaseActor = NActors::TActorBootstrapped<TDerived>;

    using TBaseActor::Send;

protected:
    TControlPlaneRequestActor(typename TRequestEvent::TPtr&& ev, TRequestCounters requestCounters, TDebugInfoPtr debugInfo, TDbPool::TPtr dbPool, TYdbConnectionPtr ydbConnection)
        : TDbRequester(std::move(dbPool), std::move(ydbConnection))
        , Request(std::move(ev))
        , RequestCounters(std::move(requestCounters))
        , DebugInfo(std::move(debugInfo))
    {
        RequestCounters.IncInFly();
        RequestCounters.Common->RequestBytes->Add(Request->Get()->GetByteSize());
    }

public:
    void Bootstrap() {
        CPS_LOG_T(TDerived::RequestTypeName << "Request: {" << Request->Get()->Request.DebugString() << "}");

        AsDerived()->Start();
    }

protected:
    void ReplyWithError(const TString& msg) {
        NYql::TIssues issues;
        issues.AddIssue(msg);
        ReplyWithError(issues);
    }

    void ReplyWithError(const NYql::TIssues& issues) {
        SendResponseEventAndPassAway(std::make_unique<TResponse>(issues), false);
    }

    void Reply(const typename TResponse::TProto& proto) {
        SendResponseEventAndPassAway(std::make_unique<TResponse>(proto), true);
    }

    void SendResponseEventAndPassAway(std::unique_ptr<TResponse> event, bool success) {
        event->DebugInfo = std::move(DebugInfo);

        RequestCounters.Common->ResponseBytes->Add(event->GetByteSize());
        RequestCounters.IncInFly();
        if (success) {
            RequestCounters.IncOk();
        } else {
            RequestCounters.IncError();
        }
        RequestCounters.Common->LatencyMs->Collect(GetRequestDuration().MilliSeconds());

        AsDerived()->LwProbe(success);

        Send(Request->Sender, event.release(), 0, Request->Cookie);
        this->PassAway();
    }

    TDuration GetRequestDuration() const {
        return TInstant::Now() - StartTime;
    }

    TDerived* AsDerived() {
        return static_cast<TDerived*>(this);
    }

protected:
    const TInstant StartTime = TInstant::Now();
    const typename TRequestEvent::TPtr Request;
    TRequestCounters RequestCounters;
    TDebugInfoPtr DebugInfo;
};

} // namespace NYq
