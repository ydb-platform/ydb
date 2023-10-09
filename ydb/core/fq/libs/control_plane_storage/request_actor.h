#pragma once
#include "util.h"

#include <ydb/core/fq/libs/control_plane_storage/ydb_control_plane_storage_impl.h>
#include <ydb/core/fq/libs/control_plane_storage/events/internal_events.h>
#include <ydb/core/fq/libs/control_plane_storage/schema.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>
#include <ydb/core/fq/libs/shared_resources/db_exec.h>
#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <util/datetime/base.h>
#include <util/generic/typetraits.h>

namespace NFq {

Y_HAS_MEMBER(User);
Y_HAS_MEMBER(Token);

template <class TRequest, class TResponse, class TDerived>
class TControlPlaneRequestActor : public NActors::TActorBootstrapped<TDerived>,
                                  public TDbRequester,
                                  public TControlPlaneStorageUtils
{
public:
    using TResponseEvent = TResponse;
    using TRequestEvent = TRequest;
    using TBaseActor = NActors::TActorBootstrapped<TDerived>;

    using TBaseActor::Send;

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_STORAGE_REQUEST";

protected:
    TControlPlaneRequestActor(typename TRequestEvent::TPtr&& ev, TRequestCounters requestCounters, TDebugInfoPtr debugInfo, TDbPool::TPtr dbPool, TYdbConnectionPtr ydbConnection, const std::shared_ptr<::NFq::TControlPlaneStorageConfig>& config)
        : TDbRequester(std::move(dbPool), std::move(ydbConnection))
        , TControlPlaneStorageUtils(config)
        , Request(std::move(ev))
        , RequestCounters(std::move(requestCounters))
        , DebugInfo(std::move(debugInfo))
    {
        RequestCounters.IncInFly();
        RequestCounters.Common->RequestBytes->Add(Request->Get()->GetByteSize());
    }

public:
    const TString RequestLogString() const {
        TStringBuilder result;
        result << TDerived::RequestTypeName << "Request: {" << Request->Get()->Request.DebugString() << "} ";
        if constexpr (THasUser<TRequestEvent>::value && THasToken<TRequestEvent>::value) {
            result << MakeUserInfo(Request->Get()->User, Request->Get()->Token);
        }
        return std::move(result);
    }

    void Bootstrap() {
        CPS_LOG_T(RequestLogString());

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
        RequestCounters.DecInFly();
        if (success) {
            RequestCounters.IncOk();
        } else {
            RequestCounters.IncError();
        }
        RequestCounters.Common->LatencyMs->Collect(GetRequestDuration().MilliSeconds());

        AsDerived()->LwProbe(success);

        for (const auto& issue : event->Issues) {
            NYql::WalkThroughIssues(issue, true, [this](const NYql::TIssue& err, ui16 level) {
                Y_UNUSED(level);
                RequestCounters.Common->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
            });
        }

        Send(Request->Sender, event.release(), 0, Request->Cookie);

        this->PassAway();
    }

    bool SendResponse(const TString& name,
        const NYdb::TAsyncStatus& statusWithDbAnswer,
        const std::function<typename TPrepareResponseResultType<TResponseEvent, typename TResponseEvent::TProto>::Type()>& prepare)
    {
        Y_ABORT_UNLESS(statusWithDbAnswer.HasValue() || statusWithDbAnswer.HasException()); // ready
        NYql::TIssues internalIssues;
        NYql::TIssues issues;
        typename TResponseEvent::TProto result;
        typename TPrepareResponseResultType<TResponseEvent, typename TResponseEvent::TProto>::TResponseAuditDetails auditDetails; // void* for nonauditable events

        try {
            TStatus status = statusWithDbAnswer.GetValue();
            if (status.IsSuccess()) {
                if constexpr (TResponseEvent::Auditable) {
                    auto p = prepare();
                    result = std::move(p.first);
                    auditDetails = std::move(p.second);
                } else {
                    result = prepare();
                }
            } else {
                issues.AddIssues(status.GetIssues());
                internalIssues.AddIssues(status.GetIssues());
            }
        } catch (const TCodeLineException& exception) {
            NYql::TIssue issue = MakeErrorIssue(exception.Code, exception.GetRawMessage());
            issues.AddIssue(issue);
            NYql::TIssue internalIssue = MakeErrorIssue(exception.Code, CurrentExceptionMessage());
            internalIssues.AddIssue(internalIssue);
        } catch (const std::exception& exception) {
            NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, exception.what());
            issues.AddIssue(issue);
            NYql::TIssue internalIssue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
            internalIssues.AddIssue(internalIssue);
        } catch (...) {
            NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
            issues.AddIssue(issue);
            NYql::TIssue internalIssue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
            internalIssues.AddIssue(internalIssue);
        }

        std::unique_ptr<TResponseEvent> event;
        if (issues) {
            CPS_LOG_W(name << ": {" << TrimForLogs(Request->Get()->Request.DebugString()) << "} ERROR: " << internalIssues.ToOneLineString());
            event = std::make_unique<TResponseEvent>(issues);
        } else {
            CPS_LOG_W(name << ": {" << TrimForLogs(Request->Get()->Request.DebugString()) << "} SUCCESS");
            if constexpr (TResponseEvent::Auditable) {
                event = std::make_unique<TResponseEvent>(result, auditDetails);
            } else {
                event = std::make_unique<TResponseEvent>(result);
            }
        }
        SendResponseEventAndPassAway(std::move(event), !issues);
        return !issues;
    }

    TDuration GetRequestDuration() const {
        return TInstant::Now() - StartTime;
    }

    TDerived* AsDerived() {
        return static_cast<TDerived*>(this);
    }

    void Subscribe(NYdb::TAsyncStatus& status, std::shared_ptr<TVector<NYdb::TResultSet>> resultSets = nullptr) {
        status.Subscribe(
            [actorSystem = NActors::TActivationContext::ActorSystem(), selfId = this->SelfId(), resultSets] (const NYdb::TAsyncStatus& status) mutable {
                actorSystem->Send(new IEventHandle(selfId, selfId, new TEvControlPlaneStorageInternal::TEvDbRequestResult(status, std::move(resultSets))));
            }
        );
    }

protected:
    const TInstant StartTime = TInstant::Now();
    const typename TRequestEvent::TPtr Request;
    TRequestCounters RequestCounters;
    TDebugInfoPtr DebugInfo;
};

} // namespace NFq
