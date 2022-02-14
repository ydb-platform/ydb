#include "result_aggregator.h"
#include "result_receiver.h"

#include <ydb/library/yql/providers/dq/actors/events.h>
#include <ydb/library/yql/providers/dq/actors/executer_actor.h>
#include <ydb/library/yql/providers/dq/actors/result_actor_base.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/sql/sql.h>

#include <ydb/library/yql/utils/failure_injector/failure_injector.h>
#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/executor_pool_basic.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/scheduler_basic.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/yson/writer.h>

#include <util/generic/ptr.h>
#include <util/string/split.h>
#include <util/system/types.h>
#include <util/stream/str.h>


namespace NYql::NDqs::NExecutionHelpers {

using namespace NYql;
using namespace NYql::NDqProto;
using namespace NYql::NNodes;

using namespace NKikimr::NMiniKQL;

using namespace Yql::DqsProto;

using namespace NYql::NDq;
using namespace NYql::NDqs;
using namespace NYql::NDqProto;
using namespace NActors;

namespace {

class TResultAggregator: public TResultActorBase<TResultAggregator> {
    static constexpr ui32 MAX_RESULT_BATCH = 2048;

public:
    static constexpr char ActorName[] = "YQL_DQ_RESULT_AGGREGATOR";

    explicit TResultAggregator(const TVector<TString>& columns, const NActors::TActorId& executerId, const TString& traceId,
        const TDqConfiguration::TPtr& settings, const TString& resultType, NActors::TActorId graphExecutionEventsId, bool discard)
        : TResultActorBase<TResultAggregator>(columns, executerId, traceId, settings, resultType, graphExecutionEventsId, discard) {
        if (Settings) {
            PullRequestTimeout = TDuration::MilliSeconds(settings->PullRequestTimeoutMs.Get().GetOrElse(0));
            PingTimeout = TDuration::MilliSeconds(settings->PingTimeoutMs.Get().GetOrElse(0));
            PingPeriod = Max(PingTimeout/4, TDuration::MilliSeconds(1000));
        }
    }

public:
#define HANDLER_STUB(TEvType)                                           \
    cFunc(TEvType::EventType, [this]() {                        \
        YQL_LOG_CTX_SCOPE(TraceId);                             \
        YQL_LOG(DEBUG) << "Unexpected event " << ( #TEvType );  \
    })

    STRICT_STFUNC(Handler, {
        HFunc(TEvPullResult, OnPullResult);
        HFunc(TEvReadyState, OnReadyState);
        HFunc(TEvPullDataResponse, OnPullResponse);
        HFunc(TEvPingResponse, OnPingResponse);
        HFunc(TEvQueryResponse, OnQueryResult);
        HFunc(TEvDqFailure, OnFullResultWriterResponse);
        cFunc(TEvents::TEvPoison::EventType, PassAway)
        hFunc(TEvents::TEvUndelivered, OnUndelivered)
        cFunc(TEvents::TEvWakeup::EventType, OnWakeup)
        cFunc(TEvents::TEvGone::EventType, OnFullResultWriterShutdown)
    })

private:
    void OnWakeup() {
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(DEBUG) << __FUNCTION__;
        auto now = TInstant::Now();
        if (PullRequestTimeout && now - PullRequestStartTime > PullRequestTimeout) {
            OnError("Timeout " + ToString(SourceID.NodeId()), true, true);
        }

        if (PingTimeout && now - PingStartTime > PingTimeout) {
            OnError("PingTimeout " + ToString(SourceID.NodeId()), true, true);
        }

        if (!PingRequested) {
            PingRequested = true;
            PingStartTime = now;
            Send(SourceID, MakeHolder<TEvPingRequest>(), IEventHandle::FlagTrackDelivery);
        }

        TimerCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
        Schedule(PingPeriod, new TEvents::TEvWakeup(), TimerCookieHolder.Get());
    }

    void OnReadyState(TEvReadyState::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_SCOPE(TraceId);
        AddCounters(ev->Get()->Record);

        SourceID = NActors::ActorIdFromProto(ev->Get()->Record.GetSourceId());
        Send(SelfId(), MakeHolder<TEvPullResult>());

        PingStartTime = PullRequestStartTime = TInstant::Now();
        TimerCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
        Schedule(PingPeriod, new TEvents::TEvWakeup(), TimerCookieHolder.Get());

        AddCriticalEventType(TEvents::TEvWakeup::EventType);
        AddCriticalEventType(TEvPingResponse::EventType);
    }

    void OnPullResult(TEvPullResult::TPtr&, const TActorContext&) {
        YQL_LOG_CTX_SCOPE(TraceId);
        PullRequestStartTime = TInstant::Now();
        Send(SourceID, MakeHolder<TEvPullDataRequest>(MAX_RESULT_BATCH), IEventHandle::FlagTrackDelivery);
    }

    void OnPingResponse(TEvPingResponse::TPtr&, const TActorContext&) {
        PingRequested = false;
    }

    void OnPullResponse(TEvPullDataResponse::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(DEBUG) << __FUNCTION__;

        if (FinishCalled) {
            // finalization has been begun, actor will not kill himself anymore, should ignore responses instead
            return;
        }

        auto& response = ev->Get()->Record;

        AddCounters(response);

        switch (response.GetResponseType()) {
            case NYql::NDqProto::CONTINUE: {
                Send(SelfId(), MakeHolder<TEvPullResult>());
            } break;
            case NYql::NDqProto::FINISH:
                Finish();
                return;
            case NYql::NDqProto::YIELD:
                Schedule(TDuration::MilliSeconds(10), new TEvPullResult());
                return;
            case NYql::NDqProto::ERROR: {
                OnError(ev->Get()->Record.GetErrorMessage(), false, true);
                break;
            }
            case NYql::NDqProto::UNKNOWN:
                [[fallthrough]];
            case NYql::NDqProto::EPullResponseType_INT_MIN_SENTINEL_DO_NOT_USE_:
                [[fallthrough]];
            case NYql::NDqProto::EPullResponseType_INT_MAX_SENTINEL_DO_NOT_USE_:
                YQL_ENSURE(false, "Unknown pull result");
                break;
        }

        OnReceiveData(std::move(*response.MutableData()));
    }

    NActors::TActorId SourceID;
    TDuration PullRequestTimeout;
    TDuration PingTimeout;
    TDuration PingPeriod;
    TInstant PingStartTime;
    TInstant PullRequestStartTime;
    bool PingRequested = false;
    NActors::TSchedulerCookieHolder TimerCookieHolder;
};

class TResultPrinter: public TActor<TResultPrinter> {
public:
    static constexpr char ActorName[] = "YQL_DQ_RESULT_PRINTER";

    TResultPrinter(IOutputStream& output, NThreading::TPromise<void>& promise)
        : TActor<TResultPrinter>(&TResultPrinter::Handler)
        , Output(output)
        , Promise(promise)
    {
    }

private:
    STRICT_STFUNC(Handler, { HFunc(TEvQueryResponse, OnQueryResult); })

    void OnQueryResult(TEvQueryResponse::TPtr& ev, const TActorContext&) {
        if (!ev->Get()->Record.HasResultSet()&&ev->Get()->Record.GetYson().empty()) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
            Cerr << issues.ToString() << Endl;
        } else {
            auto ysonString = !ev->Get()->Record.GetYson().empty()
                ? ev->Get()->Record.GetYson()
                : NYdb::FormatResultSetYson(ev->Get()->Record.GetResultSet(), NYson::EYsonFormat::Binary);
            auto ysonNode = NYT::NodeFromYsonString(ysonString, NYson::EYsonType::Node);
            YQL_ENSURE(ysonNode.GetType() == NYT::TNode::EType::List);
            for (const auto& row : ysonNode.AsList()) {
                Output << NYT::NodeToYsonString(row) << "\n";
            }
        }

        Promise.SetValue();
        PassAway();
    }

private:
    IOutputStream& Output;
    NThreading::TPromise<void>& Promise;
};

} // unnamed

THolder<NActors::IActor> MakeResultAggregator(
    const TVector<TString>& columns,
    const NActors::TActorId& executerId,
    const TString& traceId,
    const THashMap<TString, TString>& secureParams,
    const TDqConfiguration::TPtr& settings,
    const TString& resultType,
    bool discard,
    const NActors::TActorId& graphExecutionEventsId)
{
    THolder<IActor> result;
    if (!settings->EnableComputeActor.Get().GetOrElse(false)) {
        // worker actor pull
        result = MakeHolder<TResultAggregator>(columns, executerId, traceId, settings, resultType, graphExecutionEventsId, discard);
    } else {
        // compute actor push
        result = NYql::MakeResultReceiver(columns, executerId, traceId, settings, secureParams, resultType, graphExecutionEventsId, discard);
    }
    return MakeHolder<TLogWrapReceive>(result.Release(), traceId);
}

} // NYql::NDqs::NExecutionHelpers
