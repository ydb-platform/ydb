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

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/yson/writer.h>

#include <util/generic/ptr.h>
#include <util/generic/guid.h>
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
    using TBase = TResultActorBase<TResultAggregator>;
    static constexpr ui32 MAX_RESULT_BATCH = 2048;

public:
    static constexpr char ActorName[] = "YQL_DQ_RESULT_AGGREGATOR";

    explicit TResultAggregator(const TVector<TString>& columns, const NActors::TActorId& executerId, const TString& traceId,
        const TDqConfiguration::TPtr& settings, const TString& resultType, NActors::TActorId graphExecutionEventsId, bool discard)
        : TBase(columns, executerId, traceId, settings, resultType, graphExecutionEventsId, discard)
        , Continue(false) {
        if (Settings) {
            PullRequestTimeout = TDuration::MilliSeconds(settings->PullRequestTimeoutMs.Get().GetOrElse(0));
            PingTimeout = TDuration::MilliSeconds(settings->PingTimeoutMs.Get().GetOrElse(0));
            PingPeriod = Max(PingTimeout/4, TDuration::MilliSeconds(1000));
        }
    }

public:
    STFUNC(Handler) {
        switch (const ui32 etype = ev->GetTypeRewrite()) {
            HFunc(TEvPullDataResponse, OnPullResponse);
            cFunc(TEvents::TEvWakeup::EventType, OnWakeup)
            sFunc(TEvMessageProcessed, OnMessageProcessed)
            HFunc(TEvPullResult, OnPullResult);
            HFunc(TEvReadyState, OnReadyState);
            HFunc(TEvPingResponse, OnPingResponse);
            default:
                TBase::HandlerBase(ev);
        }
    }

    STFUNC(ShutdownHandler) {
        switch (const ui32 etype = ev->GetTypeRewrite()) {
            sFunc(TEvMessageProcessed, OnMessageProcessed);
            default:
                TBase::ShutdownHandlerBase(ev);
        }
    }

private:
    void OnMessageProcessed() {
        if (!Continue) {
            return;
        }

        Continue = false;
        Send(SelfId(), MakeHolder<TEvPullResult>());
    }

    void OnWakeup() {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
        auto now = TInstant::Now();
        if (PullRequestTimeout && now - PullRequestStartTime > PullRequestTimeout) {
            OnError(NYql::NDqProto::StatusIds::TIMEOUT, "Timeout " + ToString(SourceID.NodeId()));
        }

        if (PingTimeout && now - PingStartTime > PingTimeout) {
            OnError(NYql::NDqProto::StatusIds::TIMEOUT, "PingTimeout " + ToString(SourceID.NodeId()));
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
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
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
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        PullRequestStartTime = TInstant::Now();
        Send(SourceID, MakeHolder<TEvPullDataRequest>(MAX_RESULT_BATCH), IEventHandle::FlagTrackDelivery);
    }

    void OnPingResponse(TEvPingResponse::TPtr&, const TActorContext&) {
        PingRequested = false;
    }

    void OnPullResponse(TEvPullDataResponse::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);

        if (FinishCalled) {
            // finalization has been begun, actor will not kill himself anymore, should ignore responses instead
            return;
        }

        auto& response = ev->Get()->Record;

        AddCounters(response);

        switch (response.GetResponseType()) {
            case NYql::NDqProto::CONTINUE: {
                Continue = true;
            } break;
            case NYql::NDqProto::FINISH:
                Finish();
                return;
            case NYql::NDqProto::YIELD:
                Schedule(TDuration::MilliSeconds(10), new TEvPullResult());
                return;
            case NYql::NDqProto::ERROR: {
                OnError(NYql::NDqProto::StatusIds::UNSUPPORTED, ev->Get()->Record.GetErrorMessage());
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

        TDqSerializedBatch batch;
        batch.Proto = std::move(*response.MutableData());
        if (batch.Proto.HasPayloadId()) {
            batch.Payload = ev->Get()->GetPayload(batch.Proto.GetPayloadId());
        }

        // guid here is redundant and serves only for logic validation
        OnReceiveData(std::move(batch), TGUID::Create().AsGuidString());
    }

private:
    NActors::TActorId SourceID;
    TDuration PullRequestTimeout;
    TDuration PingTimeout;
    TDuration PingPeriod;
    TInstant PingStartTime;
    TInstant PullRequestStartTime;
    bool PingRequested = false;
    NActors::TSchedulerCookieHolder TimerCookieHolder;
    bool Continue;
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
