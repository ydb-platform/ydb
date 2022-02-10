#include "result_aggregator.h"
#include "result_receiver.h"
#include "proto_builder.h"
#include "full_result_writer.h"

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/events.h>
#include <ydb/library/yql/providers/dq/actors/executer_actor.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>

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
#include <util/stream/holder.h>
#include <util/stream/str.h>
#include <util/stream/length.h>


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

class TResultAggregator: public TSynchronizableRichActor<TResultAggregator>, NYql::TCounters {
    static constexpr ui32 MAX_RESULT_BATCH = 2048;

public:
    static constexpr char ActorName[] = "YQL_DQ_RESULT_AGGREGATOR";

    explicit TResultAggregator(const TVector<TString>& columns, const NActors::TActorId& executerId, const TString& traceId,
        const TDqConfiguration::TPtr& settings, const TString& resultType, NActors::TActorId graphExecutionEventsId, bool discard)
        : TSynchronizableRichActor<TResultAggregator>(&TResultAggregator::Handler)
        , ExecuterID(executerId)
        , GraphExecutionEventsId(graphExecutionEventsId)
        , Discard(discard)
        , TraceId(traceId)
        , Settings(settings)
        , ResultBuilder(MakeHolder<TProtoBuilder>(resultType, columns))
        , ResultYsonOut(new THoldingStream<TCountingOutput>(MakeHolder<TStringOutput>(ResultYson)))
        , ResultYsonWriter(MakeHolder<NYson::TYsonWriter>(ResultYsonOut.Get(), NYson::EYsonFormat::Binary, ::NYson::EYsonType::Node, true))
    {
        ResultYsonWriter->OnBeginList();
        if (Settings) {
            PullRequestTimeout = TDuration::MilliSeconds(settings->PullRequestTimeoutMs.Get().GetOrElse(0));
            PingTimeout = TDuration::MilliSeconds(settings->PingTimeoutMs.Get().GetOrElse(0));
            PingPeriod = Max(PingTimeout/4, TDuration::MilliSeconds(1000));

            SizeLimit = Settings->_AllResultsBytesLimit.Get().GetOrElse(64000000);
            YQL_LOG(DEBUG) << "_AllResultsBytesLimit = " << SizeLimit;

            if (Settings->_RowsLimitPerWrite.Get()) {
                YQL_LOG(DEBUG) << "_RowsLimitPerWrite = " << *Settings->_RowsLimitPerWrite.Get();
                RowsLimit = Settings->_RowsLimitPerWrite.Get();
            }
        }
    }

private:
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
        hFunc(TEvents::TEvUndelivered, [this] (TEvents::TEvUndelivered::TPtr& ev) {
            YQL_LOG_CTX_SCOPE(TraceId);
            TString message = "Undelivered from " + ToString(ev->Sender) + " to " + ToString(SelfId())
                + " reason: " + ToString(ev->Get()->Reason) + " sourceType: " + ToString(ev->Get()->SourceType >> 16)
                + "." + ToString(ev->Get()->SourceType & 0xFFFF);
            OnError(message, true, true);
        })
        cFunc(TEvents::TEvWakeup::EventType, OnWakeup)
        cFunc(TEvents::TEvGone::EventType, OnFullResultWriterShutdown)
    })


    STRICT_STFUNC(ShutdownHandler, {
        HFunc(TEvents::TEvGone, OnShutdownQueryResult);
        HANDLER_STUB(TEvPullResult)
        HANDLER_STUB(TEvReadyState)
        HANDLER_STUB(TEvPullDataResponse)
        HANDLER_STUB(TEvPingResponse)
        HANDLER_STUB(TEvQueryResponse)
        HANDLER_STUB(TEvDqFailure)
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        HANDLER_STUB(TEvents::TEvUndelivered)
        HANDLER_STUB(TEvents::TEvWakeup)
    })

    void DoPassAway() override {
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(DEBUG) << __FUNCTION__;
    }

    void OnFullResultWriterShutdown() {
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(DEBUG) << "Got TEvGone";

        FullResultWriterID = {};
    }

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

    void OnError(const TString& message, bool retriable, bool needFallback) {
        YQL_LOG(ERROR) << "OnError " << message;
        auto issueCode = needFallback
            ? TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR
            : TIssuesIds::DQ_GATEWAY_ERROR;
        auto req = MakeHolder<TEvDqFailure>(TIssue(message).SetCode(issueCode, TSeverityIds::S_ERROR), retriable, needFallback);
        FlushCounters(req->Record);
        Send(ExecuterID, req.Release());
    }

    void OnPingResponse(TEvPingResponse::TPtr&, const TActorContext&) {
        PingRequested = false;
    }

    void OnPullResponse(TEvPullDataResponse::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_SCOPE(TraceId);

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
                Finish(Truncated);
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

        if (!Discard) {
            auto fullResultTableEnabled = Settings->EnableFullResultWrite.Get().GetOrElse(false);

            if (fullResultTableEnabled && Truncated) {
                WriteToFullResultTable(new NDqProto::TData(std::move(*response.MutableData())));
            } else {
                DataParts.emplace_back(std::move(*response.MutableData()));

                bool full = true;
                bool exceedRows = false;
                try {
                    full = ResultBuilder->WriteYsonData(DataParts.back(), [this, &exceedRows](const TString& rawYson) {
                        if (RowsLimit && Rows + 1 > *RowsLimit) {
                            exceedRows = true;
                            return false;
                        } else if (ResultYsonOut->Counter() + rawYson.size() > SizeLimit) {
                            return false;
                        }
                        ResultYsonWriter->OnListItem();
                        ResultYsonWriter->OnRaw(rawYson);
                        ++Rows;
                        return true;
                    });
                } catch (...) {
                    OnError(CurrentExceptionMessage(), false, false);
                    return;
                }

                if (!full) {
                    if (fullResultTableEnabled) {
                        FlushCurrent();
                    } else {
                        TString issueMsg;
                        if (exceedRows) {
                            issueMsg = TStringBuilder() << "Rows limit reached: " << *RowsLimit;
                        } else {
                            issueMsg = TStringBuilder() << "Size limit reached: " << SizeLimit;
                        }
                        TIssue issue(issueMsg);
                        issue.Severity = TSeverityIds::S_WARNING;
                        Finish(/*truncated = */ true, {issue});
                    }
                }
            }
        }
    }

    void FlushCurrent() {
        YQL_LOG(DEBUG) << __FUNCTION__;
        YQL_ENSURE(!Truncated);
        YQL_ENSURE(!FullResultWriterID);
        YQL_ENSURE(Settings->EnableFullResultWrite.Get().GetOrElse(false));

        NDqProto::TGraphExecutionEvent record;
        record.SetEventType(NDqProto::EGraphExecutionEventType::FULL_RESULT);
        NDqProto::TGraphExecutionEvent::TFullResultDescriptor payload;
        payload.SetResultType(ResultBuilder->GetSerializedType());
        record.MutableMessage()->PackFrom(payload);
        Send(GraphExecutionEventsId, new TEvGraphExecutionEvent(record));
        Synchronize<TEvGraphExecutionEvent>([this](TEvGraphExecutionEvent::TPtr& ev) {
            Y_VERIFY(ev->Get()->Record.GetEventType() == NYql::NDqProto::EGraphExecutionEventType::SYNC);
            YQL_LOG_CTX_SCOPE(TraceId);

            if (auto msg = ev->Get()->Record.GetErrorMessage()) {
                OnError(msg, false, true);
            } else {
                NActorsProto::TActorId fullResultWriterProto;
                ev->Get()->Record.GetMessage().UnpackTo(&fullResultWriterProto);
                FullResultWriterID = NActors::ActorIdFromProto(fullResultWriterProto);
                Truncated = true;
                WriteAllDataPartsToFullResultTable();
            }
        });
    }

    bool CanSendToFullResultWriter() {
        // TODO Customize
        return FullResultSentBytes - FullResultReceivedBytes <= 32_MB;
    }

    template <class TCallback>
    void UpdateEventQueueStatus(TCallback callback) {
        YQL_LOG(DEBUG) << "UpdateEQStatus before: sent " << (FullResultSentBytes / 1024.0) << " kB "
                       << " received " << (FullResultReceivedBytes / 1024.0) << " kB "
                       << " diff " << (FullResultSentBytes - FullResultReceivedBytes) / 1024.0 << " kB";
        Send(FullResultWriterID, new TEvFullResultWriterStatusRequest());
        Synchronize<TEvFullResultWriterStatusResponse>([this, callback](TEvFullResultWriterStatusResponse::TPtr& ev) {
            YQL_LOG_CTX_SCOPE(TraceId);
            this->FullResultReceivedBytes = ev->Get()->Record.GetBytesReceived();
            YQL_LOG(DEBUG) << "UpdateEQStatus after: sent " << (FullResultSentBytes / 1024.0) << " kB "
                           << " received " << (FullResultReceivedBytes / 1024.0) << " kB "
                           << " diff " << (FullResultSentBytes - FullResultReceivedBytes) / 1024.0 << " kB";
            if (ev->Get()->Record.HasErrorMessage()) {
                YQL_LOG(DEBUG) << "Received error message: " << ev->Get()->Record.GetErrorMessage();
                OnError(ev->Get()->Record.GetErrorMessage(), false, false);
                return;
            }
            callback();
        });
    }

    void WriteAllDataPartsToFullResultTable() {
        while (FullResultSentDataParts < DataParts.size() && CanSendToFullResultWriter()) {
            UnsafeWriteToFullResultTable(DataParts[FullResultSentDataParts]);
            DataParts[FullResultSentDataParts].Clear();
            ++FullResultSentDataParts;
        }
        if (FullResultSentDataParts == DataParts.size()) {
            return;
        }
        // here we cannot continue since the event queue is overloaded
        // kind of tail recursion (but without recursion)
        UpdateEventQueueStatus([this]() {
            WriteAllDataPartsToFullResultTable();
        });
    }

    void WriteToFullResultTable(TAutoPtr<NDqProto::TData> data) {
        if (CanSendToFullResultWriter()) {
            UnsafeWriteToFullResultTable(*data);
            return;
        }
        UpdateEventQueueStatus([this, data]() {
            WriteToFullResultTable(data);
        });
    }

    void UnsafeWriteToFullResultTable(const NDqProto::TData& data) {
        NDqProto::TPullResponse response;
        response.SetResponseType(EPullResponseType::CONTINUE);
        response.MutableData()->CopyFrom(data);
        ui64 respSize = response.ByteSizeLong();
        Send(FullResultWriterID, MakeHolder<TEvPullDataResponse>(response));
        FullResultSentBytes += respSize;
    }

    void Finish(bool truncated = false, const TIssues& issues = {}) {
        YQL_LOG(DEBUG) << __FUNCTION__ << ", truncated=" << truncated;
        YQL_ENSURE(!FinishCalled);
        FinishCalled = true;
        FinishTruncated = truncated;
        FinishIssues = issues;
        if (FullResultWriterID) {
            NDqProto::TPullResponse response;
            response.SetResponseType(EPullResponseType::FINISH);
            Send(FullResultWriterID, MakeHolder<TEvPullDataResponse>(response));
        } else {
            DoFinish();
        }
    }

    void DoFinish() {
        Send(ExecuterID, new TEvGraphFinished());
    }

    void OnFullResultWriterResponse(TEvDqFailure::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(DEBUG) << __FUNCTION__;
        if (ev->Get()->Record.IssuesSize() == 0) {
            DoFinish();
        } else {
            Send(ExecuterID, ev->Release().Release());
        }
    }

    void OnQueryResult(TEvQueryResponse::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_ENSURE(!ev->Get()->Record.HasResultSet() && ev->Get()->Record.GetYson().empty());
        YQL_LOG(DEBUG) << "Shutting down TResultAggregator";

        BlockingActors.clear();
        if (FullResultWriterID) {
            BlockingActors.insert(FullResultWriterID);
            Send(FullResultWriterID, MakeHolder<TEvents::TEvPoison>());
        }

        YQL_LOG(DEBUG) << "Waiting for " << BlockingActors.size() << " blocking actors";

        QueryResponse.Reset(ev->Release().Release());
        Become(&TResultAggregator::ShutdownHandler);
        Send(SelfId(), MakeHolder<TEvents::TEvGone>());
    }

    void OnShutdownQueryResult(TEvents::TEvGone::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_SCOPE(TraceId);
        auto iter = BlockingActors.find(ev->Sender);
        if (iter != BlockingActors.end()) {
            BlockingActors.erase(iter);
        }

        YQL_LOG(DEBUG) << "Shutting down TResultAggregator, " << BlockingActors.size() << " blocking actors left";

        if (BlockingActors.empty()) {
            EndOnQueryResult();
        }
    }

    void EndOnQueryResult() {
        YQL_LOG(DEBUG) << __FUNCTION__;
        NDqProto::TQueryResponse result = QueryResponse->Record;

        YQL_ENSURE(!result.HasResultSet() && result.GetYson().empty());
        FlushCounters(result);

        if (ResultYsonWriter) {
            ResultYsonWriter->OnEndList();
            ResultYsonWriter.Destroy();
        }
        ResultYsonOut.Destroy();

        *result.MutableYson() = ResultYson;

        if (!FinishIssues.Empty()) {
            IssuesToMessage(FinishIssues, result.MutableIssues());
        }
        result.SetTruncated(FinishTruncated);

        Send(ExecuterID, new TEvQueryResponse(std::move(result)));
    }

    const NActors::TActorId ExecuterID;
    NActors::TActorId SourceID;
    const NActors::TActorId GraphExecutionEventsId;
    const bool Discard;
    TVector<NDqProto::TData> DataParts;
    const TString TraceId;
    TDqConfiguration::TPtr Settings;
    TDuration PullRequestTimeout;
    TDuration PingTimeout;
    TDuration PingPeriod;
    TInstant PingStartTime;
    TInstant PullRequestStartTime;
    bool PingRequested = false;
    NActors::TSchedulerCookieHolder TimerCookieHolder;
    ui64 SizeLimit = 64000000; // GRPC limit
    TMaybe<ui64> RowsLimit;
    ui64 Rows = 0;
    bool Truncated = false;
    NActors::TActorId FullResultWriterID;
    THolder<TProtoBuilder> ResultBuilder;
    TString ResultYson;
    THolder<TCountingOutput> ResultYsonOut;
    THolder<NYson::TYsonWriter> ResultYsonWriter;
    ui64 FullResultSentBytes = 0;
    ui64 FullResultReceivedBytes = 0;
    ui64 FullResultSentDataParts = 0;

    TIssues FinishIssues;
    bool FinishTruncated = false;
    bool FinishCalled = false;

    THashSet<TActorId> BlockingActors;
    THolder<TEvQueryResponse> QueryResponse;
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
        result = NYql::MakeResultReceiver(columns, executerId, traceId, settings, secureParams, resultType, discard);
    }
    return MakeHolder<TLogWrapReceive>(result.Release(), traceId);
}

} // NYql::NDqs::NExecutionHelpers
