#pragma once

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/events.h>
#include <ydb/library/yql/providers/dq/actors/proto_builder.h>
#include <ydb/library/yql/providers/dq/api/protos/dqs.pb.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/utils/failure_injector/failure_injector.h>

#include <util/stream/holder.h>
#include <util/stream/length.h>
#include <util/generic/queue.h>

namespace NYql::NDqs::NExecutionHelpers {

struct TQueueItem {
    TQueueItem(NDq::TDqSerializedBatch&& data, const TString& messageId)
        : Data(std::move(data))
        , MessageId(messageId)
        , SentProcessedEvent(false)
        , IsFinal(false)
        , Size(Data.Size())
        {
        }

    static TQueueItem Final() {
        TQueueItem item({}, "FinalMessage");
        item.SentProcessedEvent = true;
        item.IsFinal = true;
        return item;
    }

    NDq::TDqSerializedBatch Data;
    const TString MessageId;
    bool SentProcessedEvent = false;
    bool IsFinal = false;
    ui64 Size = 0;
};

struct TWriteQueue {
    TQueue<TQueueItem> Queue;
    ui64 ByteSize = 0;

    template< class... Args >
    decltype(auto) emplace( Args&&... args) {
        Queue.emplace(std::forward<Args>(args)...);
        ByteSize += Queue.back().Size;
    }

    auto& front() {
        return Queue.front();
    }

    auto& back() {
        return Queue.back();
    }

    auto pop() {
        YQL_ENSURE(ByteSize >= Queue.front().Size);
        ByteSize -= Queue.front().Size;
        return Queue.pop();
    }

    auto empty() const {
        return Queue.empty();
    }

    void clear() {
        Queue.clear();
        ByteSize = 0;
    }
};

    template <class TDerived>
    class TResultActorBase : public NYql::TSynchronizableRichActor<TDerived>, public NYql::TCounters {
    protected:
        using TBase = NYql::TSynchronizableRichActor<TDerived>;

        TResultActorBase(
            const TVector<TString>& columns,
            const NActors::TActorId& executerId,
            const TString& traceId,
            const TDqConfiguration::TPtr& settings,
            const TString& resultType,
            NActors::TActorId graphExecutionEventsId,
            bool discard)
            : TBase(&TDerived::Handler)
            , ExecuterID(executerId)
            , TraceId(traceId)
            , Settings(settings)
            , FinishCalled(false)
            , EarlyFinish(false)
            , FullResultTableEnabled(settings->EnableFullResultWrite.Get().GetOrElse(false))
            , GraphExecutionEventsId(graphExecutionEventsId)
            , Discard(discard)
            , WriteQueue()
            , SizeLimit(
                (Settings && Settings->_AllResultsBytesLimit.Get().Defined())
                ? Settings->_AllResultsBytesLimit.Get().GetRef()
                : 64000000) // GRPC limit
            , RowsLimit(settings ? Settings->_RowsLimitPerWrite.Get() : Nothing())
            , Rows(0)
            , Truncated(false)
            , FullResultWriterID()
            , ResultBuilder(resultType ? MakeHolder<TProtoBuilder>(resultType, columns) : nullptr)
            , ResultYson()
            , ResultYsonOut(new THoldingStream<TCountingOutput>(MakeHolder<TStringOutput>(ResultYson)))
            , ResultYsonWriter(MakeHolder<NYson::TYsonWriter>(ResultYsonOut.Get(), NYson::EYsonFormat::Binary, ::NYson::EYsonType::Node, true))
            , Issues()
            , BlockingActors()
            , QueryResponse()
            , WaitingAckFromFRW(false) {
            ResultYsonWriter->OnBeginList();
            YQL_CLOG(DEBUG, ProviderDq) << "_AllResultsBytesLimit = " << SizeLimit;
            YQL_CLOG(DEBUG, ProviderDq) << "_RowsLimitPerWrite = " << (RowsLimit.Defined() ? ToString(RowsLimit.GetRef()) : "nothing");
        }

        virtual void FinishFullResultWriter() {
            TBase::Send(FullResultWriterID, MakeHolder<NActors::TEvents::TEvPoison>());
        }

        void OnReceiveData(NDq::TDqSerializedBatch&& data, const TString& messageId = "", bool autoAck = false) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);

            if (data.RowCount() > 0 && !ResultBuilder) {
                Issues.AddIssue(TIssue("Non empty rows: >=" + ToString(data.RowCount())).SetCode(0, TSeverityIds::S_WARNING));
            }
            if (Discard || !ResultBuilder || autoAck) {
                TBase::Send(TBase::SelfId(), MakeHolder<TEvMessageProcessed>(messageId));
                return;
            }

            WriteQueue.emplace(std::move(data), messageId);
            if (FullResultTableEnabled && FullResultWriterID) {
                TryWriteToFullResultTable();
            } else {
                bool full = true;
                bool exceedRows = false;
                try {
                    TFailureInjector::Reach("result_actor_base_fail_on_response_write", [] { throw yexception() << "result_actor_base_fail_on_response_write"; });
                    NDq::TDqSerializedBatch dataCopy = WriteQueue.back().Data;
                    dataCopy.ConvertToNoOOB();
                    full = ResultBuilder->WriteYsonData(std::move(dataCopy), [this, &exceedRows](const TString& rawYson) {
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
                    OnError(NYql::NDqProto::StatusIds::UNSUPPORTED, CurrentExceptionMessage());
                    return;
                }

                if (full) {
                    WriteQueue.back().SentProcessedEvent = true;
                    TBase::Send(TBase::SelfId(), MakeHolder<TEvMessageProcessed>(messageId));
                    return;
                }

                Truncated = true;
                if (FullResultTableEnabled) {
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
                    Issues.AddIssues({issue});
                    EarlyFinish = true;
                    Finish();
                }
            }
        }

        void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, const TString& message) {
            YQL_CLOG(ERROR, ProviderDq) << "OnError " << message;
            auto issueCode = NCommon::NeedFallback(statusCode)
                ? TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR
                : TIssuesIds::DQ_GATEWAY_ERROR;
            const auto issue = TIssue(message).SetCode(issueCode, TSeverityIds::S_ERROR);
            Issues.AddIssues({issue});  // remember issue to pass it with TEvQueryResponse, cause executor_actor ignores TEvDqFailure after finish
            auto req = MakeHolder<TEvDqFailure>(statusCode, issue);
            FlushCounters(req->Record);
            TBase::Send(ExecuterID, req.Release());
        }

        void Finish() {
            YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__ << ", truncated=" << Truncated;
            YQL_ENSURE(!FinishCalled);
            FinishCalled = true;

            if (FullResultWriterID) {
                WriteQueue.emplace(TQueueItem::Final());
                TryWriteToFullResultTable();
            } else {
                DoFinish();
            }
        }

    protected:
        STFUNC(HandlerBase) {
            switch (const ui32 etype = ev->GetTypeRewrite()) {
                hFunc(NActors::TEvents::TEvUndelivered, OnUndelivered);
                HFunc(TEvQueryResponse, OnQueryResult);
                HFunc(TEvFullResultWriterAck, OnFullResultWriterAck);
                HFunc(TEvDqFailure, OnFullResultWriterResponse);
                cFunc(NActors::TEvents::TEvGone::EventType, OnFullResultWriterShutdown);
                cFunc(NActors::TEvents::TEvPoison::EventType, TBase::PassAway)
                default:
                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
                    YQL_CLOG(DEBUG, ProviderDq) << "Unexpected event " << etype;
                    break;
            }
        }

        STFUNC(ShutdownHandlerBase) {
            switch (const ui32 etype = ev->GetTypeRewrite()) {
                HFunc(NActors::TEvents::TEvGone, OnShutdownQueryResult);
                cFunc(NActors::TEvents::TEvPoison::EventType, TBase::PassAway);
                HFunc(TEvDqFailure, OnErrorInShutdownState);
                HFunc(TEvFullResultWriterAck, OnFullResultWriterAck);
                default:
                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
                    YQL_CLOG(DEBUG, ProviderDq) << "Unexpected event " << etype;
                    break;
            }
        }

        ui64 InflightBytes() {
            return WriteQueue.ByteSize;
        }

    private:
        void OnQueryResult(TEvQueryResponse::TPtr& ev, const NActors::TActorContext&) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            YQL_ENSURE(!ev->Get()->Record.HasResultSet() && ev->Get()->Record.GetYson().empty());
            YQL_CLOG(DEBUG, ProviderDq) << "Shutting down TResultAggregator";

            BlockingActors.clear();
            if (FullResultWriterID) {
                BlockingActors.insert(FullResultWriterID);
                FinishFullResultWriter();
            }

            YQL_CLOG(DEBUG, ProviderDq) << "Waiting for " << BlockingActors.size() << " blocking actors";

            QueryResponse.Reset(ev->Release().Release());
            TBase::Become(&TDerived::ShutdownHandler);
            TBase::Send(TBase::SelfId(), MakeHolder<NActors::TEvents::TEvGone>());
        }

        void OnFullResultWriterShutdown() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            YQL_CLOG(DEBUG, ProviderDq) << "Got TEvGone";

            FullResultWriterID = {};
        }

        void OnFullResultWriterResponse(NYql::NDqs::TEvDqFailure::TPtr& ev, const NActors::TActorContext&) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
            if (ev->Get()->Record.IssuesSize() == 0) {  // weird way used by writer to acknowledge it's death
                DoFinish();
            } else {
                WaitingAckFromFRW = false;
                WriteQueue.clear();
                Y_ABORT_UNLESS(ev->Get()->Record.GetStatusCode() != NYql::NDqProto::StatusIds::SUCCESS);
                TBase::Send(ExecuterID, ev->Release().Release());
            }
        }

        void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            TString message = "Undelivered from " + ToString(ev->Sender) + " to " + ToString(TBase::SelfId())
                + " reason: " + ToString(ev->Get()->Reason) + " sourceType: " + ToString(ev->Get()->SourceType >> 16)
                + "." + ToString(ev->Get()->SourceType & 0xFFFF);
            OnError(NYql::NDqProto::StatusIds::UNAVAILABLE, message);
        }

        void OnFullResultWriterAck(TEvFullResultWriterAck::TPtr& ev, const NActors::TActorContext&) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
            Y_ABORT_UNLESS(ev->Get()->Record.GetMessageId() == WriteQueue.front().MessageId);
            if (!WriteQueue.front().SentProcessedEvent) {  // messages, received before limits exceeded, are already been reported
                TBase::Send(TBase::SelfId(), MakeHolder<TEvMessageProcessed>(WriteQueue.front().MessageId));
            }
            WriteQueue.pop();

            if (WriteQueue.empty()) {
                WaitingAckFromFRW = false;
                return;
            }

            UnsafeWriteToFullResultTable();
        }

        void OnErrorInShutdownState(NYql::NDqs::TEvDqFailure::TPtr& ev, const NActors::TActorContext&) {
            // FullResultWriter will always send TEvGone after this, so these issues will be passed to executor with TEvQueryResponse
            TIssues issues;
            IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
            Issues.AddIssues(issues);
        }

        void OnShutdownQueryResult(NActors::TEvents::TEvGone::TPtr& ev, const NActors::TActorContext&) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            auto iter = BlockingActors.find(ev->Sender);
            if (iter != BlockingActors.end()) {
                BlockingActors.erase(iter);
            }

            YQL_CLOG(DEBUG, ProviderDq) << "Shutting down TResultAggregator, " << BlockingActors.size() << " blocking actors left";

            if (BlockingActors.empty()) {
                EndOnQueryResult();
            }
        }

        void DoFinish() {
            TBase::Send(ExecuterID, new TEvGraphFinished());
        }

        void FlushCurrent() {
            YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
            YQL_ENSURE(!FullResultWriterID);
            YQL_ENSURE(FullResultTableEnabled);

            NDqProto::TGraphExecutionEvent record;
            record.SetEventType(NDqProto::EGraphExecutionEventType::FULL_RESULT);
            NDqProto::TGraphExecutionEvent::TFullResultDescriptor payload;
            payload.SetResultType(ResultBuilder->GetSerializedType());
            record.MutableMessage()->PackFrom(payload);
            TBase::Send(GraphExecutionEventsId, new TEvGraphExecutionEvent(record));
            TBase::template Synchronize<TEvGraphExecutionEvent>([this](TEvGraphExecutionEvent::TPtr& ev) {
                Y_ABORT_UNLESS(ev->Get()->Record.GetEventType() == NYql::NDqProto::EGraphExecutionEventType::SYNC);
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);

                if (auto msg = ev->Get()->Record.GetErrorMessage()) {
                    OnError(NYql::NDqProto::StatusIds::UNSUPPORTED, msg);
                } else {
                    NActorsProto::TActorId fullResultWriterProto;
                    ev->Get()->Record.GetMessage().UnpackTo(&fullResultWriterProto);
                    FullResultWriterID = NActors::ActorIdFromProto(fullResultWriterProto);
                    TryWriteToFullResultTable();
                }
            });
        }

        void EndOnQueryResult() {
            YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
            NDqProto::TQueryResponse result = QueryResponse->Record;

            YQL_ENSURE(!result.HasResultSet() && result.GetYson().empty());
            FlushCounters(result);

            if (ResultYsonWriter) {
                ResultYsonWriter->OnEndList();
                ResultYsonWriter.Destroy();
            }
            ResultYsonOut.Destroy();

            *result.MutableYson() = ResultYson;

            if (!Issues.Empty()) {
                NYql::IssuesToMessage(Issues, result.MutableIssues());
            }
            result.SetTruncated(Truncated);

            TBase::Send(ExecuterID, new TEvQueryResponse(std::move(result)));
        }

        void DoPassAway() override {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
        }

        void TryWriteToFullResultTable() {
            if (WaitingAckFromFRW) {
                return;
            }
            WaitingAckFromFRW = true;
            UnsafeWriteToFullResultTable();
        }

        void UnsafeWriteToFullResultTable() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
            YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;

            auto& src = WriteQueue.front();
            auto req = MakeHolder<TEvFullResultWriterWriteRequest>();

            req->Record.SetMessageId(src.MessageId);
            *(req->Record.MutableData()) = std::move(src.Data.Proto);
            req->Record.MutableData()->ClearPayloadId();
            if (!src.Data.Payload.IsEmpty()) {
                req->Record.MutableData()->SetPayloadId(req->AddPayload(std::move(src.Data.Payload)));
            }
            req->Record.SetFinish(src.IsFinal);

            TBase::Send(FullResultWriterID, std::move(req));
        }

    protected:
        const NActors::TActorId ExecuterID;
        const TString TraceId;
        TDqConfiguration::TPtr Settings;
        bool FinishCalled;
        bool EarlyFinish;

    private:
        const bool FullResultTableEnabled;
        const NActors::TActorId GraphExecutionEventsId;
        const bool Discard;
        TWriteQueue WriteQueue;
        ui64 SizeLimit;
        TMaybe<ui64> RowsLimit;
        ui64 Rows;
        bool Truncated;
        NActors::TActorId FullResultWriterID;
        THolder<TProtoBuilder> ResultBuilder;
        TString ResultYson;
        THolder<TCountingOutput> ResultYsonOut;
        THolder<NYson::TYsonWriter> ResultYsonWriter;
        TIssues Issues;
        THashSet<NActors::TActorId> BlockingActors;
        THolder<TEvQueryResponse> QueryResponse;
        bool WaitingAckFromFRW;
    };
} // namespace NYql::NDqs::NExecutionHelpers
