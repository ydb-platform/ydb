#include "result_receiver.h"
#include "proto_builder.h"

#include <ydb/library/yql/providers/dq/actors/execution_helpers.h>
#include <ydb/library/yql/providers/dq/actors/events.h>

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/executer_actor.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/utils/log/log.h> 
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h> 

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/executor_pool_basic.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/scheduler_basic.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/size_literals.h>
#include <util/generic/ptr.h>
#include <util/string/split.h>
#include <util/system/types.h>

namespace NYql {

using namespace NActors;
using namespace NDqs;

namespace {

class TResultReceiver: public TRichActor<TResultReceiver> {
public:
    static constexpr char ActorName[] = "YQL_DQ_RESULT_RECEIVER";

    explicit TResultReceiver(const TVector<TString>& columns, const NActors::TActorId& executerId, const TString& traceId, const TDqConfiguration::TPtr& settings,
        const THashMap<TString, TString>& secureParams, const TString& resultType, bool discard)
        : TRichActor<TResultReceiver>(&TResultReceiver::Handler)
        , ExecuterId(executerId)
        , TraceId(traceId)
        , Settings(settings)
        , SecureParams(std::move(secureParams))
        , ResultBuilder(
            resultType
            ? MakeHolder<TProtoBuilder>(resultType, columns)
            : nullptr)
        , Discard(discard)
    {
        if (Settings) {
            if (Settings->_AllResultsBytesLimit.Get()) {
                YQL_LOG(DEBUG) << "_AllResultsBytesLimit = " << *Settings->_AllResultsBytesLimit.Get();
            }
            if (Settings->_RowsLimitPerWrite.Get()) {
                YQL_LOG(DEBUG) << "_RowsLimitPerWrite = " << *Settings->_RowsLimitPerWrite.Get();
            }
        }

        Y_UNUSED(Size);
        Y_UNUSED(Rows);
    }

private:
    STRICT_STFUNC(Handler, {
        HFunc(NDq::TEvDqCompute::TEvChannelData, OnChannelData)
        HFunc(TEvReadyState, OnReadyState);
        HFunc(TEvQueryResponse, OnQueryResult);
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    })

    void OnChannelData(NDq::TEvDqCompute::TEvChannelData::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_SCOPE(TraceId);

        auto res = MakeHolder<NDq::TEvDqCompute::TEvChannelDataAck>();

        if (!Discard) {
            if (!Finished && ev->Get()->Record.GetChannelData().GetData().GetRaw().size() > 0) {
                DataParts.emplace_back(std::move(ev->Get()->Record.GetChannelData().GetData()));
                Size += DataParts.back().GetRaw().size();
                Rows += DataParts.back().GetRows();
                YQL_LOG(DEBUG) << "Size: " << Size;
                YQL_LOG(DEBUG) << "Rows: " << Rows;
            }

            if (Size > 64000000 /* grpc limit*/) {
                OnError("Too big result (grpc limit reached: " + ToString(Size) + " > 64000000)" , false, true);
            } else if (Settings && Settings->_AllResultsBytesLimit.Get() && Size > *Settings->_AllResultsBytesLimit.Get()) {
                TIssue issue("Size limit reached: " + ToString(Size) + ">" + ToString(Settings->_AllResultsBytesLimit.Get()));
                issue.Severity = TSeverityIds::S_WARNING;
                Issues.AddIssue(issue);
                Finish(/*truncated = */ true);
            } else if (Settings && Settings->_RowsLimitPerWrite.Get() && Rows > *Settings->_RowsLimitPerWrite.Get()) {
                TIssue issue("Rows limit reached: " + ToString(Rows) + ">" + ToString(Settings->_RowsLimitPerWrite.Get()));
                issue.Severity = TSeverityIds::S_WARNING;
                Issues.AddIssue(issue);
                Finish(/*truncated = */ true);
            }
        }

        res->Record.SetChannelId(ev->Get()->Record.GetChannelData().GetChannelId());
        res->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        res->Record.SetFreeSpace(256_MB);
        res->Record.SetFinish(Finished);
        Send(ev->Sender, res.Release());

        YQL_LOG(DEBUG) << "Finished: " << ev->Get()->Record.GetChannelData().GetFinished();
    }

    void OnReadyState(TEvReadyState::TPtr&, const TActorContext&) {
        // do nothing
    }

    void OnQueryResult(TEvQueryResponse::TPtr& ev, const TActorContext&) {
        NDqProto::TQueryResponse result(ev->Get()->Record);
        YQL_ENSURE(!result.HasResultSet() && result.GetYson().empty());

        if (ResultBuilder) {
            try {
                TString yson = Discard ? "" : ResultBuilder->BuildYson(
                    DataParts,
                    Settings && Settings->_AllResultsBytesLimit.Get()
                        ? *Settings->_AllResultsBytesLimit.Get()
                        : 64000000 /* grpc limit*/);
                *result.MutableYson() = yson;
            } catch (...) {
                Issues.AddIssue(TIssue(CurrentExceptionMessage()).SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_WARNING));
                result.SetNeedFallback(true);
            }
        } else {
            if (Rows > 0) {
                Issues.AddIssue(TIssue("Non empty rows: " + ToString(Rows)).SetCode(0, TSeverityIds::S_WARNING));
            }
        }

        if (!Issues.Empty()) {
            IssuesToMessage(Issues, result.MutableIssues());
        }
        result.SetTruncated(Truncated);
        Send(ExecuterId, new TEvQueryResponse(std::move(result)));
    }

    void OnError(const TString& message, bool retriable, bool needFallback) {
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(DEBUG) << "OnError " << message;
        auto req = MakeHolder<TEvDqFailure>(TIssue(message).SetCode(-1, TSeverityIds::S_ERROR), retriable, needFallback);
        Send(ExecuterId, req.Release());
        Finished = true;
    }

    void Finish(bool truncated = false) {
        Send(ExecuterId, new TEvGraphFinished());
        Finished = true;
        Truncated = truncated;
    }

    const NActors::TActorId ExecuterId;
    TVector<NDqProto::TData> DataParts;
    const TString TraceId;
    TDqConfiguration::TPtr Settings;
    bool Finished = false;
    bool Truncated = false;
    TIssues Issues;
    ui64 Size = 0;
    ui64 Rows = 0;
    // const Yql::DqsProto::TFullResultTable FullResultTable;
    const THashMap<TString, TString> SecureParams;
    // THolder<IFullResultWriter> FullResultWriter;
    THolder<TProtoBuilder> ResultBuilder;
    bool Discard = false;
};

} /* namespace */

THolder<NActors::IActor> MakeResultReceiver(const TVector<TString>& columns, const NActors::TActorId& executerId, const TString& traceId, const TDqConfiguration::TPtr& settings, const THashMap<TString, TString>& secureParams, const TString& resultType, bool discard) {
    return MakeHolder<TResultReceiver>(columns, executerId, traceId, settings, secureParams, resultType, discard);
}

} /* namespace NYql */
