#include "result_receiver.h"
#include "proto_builder.h"

#include <ydb/library/yql/providers/dq/actors/execution_helpers.h>
#include <ydb/library/yql/providers/dq/actors/events.h>
#include <ydb/library/yql/providers/dq/actors/result_actor_base.h>

#include <ydb/library/yql/providers/dq/actors/executer_actor.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/scheduler_basic.h>
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

class TResultReceiver: public NYql::NDqs::NExecutionHelpers::TResultActorBase<TResultReceiver> {
public:
    using TBase = TResultActorBase<TResultReceiver>;

    static constexpr char ActorName[] = "YQL_DQ_RESULT_RECEIVER";

    explicit TResultReceiver(const TVector<TString>& columns, const NActors::TActorId& executerId, const TString& traceId, const TDqConfiguration::TPtr& settings,
        const THashMap<TString, TString>& /*secureParams*/, const TString& resultType, const NActors::TActorId& graphExecutionEventsId, bool discard)
        : TBase(columns, executerId, traceId, settings, resultType, graphExecutionEventsId, discard)
        , PendingMessages() {
    }

public:
    STFUNC(Handler) {
        switch (const ui32 etype = ev->GetTypeRewrite()) {
            HFunc(NDq::TEvDqCompute::TEvChannelData, OnChannelData);
            HFunc(TEvReadyState, OnReadyState);
            hFunc(TEvMessageProcessed, OnMessageProcessed);
            default:
                TBase::HandlerBase(ev);
        }
    }

    STFUNC(ShutdownHandler) {
        switch (const ui32 etype = ev->GetTypeRewrite()) {
            hFunc(TEvMessageProcessed, OnMessageProcessed);
            default:
                TBase::ShutdownHandlerBase(ev);
        }
    }

private:
    void OnChannelData(NDq::TEvDqCompute::TEvChannelData::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;

        bool finishRequested = ev->Get()->Record.GetChannelData().GetFinished();
        if (!FinishCalled) {
            const auto messageId = GetMessageId(ev);
            const auto hasData = ev->Get()->Record.GetChannelData().HasData();
            NDq::TDqSerializedBatch batch;
            batch.Proto = std::move(*ev->Get()->Record.MutableChannelData()->MutableData());
            if (batch.Proto.HasPayloadId()) {
                batch.Payload = ev->Get()->GetPayload(batch.Proto.GetPayloadId());
            }
            OnReceiveData(std::move(batch), messageId, !hasData);
            const auto [it, inserted] = PendingMessages.insert({messageId, std::move(ev)});
            Y_ENSURE(inserted);
        }

        YQL_CLOG(DEBUG, ProviderDq) << "Finished: " << finishRequested;
    }

    void OnReadyState(TEvReadyState::TPtr&, const TActorContext&) {
        // do nothing
    }

    void OnMessageProcessed(TEvMessageProcessed::TPtr& ev) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
        YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
        SendAck(ev->Get()->MessageId);
    }

    void SendAck(const TString& messageId) {
        const auto messageIt = PendingMessages.find(messageId);
        Y_ABORT_UNLESS(messageIt != PendingMessages.end());
        const auto& message = messageIt->second;

        auto req = MakeHolder<NDq::TEvDqCompute::TEvChannelDataAck>();
        req->Record.SetChannelId(message->Get()->Record.GetChannelData().GetChannelId());
        req->Record.SetSeqNo(message->Get()->Record.GetSeqNo());
        req->Record.SetFreeSpace((i64)256_MB - (i64)InflightBytes());
        req->Record.SetFinish(EarlyFinish);  // set if premature finish started (when response limit reached and FullResultTable not enabled)

        Send(message->Sender, req.Release());
        PendingMessages.erase(messageIt);
    }

    TString GetMessageId(const NDq::TEvDqCompute::TEvChannelData::TPtr& message) const {
        TStringBuilder res;
        res << message->Get()->Record.GetChannelData().GetChannelId()
            << " " << message->Get()->Record.GetSeqNo()
            << " " << ToString(message->Sender);
        return res;
    }

    void FinishFullResultWriter() override {
        Finish();
    }

private:
    THashMap<TString, NDq::TEvDqCompute::TEvChannelData::TPtr> PendingMessages;  // N.B. TEvChannelData is partially moved
};

} /* namespace */

THolder<NActors::IActor> MakeResultReceiver(
    const TVector<TString>& columns,
    const NActors::TActorId& executerId,
    const TString& traceId,
    const TDqConfiguration::TPtr& settings,
    const THashMap<TString, TString>& secureParams,
    const TString& resultType,
    const NActors::TActorId& graphExecutionEventsId,
    bool discard) {
    return MakeHolder<TResultReceiver>(columns, executerId, traceId, settings, secureParams, resultType, graphExecutionEventsId, discard);
}

} /* namespace NYql */
