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

class TResultReceiver: public NYql::NDqs::NExecutionHelpers::TResultActorBase<TResultReceiver> {
public:
    static constexpr char ActorName[] = "YQL_DQ_RESULT_RECEIVER";

    explicit TResultReceiver(const TVector<TString>& columns, const NActors::TActorId& executerId, const TString& traceId, const TDqConfiguration::TPtr& settings,
        const THashMap<TString, TString>& /*secureParams*/, const TString& resultType, const NActors::TActorId& graphExecutionEventsId, bool discard)
        : TResultActorBase<TResultReceiver>(columns, executerId, traceId, settings, resultType, graphExecutionEventsId, discard) {
    }

public:
    STRICT_STFUNC(Handler, {
        HFunc(NDq::TEvDqCompute::TEvChannelData, OnChannelData)
        HFunc(TEvReadyState, OnReadyState);
        HFunc(TEvQueryResponse, OnQueryResult);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        HFunc(TEvDqFailure, OnFullResultWriterResponse);
        hFunc(TEvents::TEvUndelivered, OnUndelivered);
        cFunc(TEvents::TEvGone::EventType, OnFullResultWriterShutdown);
        sFunc(TEvResultReceiverFinish, Finish);
    })

private:
    void OnChannelData(NDq::TEvDqCompute::TEvChannelData::TPtr& ev, const TActorContext&) {
        YQL_LOG_CTX_SCOPE(TraceId);
        YQL_LOG(DEBUG) << __FUNCTION__;

        if (!FinishCalled) {
            OnReceiveData(std::move(*ev->Get()->Record.MutableChannelData()->MutableData()));
        }
        if (!FinishCalled && ev->Get()->Record.GetChannelData().GetFinished()) {
            Send(SelfId(), MakeHolder<TEvResultReceiverFinish>());  // postpone finish until TFullResultWriterActor is instaniated
        }

        // todo: ack after data is stored to yt?
        auto res = MakeHolder<NDq::TEvDqCompute::TEvChannelDataAck>();
        res->Record.SetChannelId(ev->Get()->Record.GetChannelData().GetChannelId());
        res->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        res->Record.SetFreeSpace(256_MB);
        res->Record.SetFinish(FinishCalled);  // set if premature finish started (when response limit reached and FullResultTable not enabled)
        Send(ev->Sender, res.Release());

        YQL_LOG(DEBUG) << "Finished: " << ev->Get()->Record.GetChannelData().GetFinished();
    }

    void OnReadyState(TEvReadyState::TPtr&, const TActorContext&) {
        // do nothing
    }
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
