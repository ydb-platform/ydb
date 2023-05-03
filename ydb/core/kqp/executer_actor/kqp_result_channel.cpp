#include "kqp_result_channel.h"

#include "kqp_executer.h"
#include "kqp_executer_impl.h"
#include "kqp_executer_stats.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr {
namespace NKqp {
namespace {

class TResultCommonChannelProxy : public NActors::TActor<TResultCommonChannelProxy> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_RESULT_CHANNEL_PROXY;
    }

    TResultCommonChannelProxy(ui64 txId, ui64 channelId, TQueryExecutionStats* stats, TActorId executer)
        : TActor(&TResultCommonChannelProxy::WorkState)
        , TxId(txId)
        , ChannelId(channelId)
        , Stats(stats)
        , Executer(executer) {}

protected:
    virtual void SendResults(NYql::NDqProto::TEvComputeChannelData& computeData, TActorId sender) = 0;

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NYql::NDq::TEvDqCompute::TEvChannelData, HandleWork);
            hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleWork);
            hFunc(TEvents::TEvPoison, HandlePoison);
            default: {
                InternalError(TStringBuilder() << "TxId: " << TxId << ", channelId: " << ChannelId
                    << "Handle unexpected event " << ev->GetTypeRewrite());
            }
        }
    }

    void UpdateStatistics(const ::NYql::NDqProto::TData& data) {
        if (!Stats) {
            return;
        }

        Stats->ResultBytes += data.GetRaw().size();
        Stats->ResultRows += data.GetRows();
    }

    void HandleWork(NYql::NDq::TEvDqCompute::TEvChannelData::TPtr& ev) {
        NYql::NDqProto::TEvComputeChannelData& record = ev->Get()->Record;
        auto& channelData = record.GetChannelData();

        ComputeActor = ev->Sender;

        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ", got result"
            << ", channelId: " << channelData.GetChannelId()
            << ", seqNo: " << record.GetSeqNo()
            << ", from: " << ev->Sender);

        UpdateStatistics(channelData.GetData());
        SendResults(record, ev->Sender);
    }

    void HandleWork(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        ui64 seqNo = ev->Get()->Record.GetSeqNo();
        ui64 freeSpace = ev->Get()->Record.GetFreeSpace();

        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId
            << ", send ack to channelId: " << ChannelId
            << ", seqNo: " << seqNo
            << ", enough: " << ev->Get()->Record.GetEnough()
            << ", freeSpace: " << freeSpace
            << ", to: " << ComputeActor);

        auto ackEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelDataAck>();
        ackEv->Record.SetSeqNo(seqNo);
        ackEv->Record.SetChannelId(ChannelId);
        ackEv->Record.SetFreeSpace(freeSpace);
        ackEv->Record.SetFinish(ev->Get()->Record.GetEnough());
        Send(ComputeActor, ackEv.Release(), /* TODO: undelivery */ 0, /* cookie */ ChannelId);
    }

    void InternalError(const TString& msg) {
        LOG_CRIT_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER, msg);

        auto evAbort = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::INTERNAL_ERROR, msg);
        Send(Executer, evAbort.Release());

        Become(&TResultCommonChannelProxy::DeadState);
    }

private:
    STATEFN(DeadState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPoison, HandlePoison);
        }
    }

private:
    void HandlePoison(TEvents::TEvPoison::TPtr&) {
        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId
            << ", result channelId: " << ChannelId << ", pass away");
        PassAway();
    }

private:
    const ui64 TxId;
    const ui64 ChannelId;
    TQueryExecutionStats* Stats; // owned by KqpExecuter
    const NActors::TActorId Executer;
    NActors::TActorId ComputeActor;
};

class TResultStreamChannelProxy : public TResultCommonChannelProxy {
public:
    TResultStreamChannelProxy(ui64 txId, ui64 channelId, NKikimr::NMiniKQL::TType* itemType,
        const TVector<ui32>* columnOrder, ui32 queryResultIndex, TActorId target, TQueryExecutionStats* stats,
        TActorId executer)
        : TResultCommonChannelProxy(txId, channelId, stats, executer)
        , ColumnOrder(columnOrder)
        , ItemType(itemType)
        , QueryResultIndex(queryResultIndex)
        , Target(target) {}

private:
    virtual void SendResults(NYql::NDqProto::TEvComputeChannelData& computeData, TActorId sender) {
        Y_UNUSED(sender);

        auto& channelData = computeData.GetChannelData();

        TKqpProtoBuilder protoBuilder{*AppData()->FunctionRegistry};
        auto resultSet = protoBuilder.BuildYdbResultSet({channelData.GetData()}, ItemType, ColumnOrder);

        auto streamEv = MakeHolder<TEvKqpExecuter::TEvStreamData>();
        streamEv->Record.SetSeqNo(computeData.GetSeqNo());
        streamEv->Record.SetQueryResultIndex(QueryResultIndex);
        streamEv->Record.MutableResultSet()->Swap(&resultSet);

        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER,
            "Send TEvStreamData to " << Target << ", seqNo: " << streamEv->Record.GetSeqNo()
            << ", nRows: " << channelData.GetData().GetRows() );

        Send(Target, streamEv.Release());
    }
private:
    const TVector<ui32>* ColumnOrder;
    NKikimr::NMiniKQL::TType* ItemType;
    ui32 QueryResultIndex = 0;
    const NActors::TActorId Target;
};

class TResultDataChannelProxy : public TResultCommonChannelProxy {
public:
    TResultDataChannelProxy(ui64 txId, ui64 channelId, TQueryExecutionStats* stats, TActorId executer,
        ui32 inputIndex, TEvKqpExecuter::TEvTxResponse* resultReceiver)
        : TResultCommonChannelProxy(txId, channelId, stats, executer)
        , InputIndex(inputIndex)
        , ResultReceiver(resultReceiver) {}

private:
    virtual void SendResults(NYql::NDqProto::TEvComputeChannelData& computeData, TActorId sender) {
        auto& channelData = computeData.GetChannelData();
        auto channelId = channelData.GetChannelId();

        ResultReceiver->TakeResult(InputIndex, channelData.GetData());

        auto ackEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelDataAck>();

        ackEv->Record.SetSeqNo(computeData.GetSeqNo());
        ackEv->Record.SetChannelId(channelId);
        ackEv->Record.SetFreeSpace(1_MB);

        Send(sender, ackEv.Release(), /* TODO: undelivery */ 0, /* cookie */ channelId);
    }

private:
    ui32 InputIndex;
    TEvKqpExecuter::TEvTxResponse* ResultReceiver;
};

} // anonymous namespace end

NActors::IActor* CreateResultStreamChannelProxy(ui64 txId, ui64 channelId, NKikimr::NMiniKQL::TType* itemType,
    const TVector<ui32>* columnOrder, ui32 queryResultIndex, TActorId target, TQueryExecutionStats* stats,
    TActorId executer)
{
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER,
        "CreateResultStreamChannelProxy: TxId: " << txId <<
        ", channelId: " << channelId
    );

    return new TResultStreamChannelProxy(txId, channelId, itemType, columnOrder, queryResultIndex, target,
        stats, executer);
}

NActors::IActor* CreateResultDataChannelProxy(ui64 txId, ui64 channelId,
    TQueryExecutionStats* stats, TActorId executer,
    ui32 inputIndex, TEvKqpExecuter::TEvTxResponse* resultsReceiver)
{
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER,
        "CreateResultDataChannelProxy: TxId: " << txId <<
        ", channelId: " << channelId
    );

    return new TResultDataChannelProxy(txId, channelId, stats, executer, inputIndex, resultsReceiver);
}

} // namespace NKqp
} // namespace NKikimr
