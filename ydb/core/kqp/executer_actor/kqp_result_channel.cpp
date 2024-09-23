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

struct TEvComputeChannelDataOOB {
    NYql::NDqProto::TEvComputeChannelData Proto;
    TRope Payload;

    size_t Size() const {
        return Proto.GetChannelData().GetData().GetRaw().size() + Payload.size();
    }

    ui32 RowCount() const {
        return Proto.GetChannelData().GetData().GetRows();
    }
};

class TResultCommonChannelProxy : public NActors::TActor<TResultCommonChannelProxy> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_RESULT_CHANNEL_PROXY;
    }

    TResultCommonChannelProxy(ui64 txId, ui64 channelId, TActorId executer)
        : TActor(&TResultCommonChannelProxy::WorkState)
        , TxId(txId)
        , ChannelId(channelId)
        , Executer(executer) {}

protected:
    virtual void SendResults(TEvComputeChannelDataOOB& computeData, TActorId sender) = 0;

private:
    STATEFN(WorkState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NYql::NDq::TEvDqCompute::TEvChannelData, HandleWork);
                hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleWork);
                hFunc(TEvents::TEvPoison, HandlePoison);
                default: {
                    InternalError(TStringBuilder() << "TxId: " << TxId << ", channelId: " << ChannelId
                        << "Handle unexpected event " << ev->GetTypeRewrite());
                }
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        } catch (const NKikimr::TMemoryLimitExceededException& ex) {
            InternalError("Memory limit exceeded exception", NYql::NDqProto::StatusIds::PRECONDITION_FAILED);
        }
    }

    void HandleWork(NYql::NDq::TEvDqCompute::TEvChannelData::TPtr& ev) {
        TEvComputeChannelDataOOB record;
        record.Proto = std::move(ev->Get()->Record);
        if (record.Proto.GetChannelData().GetData().HasPayloadId()) {
            record.Payload = ev->Get()->GetPayload(record.Proto.GetChannelData().GetData().GetPayloadId());
        }

        const auto& channelData = record.Proto.GetChannelData();

        ComputeActor = ev->Sender;

        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ", got result"
            << ", channelId: " << channelData.GetChannelId()
            << ", seqNo: " << record.Proto.GetSeqNo()
            << ", from: " << ev->Sender);

        SendResults(record, ev->Sender);
    }

    void HandleWork(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        ui64 seqNo = ev->Get()->Record.GetSeqNo();
        i64 freeSpace = ev->Get()->Record.GetFreeSpace();

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

    void InternalError(const TString& msg, const NYql::NDqProto::StatusIds_StatusCode& code = NYql::NDqProto::StatusIds::INTERNAL_ERROR) {
        LOG_CRIT_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER, msg);

        auto evAbort = MakeHolder<TEvKqp::TEvAbortExecution>(code, msg);
        Send(Executer, evAbort.Release());

        Become(&TResultCommonChannelProxy::DeadState);
    }

private:
    STATEFN(DeadState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvPoison, HandlePoison);
            }

        } catch(const yexception& ex) {
            InternalError(ex.what());
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
    const NActors::TActorId Executer;
    NActors::TActorId ComputeActor;
};

class TResultStreamChannelProxy : public TResultCommonChannelProxy {
public:
    TResultStreamChannelProxy(ui64 txId, ui64 channelId, NKikimr::NMiniKQL::TType* itemType,
        const TVector<ui32>* columnOrder, ui32 queryResultIndex, TActorId target,
        TActorId executer, size_t statementResultIndex)
        : TResultCommonChannelProxy(txId, channelId, executer)
        , ColumnOrder(columnOrder)
        , ItemType(itemType)
        , QueryResultIndex(queryResultIndex)
        , Target(target)
        , StatementResultIndex(statementResultIndex) {}

private:
    void SendResults(TEvComputeChannelDataOOB& computeData, TActorId sender) override {
        Y_UNUSED(sender);

        TVector<NYql::NDq::TDqSerializedBatch> batches(1);
        auto& batch = batches.front();

        batch.Proto = std::move(*computeData.Proto.MutableChannelData()->MutableData());
        batch.Payload = std::move(computeData.Payload);

        TKqpProtoBuilder protoBuilder{*AppData()->FunctionRegistry};
        auto resultSet = protoBuilder.BuildYdbResultSet(std::move(batches), ItemType, ColumnOrder);

        auto streamEv = MakeHolder<TEvKqpExecuter::TEvStreamData>();
        streamEv->Record.SetSeqNo(computeData.Proto.GetSeqNo());
        streamEv->Record.SetQueryResultIndex(QueryResultIndex + StatementResultIndex);
        streamEv->Record.MutableResultSet()->Swap(&resultSet);

        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER,
            "Send TEvStreamData to " << Target << ", seqNo: " << streamEv->Record.GetSeqNo()
            << ", nRows: " << batch.RowCount() );

        Send(Target, streamEv.Release());
    }

private:
    const TVector<ui32>* ColumnOrder;
    NKikimr::NMiniKQL::TType* ItemType;
    ui32 QueryResultIndex = 0;
    const NActors::TActorId Target;
    size_t StatementResultIndex;
};

class TResultDataChannelProxy : public TResultCommonChannelProxy {
public:
    TResultDataChannelProxy(ui64 txId, ui64 channelId, TActorId executer,
        ui32 inputIndex, TEvKqpExecuter::TEvTxResponse* resultReceiver)
        : TResultCommonChannelProxy(txId, channelId, executer)
        , InputIndex(inputIndex)
        , ResultReceiver(resultReceiver) {}

private:
    virtual void SendResults(TEvComputeChannelDataOOB& computeData, TActorId sender) {
        NYql::NDq::TDqSerializedBatch batch;
        batch.Proto = std::move(*computeData.Proto.MutableChannelData()->MutableData());
        batch.Payload = std::move(computeData.Payload);

        auto channelId = computeData.Proto.GetChannelData().GetChannelId();

        ResultReceiver->TakeResult(InputIndex, std::move(batch));

        auto ackEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelDataAck>();

        ackEv->Record.SetSeqNo(computeData.Proto.GetSeqNo());
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
    const TVector<ui32>* columnOrder, ui32 queryResultIndex, TActorId target,
    TActorId executer, ui32 statementResultIndex)
{
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER,
        "CreateResultStreamChannelProxy: TxId: " << txId <<
        ", channelId: " << channelId
    );

    return new TResultStreamChannelProxy(txId, channelId, itemType, columnOrder, queryResultIndex, target,
        executer, statementResultIndex);
}

NActors::IActor* CreateResultDataChannelProxy(ui64 txId, ui64 channelId, TActorId executer,
    ui32 inputIndex, TEvKqpExecuter::TEvTxResponse* resultsReceiver)
{
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER,
        "CreateResultDataChannelProxy: TxId: " << txId <<
        ", channelId: " << channelId
    );

    return new TResultDataChannelProxy(txId, channelId, executer, inputIndex, resultsReceiver);
}

} // namespace NKqp
} // namespace NKikimr
