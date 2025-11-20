#include "mlp_common.h"

#include <ydb/public/lib/base/msgbus_status.h>

namespace NKikimr::NPQ::NMLP {

std::unique_ptr<TEvPersQueue::TEvRequest> MakeEvPQRead(
    const TString& consumerName,
    ui32 partitionId,
    ui64 startOffset,
    std::optional<ui64> count
) {
    auto request = std::make_unique<TEvPersQueue::TEvRequest>();

    auto* partitionRequest = request->Record.MutablePartitionRequest();
    partitionRequest->SetPartition(partitionId);
    auto* read = partitionRequest->MutableCmdRead();
    read->SetClientId(consumerName);
    read->SetOffset(startOffset);
    read->SetTimeoutMs(0);
    if (count) {
        read->SetCount(count.value());
    }

    return request;
}

std::unique_ptr<TEvPQ::TEvRead> MakeEvRead(
    const TActorId& selfId,
    const TString& consumerName,
    ui64 startOffset,
    ui64 count,
    ui64 cookie,
    ui64 nextPartNo
) {
    return std::make_unique<TEvPQ::TEvRead>(
        cookie,
        startOffset,
        startOffset + count,
        nextPartNo,
        count,
        TString{},
        consumerName,
        0,
        8_MB,
        0,
        0,
        "unknown",
        false,
        TActorId{},
        selfId
    );
}

std::unique_ptr<TEvPQ::TEvSetClientInfo> MakeEvCommit(
    const NKikimrPQ::TPQTabletConfig::TConsumer& consumer,
    ui64 offset,
    ui64 cookie
) {
    return std::make_unique<TEvPQ::TEvSetClientInfo>(
        cookie,
        consumer.GetName(),
        offset,
        TString{}, // sessionId
        0, // partitionSessionId
        consumer.GetGeneration(),
        0, // step
        TActorId{} // pipeClient
    );    
}

std::unique_ptr<TEvPersQueue::TEvHasDataInfo> MakeEvHasData(
    const TActorId& selfId,
    ui32 partitionId,
    ui64 offset,
    const NKikimrPQ::TPQTabletConfig::TConsumer& consumer
) {

    auto result = std::make_unique<TEvPersQueue::TEvHasDataInfo>();
    auto& record = result->Record;
    record.SetPartition(partitionId);
    record.SetOffset(offset);
    record.SetDeadline(TDuration::Seconds(5).MilliSeconds());
    ActorIdToProto(selfId, record.MutableSender());
    record.SetClientId(consumer.GetName());

    return result;
}

bool IsSucess(const TEvPQ::TEvProxyResponse::TPtr& ev) {
    return ev->Get()->Response->GetStatus() == NMsgBusProxy::MSTATUS_OK &&
        ev->Get()->Response->GetErrorCode() == NPersQueue::NErrorCode::OK;
}

bool IsSucess(const TEvPersQueue::TEvResponse::TPtr& ev) {
    return ev->Get()->Record.GetStatus() == NMsgBusProxy::MSTATUS_OK &&
        ev->Get()->Record.GetErrorCode() == NPersQueue::NErrorCode::OK;
}

ui64 GetCookie(const TEvPQ::TEvProxyResponse::TPtr& ev) {
    return ev->Get()->Response->GetCookie();
}

}
