#include "mlp_common.h"

#include <ydb/public/lib/base/msgbus_status.h>

namespace NKikimr::NPQ::NMLP {

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
    const NKikimrPQ::TPQTabletConfig::TConsumer consumer,
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
