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
        1000,
        std::numeric_limits<ui32>::max(),
        0,
        0,
        "unknown",
        false,
        TActorId{},
        selfId
    );
}

bool IsSucess(const TEvPQ::TEvProxyResponse::TPtr& ev) {
    return ev->Get()->Response->GetStatus() == NMsgBusProxy::MSTATUS_OK &&
        ev->Get()->Response->GetErrorCode() == NPersQueue::NErrorCode::OK;
}

}
