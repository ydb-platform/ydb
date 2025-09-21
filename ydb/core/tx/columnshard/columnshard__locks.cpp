#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

void TColumnShard::SubscribeLock(const ui64 lockId, const ui32 lockNodeId) {
    Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()),
        new NLongTxService::TEvLongTxService::TEvSubscribeLock(
            lockId,
            lockNodeId));
}

void TColumnShard::Handle(NLongTxService::TEvLongTxService::TEvLockStatus::TPtr& ev, const TActorContext& /*ctx*/) {
    auto* msg = ev->Get();
    const ui64 lockId = msg->Record.GetLockId();
    switch (msg->Record.GetStatus()) {
        case NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND:
        case NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE:
            GetOperationsManager().AbortLock(*this, lockId);
            break;

        default:
            break;
    }
}

} // namespace NKikimr::NDataShard