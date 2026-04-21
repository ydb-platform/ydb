#include "events.h"

Y_DECLARE_OUT_SPEC(, NKikimr::NLongTxService::TEvLongTxService::TEvWaitingLockAdd, o, ev) {
    o << "{ RequestId: " << ev.RequestId
        << ", Lock: " << ev.Lock
        << ", OtherLock: " << ev.OtherLock
        << " }";
}

Y_DECLARE_OUT_SPEC(, NKikimr::NLongTxService::TEvLongTxService::TEvWaitingLockRemove, o, ev) {
    o << "{ RequestId: " << ev.RequestId << " }";
}
