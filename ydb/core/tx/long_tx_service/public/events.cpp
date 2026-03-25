#include "events.h"

template <>
void Out<NKikimr::NLongTxService::TEvLongTxService::TEvWaitingLockAdd>(
        IOutputStream& o,
        const NKikimr::NLongTxService::TEvLongTxService::TEvWaitingLockAdd& ev) {
    o << "{ RequestId: " << ev.RequestId
        << ", Lock: " << ev.Lock
        << ", OtherLock: " << ev.OtherLock
        << " }";
}

template <>
void Out<NKikimr::NLongTxService::TEvLongTxService::TEvWaitingLockRemove>(
        IOutputStream& o,
        const NKikimr::NLongTxService::TEvLongTxService::TEvWaitingLockRemove& ev) {
    o << "{ RequestId: " << ev.RequestId << " }";
}
