#include "lock_handle.h"
#include "events.h"

#include <library/cpp/actors/core/actorsystem.h>

namespace NKikimr {
namespace NLongTxService {

    void TLockHandle::Register(ui64 lockId, TActorSystem* as) noexcept {
        Y_VERIFY(lockId, "Cannot register a zero lock id");
        Y_VERIFY(as, "Cannot register without a valid actor system");
        as->Send(MakeLongTxServiceID(as->NodeId), new TEvLongTxService::TEvRegisterLock(lockId));
    }

    void TLockHandle::Unregister(ui64 lockId, TActorSystem* as) noexcept {
        Y_VERIFY(lockId, "Cannot unregister a zero lock id");
        Y_VERIFY(as, "Cannot unregister without a valid actor system");
        as->Send(MakeLongTxServiceID(as->NodeId), new TEvLongTxService::TEvUnregisterLock(lockId));
    }

} // namespace NLongTxService
} // namespace NKikimr
