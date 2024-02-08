#include "lock_handle.h"
#include "events.h"

#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {
namespace NLongTxService {

    void TLockHandle::Register(ui64 lockId, TActorSystem* as) noexcept {
        Y_ABORT_UNLESS(lockId, "Cannot register a zero lock id");
        Y_ABORT_UNLESS(as, "Cannot register without a valid actor system");
        as->Send(MakeLongTxServiceID(as->NodeId), new TEvLongTxService::TEvRegisterLock(lockId));
    }

    void TLockHandle::Unregister(ui64 lockId, TActorSystem* as) noexcept {
        Y_ABORT_UNLESS(lockId, "Cannot unregister a zero lock id");
        Y_ABORT_UNLESS(as, "Cannot unregister without a valid actor system");
        as->Send(MakeLongTxServiceID(as->NodeId), new TEvLongTxService::TEvUnregisterLock(lockId));
    }

} // namespace NLongTxService
} // namespace NKikimr
