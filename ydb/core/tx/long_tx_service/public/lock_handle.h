#pragma once
#include <util/system/compiler.h>
#include <util/system/types.h>

namespace NActors {

    // Avoid include dependency by forward declaring TActorSystem
    class TActorSystem;

} // namespace NActors

namespace NKikimr {
namespace NLongTxService {

    class TLockHandle {
    public:
        TLockHandle() noexcept
            : LockId(0)
            , ActorSystem(nullptr)
        { }

        TLockHandle(ui64 lockId, NActors::TActorSystem* as) noexcept
            : LockId(lockId)
            , ActorSystem(as)
        {
            if (LockId) {
                Register(LockId, ActorSystem);
            }
        }

        TLockHandle(TLockHandle&& rhs) noexcept
            : LockId(rhs.LockId)
            , ActorSystem(rhs.ActorSystem)
        {
            rhs.LockId = 0;
            rhs.ActorSystem = nullptr;
        }

        ~TLockHandle() noexcept {
            if (LockId) {
                Unregister(LockId, ActorSystem);
                LockId = 0;
            }
        }

        TLockHandle& operator=(TLockHandle&& rhs) noexcept {
            if (Y_LIKELY(this != &rhs)) {
                if (LockId) {
                    Unregister(LockId, ActorSystem);
                }
                LockId = rhs.LockId;
                ActorSystem = rhs.ActorSystem;
                rhs.LockId = 0;
                rhs.ActorSystem = nullptr;
            }
            return *this;
        }

        explicit operator bool() const noexcept {
            return bool(LockId);
        }

        ui64 GetLockId() const noexcept {
            return LockId;
        }

    private:
        static void Register(ui64 lockId, NActors::TActorSystem* as) noexcept;
        static void Unregister(ui64 lockId, NActors::TActorSystem* as) noexcept;

    private:
        ui64 LockId;
        NActors::TActorSystem* ActorSystem;
    };

} // namespace NLongTxService
} // namespace NKikimr
