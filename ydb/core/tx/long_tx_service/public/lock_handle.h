#pragma once
#include <util/datetime/base.h>
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

        TLockHandle(ui64 lockId, NActors::TActorSystem* as, TInstant lockTimestamp = TInstant::Zero()) noexcept
            : LockId(lockId)
            , ActorSystem(as)
            , LockTimestamp(lockTimestamp)
        {
            if (LockId) {
                Register(LockId, ActorSystem, LockTimestamp);
            }
        }

        TLockHandle(TLockHandle&& rhs) noexcept
            : LockId(rhs.LockId)
            , ActorSystem(rhs.ActorSystem)
            , LockTimestamp(rhs.LockTimestamp)
        {
            rhs.LockId = 0;
            rhs.ActorSystem = nullptr;
            rhs.LockTimestamp = TInstant::Zero();
        }

        ~TLockHandle() noexcept {
            Reset();
        }

        TLockHandle& operator=(TLockHandle&& rhs) noexcept {
            if (Y_LIKELY(this != &rhs)) {
                Reset();
                LockId = rhs.LockId;
                ActorSystem = rhs.ActorSystem;
                LockTimestamp = rhs.LockTimestamp;
                rhs.LockId = 0;
                rhs.ActorSystem = nullptr;
                rhs.LockTimestamp = TInstant::Zero();
            }
            return *this;
        }

        explicit operator bool() const noexcept {
            return bool(LockId);
        }

        void Reset() noexcept {
            if (LockId) {
                Unregister(LockId, ActorSystem);
                LockId = 0;
                ActorSystem = nullptr;
                LockTimestamp = TInstant::Zero();
            }
        }

        ui64 GetLockId() const noexcept {
            return LockId;
        }

        ui32 GetLockNodeId() const noexcept {
            return ActorSystem ? GetNodeId(ActorSystem) : 0;
        }

        TInstant GetLockTimestamp() const noexcept {
            return LockTimestamp;
        }

    private:
        static ui32 GetNodeId(NActors::TActorSystem* as) noexcept;
        static void Register(ui64 lockId, NActors::TActorSystem* as, TInstant lockTimestamp) noexcept;
        static void Unregister(ui64 lockId, NActors::TActorSystem* as) noexcept;

    private:
        ui64 LockId;
        NActors::TActorSystem* ActorSystem;
        TInstant LockTimestamp;
    };

} // namespace NLongTxService
} // namespace NKikimr
