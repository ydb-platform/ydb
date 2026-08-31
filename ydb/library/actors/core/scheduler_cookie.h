#pragma once

#include "defs.h"
#include <util/generic/noncopyable.h>

namespace NActors {
    class ISchedulerCookie : TNonCopyable {
        // Each logical participant owns one lifetime slot. Keep lifetime
        // accounting outside the virtual implementation so a concurrent
        // participant cannot destroy the cookie before DetachImpl() returns.
        TAtomic Lifetime;

        void Release() noexcept {
            if (AtomicDecrement(Lifetime) == 0) {
                delete this;
            }
        }

    protected:
        explicit ISchedulerCookie(ui64 lifetime)
            : Lifetime(lifetime)
        {
        }

        virtual ~ISchedulerCookie() {
        }

        virtual bool DetachImpl() noexcept = 0;
        virtual bool DetachEventImpl() noexcept = 0;

    public:
        bool Detach() noexcept {
            const bool result = DetachImpl();
            Release();
            return result;
        }

        bool DetachEvent() noexcept {
            const bool result = DetachEventImpl();
            Release();
            return result;
        }

        virtual bool IsArmed() noexcept = 0;

        static ISchedulerCookie* Make2Way();
        static ISchedulerCookie* Make3Way();
    };

    class TSchedulerCookieHolder : TNonCopyable {
        ISchedulerCookie* Cookie;

    public:
        TSchedulerCookieHolder()
            : Cookie(nullptr)
        {
        }

        TSchedulerCookieHolder(ISchedulerCookie* x)
            : Cookie(x)
        {
        }

        ~TSchedulerCookieHolder() {
            Detach();
        }

        bool operator==(const TSchedulerCookieHolder& x) const noexcept {
            return (Cookie == x.Cookie);
        }

        ISchedulerCookie* Get() const {
            return Cookie;
        }

        ISchedulerCookie* Release() {
            ISchedulerCookie* result = Cookie;
            Cookie = nullptr;
            return result;
        }

        void Reset(ISchedulerCookie* cookie) {
            Detach();
            Cookie = cookie;
        }

        bool Detach() noexcept {
            if (Cookie) {
                const bool res = Cookie->Detach();
                Cookie = nullptr;
                return res;
            } else {
                return false;
            }
        }

        bool DetachEvent() noexcept {
            if (Cookie) {
                const bool res = Cookie->DetachEvent();
                Cookie = nullptr;
                return res;
            } else {
                return false;
            }
        }
    };
}
