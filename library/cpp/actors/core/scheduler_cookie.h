#pragma once

#include "defs.h"
#include <util/generic/noncopyable.h>

namespace NActors {
    class ISchedulerCookie : TNonCopyable {
    protected:
        virtual ~ISchedulerCookie() {
        }

    public:
        virtual bool Detach() noexcept = 0;
        virtual bool DetachEvent() noexcept = 0;
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
