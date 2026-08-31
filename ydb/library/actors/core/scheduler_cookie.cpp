#include "scheduler_cookie.h"

namespace NActors {
    class TSchedulerCookie2Way: public ISchedulerCookie {
        TAtomic Value;

    public:
        TSchedulerCookie2Way()
            : ISchedulerCookie(2)
            , Value(2)
        {
        }

        bool IsArmed() noexcept override {
            return (AtomicGet(Value) == 2);
        }

        bool DetachImpl() noexcept override {
            const ui64 x = AtomicDecrement(Value);
            if (x == 1)
                return true;

            if (x == 0)
                return false;

            Y_ABORT();
        }

        bool DetachEventImpl() noexcept override {
            Y_ABORT();
        }
    };

    ISchedulerCookie* ISchedulerCookie::Make2Way() {
        return new TSchedulerCookie2Way();
    }

    class TSchedulerCookie3Way: public ISchedulerCookie {
        TAtomic Value;

    public:
        TSchedulerCookie3Way()
            : ISchedulerCookie(3)
            , Value(3)
        {
        }

        bool IsArmed() noexcept override {
            return (AtomicGet(Value) == 3);
        }

        bool DetachImpl() noexcept override {
            const ui64 x = AtomicDecrement(Value);
            if (x == 2)
                return true;
            if (x == 1 || x == 0)
                return false;

            Y_ABORT();
        }

        bool DetachEventImpl() noexcept override {
            const ui64 x = AtomicDecrement(Value);
            if (x == 2)
                return false;
            if (x == 1)
                return true;
            if (x == 0)
                return false;

            Y_ABORT();
        }
    };

    ISchedulerCookie* ISchedulerCookie::Make3Way() {
        return new TSchedulerCookie3Way();
    }
}
