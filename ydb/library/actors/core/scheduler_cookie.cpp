#include "scheduler_cookie.h"

namespace NActors {
    bool NSchedulerCookie::TTwoWayCookie::IsArmed() noexcept {
        return AtomicGet(Owners) == 2;
    }

    bool NSchedulerCookie::TTwoWayCookie::Detach() noexcept {
        const ui64 remaining = AtomicDecrement(Owners);
        Y_ABORT_UNLESS(remaining <= 1);
        if (remaining == 0) {
            Destroy();
        }
        return remaining == 1;
    }

    bool NSchedulerCookie::TTwoWayCookie::DetachEvent() noexcept {
        Y_ABORT();
    }

    namespace {
        class TStandaloneTwoWayCookie final : public NSchedulerCookie::TTwoWayCookie {
            void Destroy() noexcept override {
                delete this;
            }
        };
    }

    ISchedulerCookie* ISchedulerCookie::Make2Way() {
        return new TStandaloneTwoWayCookie();
    }

    class TSchedulerCookie3Way: public ISchedulerCookie {
        TAtomic Value;

    public:
        TSchedulerCookie3Way()
            : Value(3)
        {
        }

        bool IsArmed() noexcept override {
            return (AtomicGet(Value) == 3);
        }

        bool Detach() noexcept override {
            const ui64 x = AtomicDecrement(Value);
            if (x == 2)
                return true;
            if (x == 1)
                return false;
            if (x == 0) {
                delete this;
                return false;
            }

            Y_ABORT();
        }

        bool DetachEvent() noexcept override {
            const ui64 x = AtomicDecrement(Value);
            if (x == 2)
                return false;
            if (x == 1)
                return true;
            if (x == 0) {
                delete this;
                return false;
            }

            Y_ABORT();
        }
    };

    ISchedulerCookie* ISchedulerCookie::Make3Way() {
        return new TSchedulerCookie3Way();
    }
}
