#include "scheduler_cookie.h"

namespace NActors {
    class TSchedulerCookie2Way: public ISchedulerCookie {
        std::atomic<i64> Value;

    public:
        TSchedulerCookie2Way()
            : Value(2)
        {
        }

        bool IsArmed() noexcept override {
            return Value.load(std::memory_order_acquire) == 2;
        }

        bool Detach() noexcept override {
            const ui64 x = Value.fetch_sub(1, std::memory_order_seq_cst) - 1;
            if (x == 1)
                return true;

            if (x == 0) {
                delete this;
                return false;
            }

            Y_ABORT();
        }

        bool DetachEvent() noexcept override {
            Y_ABORT();
        }
    };

    ISchedulerCookie* ISchedulerCookie::Make2Way() {
        return new TSchedulerCookie2Way();
    }

    class TSchedulerCookie3Way: public ISchedulerCookie {
        std::atomic<i64> Value;

    public:
        TSchedulerCookie3Way()
            : Value(3)
        {
        }

        bool IsArmed() noexcept override {
            return Value.load(std::memory_order_acquire) == 3;
        }

        bool Detach() noexcept override {
            const ui64 x = Value.fetch_sub(1, std::memory_order_seq_cst) - 1;
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
            const ui64 x = Value.fetch_sub(1, std::memory_order_seq_cst) - 1;
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
