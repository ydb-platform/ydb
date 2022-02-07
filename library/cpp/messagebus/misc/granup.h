#pragma once

#include <util/datetime/base.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>

namespace NBus {
    template <typename TItem, typename TLocker = TSpinLock>
    class TGranUp {
    public:
        TGranUp(TDuration gran)
            : Gran(gran)
            , Next(TInstant::MicroSeconds(0))
        {
        }

        template <typename TFunctor>
        void Update(TFunctor functor, TInstant now, bool force = false) {
            if (force || now > Next)
                Set(functor(), now);
        }

        void Update(const TItem& item, TInstant now, bool force = false) {
            if (force || now > Next)
                Set(item, now);
        }

        TItem Get() const noexcept {
            TGuard<TLocker> guard(Lock);

            return Item;
        }

    protected:
        void Set(const TItem& item, TInstant now) {
            TGuard<TLocker> guard(Lock);

            Item = item;

            Next = now + Gran;
        }

    private:
        const TDuration Gran;
        TLocker Lock;
        TItem Item;
        TInstant Next;
    };
}
