#pragma once

#include "defs.h"
#include "time_series.h"

namespace NKikimr {

    template<typename T>
    class TSpeedTracker : public TTimeSeries<T>
    {
        using TItem = typename TTimeSeries<T>::TItem;
        using TTimeSeries<T>::Items;

    public:
        using TTimeSeries<T>::TTimeSeries;

        bool CalculateSpeed(T *res) const {
            if (Items.empty()) {
                *res = T();
                return false;
            }

            const TItem& front = Items.front();
            const TItem& back = Items.back();
            if (front.Timestamp != back.Timestamp) {
                *res = (back.Value - front.Value) * T(1000000) / (back.Timestamp - front.Timestamp).MicroSeconds();
                return true;
            } else {
                *res = T();
                return false;
            }
        }
    };

} // NKikimr
