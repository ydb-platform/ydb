#pragma once

#include "defs.h"

namespace NKikimr {

    template<typename T>
    class TTimeSeries {
        const TDuration MaxLifetime;

    protected:
        struct TItem {
            TMonotonic Timestamp;
            T Value;

            TItem(TMonotonic timestamp, T value)
                : Timestamp(timestamp)
                , Value(value)
            {}
        };
        TDeque<TItem> Items;

    public:
        using TValue = T;

        TTimeSeries(TDuration maxLifetime)
            : MaxLifetime(maxLifetime)
        {}

        void Add(TMonotonic timestamp, T value) {
            // ensure that timestamps are coming in nondecreasing order
            Y_ABORT_UNLESS(!Items || Items.back().Timestamp <= timestamp);

            // drop old entries
            auto comp = [](const TItem &x, TMonotonic y) { return x.Timestamp < y; };
            auto it = std::lower_bound(Items.begin(), Items.end(), timestamp - MaxLifetime, comp);
            Items.erase(Items.begin(), it);

            // add new one
            Items.emplace_back(timestamp, std::move(value));
        }
    };


} // NKikimr
