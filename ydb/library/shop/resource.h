#pragma once

#include <util/generic/utility.h>
#include <util/system/types.h>

#include <limits>
#include <utility>

namespace NShop {

using TWeight = double;

// Single resource max-min fairness allocation policy
template <class TFloat>
struct TSingleResourceTempl {
    using TCost = i64;
    using TTag = TFloat;
    using TKey = TFloat;

    inline static TKey OffsetKey(TKey key, TTag offset)
    {
        return key + offset;
    }

    template <class ConsumerT>
    inline static void SetKey(TKey& key, ConsumerT* consumer)
    {
        key = consumer->GetTag();
    }

    inline static TTag GetTag(const TKey& key)
    {
        return key;
    }

    inline static TKey MaxKey()
    {
        return std::numeric_limits<TKey>::max();
    }

    template <class ConsumerT>
    inline static TKey ZeroKey(ConsumerT* consumer)
    {
        Y_UNUSED(consumer);
        return 0;
    }

    inline static void ActivateKey(TKey& key, TTag vtime)
    {
        key = Max(key, vtime);
    }
};

using TSingleResource = TSingleResourceTempl<double>;
using TSingleResourceDense = TSingleResourceTempl<float>; // for lazy scheduler

// Dominant resource fairness queueing (DRFQ) allocation policy
// with two resources and perfect dovetailing
struct TPairDrf {
    using TCost = std::pair<i64, i64>;
    using TTag = std::pair<double, double>;
    using TKey = double; // dominant resource

    inline static TKey OffsetKey(TKey key, TTag offset)
    {
        return key + Dominant(offset);
    }

    template <class ConsumerT>
    inline static void SetKey(TKey& key, ConsumerT* consumer)
    {
        key = Dominant(consumer->GetTag());
    }

    inline static TKey Dominant(TTag tag)
    {
        return Max(tag.first, tag.second);
    }

    inline static TKey MaxKey()
    {
        return std::numeric_limits<TKey>::max();
    }

    template <class ConsumerT>
    inline static TKey ZeroKey(ConsumerT* consumer)
    {
        Y_UNUSED(consumer);
        return 0;
    }

    inline static void ActivateKey(TKey& key, TTag vtime)
    {
        key = Max(key, Dominant(vtime));
    }
};

}
