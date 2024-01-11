#pragma once

#include "defs.h"
#include <ydb/core/base/appdata.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NKikimr {

    template<typename TItem>
    class TGenerator {
        // generation result type
        using TResult = decltype(std::declval<TItem>().Generate());

        // a sef of items; key is the accumulated weight for this item (including the one for the item itself)
        TMap<double, TItem> Items;

        // accumulated weight
        double AccumWeight = 0;

    public:
        TGenerator() = default;

        template<typename T>
        TGenerator(const google::protobuf::RepeatedPtrField<T>& setting) {
            for (const auto& item : setting) {
                Y_ABORT_UNLESS(item.HasWeight());
                AccumWeight += item.GetWeight();
                Items.emplace(AccumWeight, item);
            }
        }

        TResult Generate() const {
            const double x = AccumWeight * TAppData::RandomProvider->GenRandReal2();
            auto it = Items.lower_bound(x); // min Key >= x
            if (it == Items.end()) {
                return TResult(); // no items in distribution
            }
            const auto& generator = it->second;
            return generator.Generate();
        }
    };

} // NKikimr
