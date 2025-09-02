#pragma once

#include "resource.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/vector.h>

namespace NShop {

///////////////////////////////////////////////////////////////////////////////

struct TConsumerCounters {
    TAtomic Consumed = 0; // in TCost units (deriv)
    TAtomic Borrowed = 0; // in TCost units (deriv)
    TAtomic Donated = 0; // in TCost units (deriv)
    TAtomic Usage = 0; // in promiles (as_is)
};

///////////////////////////////////////////////////////////////////////////////

template <class TCons, class TTag>
class TCountersAggregator {
private:
    struct TData {
        TConsumerCounters* Counters;
        TWeight Weight;
        TTag Consumed;

        explicit TData(TCons* consumer)
            : Counters(consumer->GetCounters())
            , Weight(consumer->GetWeight())
            , Consumed(consumer->ResetConsumed())
        {}
    };

    TVector<TData> Consumers;
    TTag ConsumedRest = TTag();
    TTag UsedTotal = TTag();
    TWeight WeightTotal = 0;

public:
    void Add(TCons* consumer)
    {
        Consumers.emplace_back(consumer);
        TData& d = Consumers.back();
        ConsumedRest += d.Consumed;
        UsedTotal += d.Consumed * d.Weight;
        WeightTotal += d.Weight;
    }

    void Apply()
    {
        if (WeightTotal == 0) {
            return;
        }

        Sort(Consumers, [] (const TData& lhs, const TData& rhs) {
            return lhs.Consumed < rhs.Consumed;
        });

        TTag usagePrev = TTag();
        TTag consumedPrev = TTag();
        size_t consumersRest = Consumers.size();
        TTag fairShare = UsedTotal / TTag(WeightTotal);
        for (TData& d : Consumers) {
            TTag borrowed = (d.Consumed - fairShare) * d.Weight;
            AtomicAdd(d.Counters->Consumed, TAtomicBase(d.Consumed * d.Weight));
            if (borrowed > 0) {
                AtomicAdd(d.Counters->Borrowed, TAtomicBase(borrowed));
            } else {
                AtomicAdd(d.Counters->Donated, TAtomicBase(-borrowed));
            }

            // EXPLANATION FOR USAGE FORMULA
            // If we assume:
            //   (1) all "vectors" below are sorted by consumption c[k]
            //   (2) u[k] >= u[k-1] -- u should monotonically increase with k
            //   (3) u[k] must be bounded by 1
            //   (4) the last u[n] must be equal to 1
            //   (5) u[k] must be a continuous function of every c[k]
            //
            // One can proove that the following formula satisfies these requirements:
            //                     c[k] - c[k-1]
            //    u[k] = u[k-1] + ----------------- (1 - u[k-1])
            //                     L[k] - c[k-1]
            //
            //  , where L[k] = sum(i=k..n, c[i]) / (n - k + 1)
            //          L[k] is max possible value for c[k] (due to sort by c[k])

            if (ConsumedRest > 1e-6) {
                TTag avgConsumed = ConsumedRest / consumersRest;
                if (avgConsumed > d.Consumed) {
                    usagePrev += (d.Consumed - consumedPrev) / (avgConsumed - d.Consumed) * (1.0 - usagePrev);
                }
            }
            if (usagePrev > 1.0) {
                usagePrev = 1.0; // just to account for fp-errors
            }
            consumedPrev = d.Consumed;
            ConsumedRest -= d.Consumed;
            consumersRest--;
            AtomicSet(d.Counters->Usage, TAtomicBase(usagePrev * 1000.0));
        }
    }
};

}
