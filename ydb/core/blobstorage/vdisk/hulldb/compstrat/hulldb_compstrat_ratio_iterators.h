#pragma once

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

namespace NKikimr::NHullComp {

    // Invoke the calculation synchronously, keeping the owning iterators alive.
    // Tests can supply a factory that decorates these same iterators.
    template <class TKey, class TMemRec>
    struct TStorageRatioIteratorFactory {
        using TLevelSnapshot = TLevelIndexSnapshot<TKey, TMemRec>;
        using TSst = TLevelSegment<TKey, TMemRec>;

        template <class TCalculate>
        decltype(auto) WithSstIterators(
                const THullCtxPtr& hullCtx,
                const TLevelSnapshot& snapshot,
                const TIntrusivePtr<TSst>& sst,
                const TCalculate& calculate) const
        {
            typename TSst::TMemIterator subsIt(sst.Get());
            typename TLevelSnapshot::TForwardIterator dbIt(hullCtx, &snapshot);
            return calculate(subsIt, dbIt);
        }

        template <class TCalculate>
        decltype(auto) WithHeapIterator(
                const THullCtxPtr& hullCtx,
                const TLevelSnapshot& snapshot,
                const TCalculate& calculate) const
        {
            // dbIt owns the leaves; only heapIt traverses them.
            typename TLevelSnapshot::TForwardIterator dbIt(hullCtx, &snapshot);
            THeapIterator<TKey, TMemRec, true> heapIt(&dbIt);
            return calculate(heapIt);
        }
    };

} // NKikimr::NHullComp
