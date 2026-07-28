#pragma once

#include "hulldb_compstrat_ratio_iterators.h"
#include <util/stream/output.h>

#include <type_traits>
#include <utility>

namespace NKikimr::NHullComp::NTesting {

    struct TCalcStat {
        // Counts one key for each PutToMerger[AndAdvance] on a DB iterator.
        ui64 KeysProcessed = 0;
        // SST records delivered to a source merger or the batch merger.
        ui64 SourceRecordsProcessed = 0;
        // SST and Fresh records delivered to a whole-DB merger.
        ui64 DbRecordsMerged = 0;
        ui64 Seeks = 0;
        ui64 DbIteratorNexts = 0;
    };

    enum class EIteratorKind {
        Sst,
        Db,
        Heap,
    };

    // Count records delivered to the real merger, without inspecting its
    // internal state or merging anything a second time.
    template <class TMerger, EIteratorKind Kind>
    class TCountingMerger {
        TMerger& Merger;
        TCalcStat& Stat;

    public:
        TCountingMerger(TMerger& merger, TCalcStat& stat)
            : Merger(merger)
            , Stat(stat)
        {}

        template <class... TArgs>
        void AddFromSegment(TArgs&&... args) {
            Merger.AddFromSegment(std::forward<TArgs>(args)...);
            if constexpr (Kind != EIteratorKind::Sst) {
                ++Stat.DbRecordsMerged;
            }
            if constexpr (Kind != EIteratorKind::Db) {
                ++Stat.SourceRecordsProcessed;
            }
        }

        template <class... TArgs>
        void AddFromFresh(TArgs&&... args) {
            Merger.AddFromFresh(std::forward<TArgs>(args)...);
            ++Stat.DbRecordsMerged;
        }

        bool HaveToMergeData() const {
            return Merger.HaveToMergeData();
        }
    };

    // Observe the existing iterator.
    template <class TIterator, EIteratorKind Kind>
    class TCountingIterator {
        TIterator& Iterator;
        TCalcStat& Stat;

    public:
        TCountingIterator(TIterator& iterator, TCalcStat& stat)
            : Iterator(iterator)
            , Stat(stat)
        {}

        bool Valid() const {
            return Iterator.Valid();
        }

        decltype(auto) GetCurKey() const {
            return Iterator.GetCurKey();
        }

        void SeekToFirst() {
            Iterator.SeekToFirst();
        }

        template <class TKey>
        void Seek(const TKey& key) {
            if constexpr (Kind != EIteratorKind::Sst) {
                ++Stat.Seeks;
            }
            Iterator.Seek(key);
        }

        void Next() {
            if constexpr (Kind != EIteratorKind::Sst) {
                ++Stat.DbIteratorNexts;
            }
            Iterator.Next();
        }

        template <class TMerger>
        void PutToMerger(TMerger* merger) {
            TCountingMerger<TMerger, Kind> counted(*merger, Stat);
            Iterator.PutToMerger(&counted);
            if constexpr (Kind != EIteratorKind::Sst) {
                ++Stat.KeysProcessed;
            }
        }

        template <class TMerger>
        void PutToMergerAndAdvance(TMerger* merger) {
            static_assert(Kind == EIteratorKind::Heap);
            TCountingMerger<TMerger, Kind> counted(*merger, Stat);
            Iterator.PutToMergerAndAdvance(&counted);
            ++Stat.KeysProcessed;
            ++Stat.DbIteratorNexts;
        }

        template <class TExtractor>
        auto GetDiskData(TExtractor* extractor) const {
            return Iterator.GetDiskData(extractor);
        }

        void DumpAll(IOutputStream& out) const {
            Iterator.DumpAll(out);
        }
    };

    // Reuse production iterator construction and lifetimes; only decorate the
    // references passed to the calculation. No scheduling or scan is duplicated.
    template <class TKey, class TMemRec>
    class TCountingIteratorFactory : public TStorageRatioIteratorFactory<TKey, TMemRec> {
        using TBase = TStorageRatioIteratorFactory<TKey, TMemRec>;
        TCalcStat& Stat;

    public:
        explicit TCountingIteratorFactory(TCalcStat& stat)
            : Stat(stat)
        {}

        template <class TCalculate>
        decltype(auto) WithSstIterators(
                const THullCtxPtr& hullCtx,
                const typename TBase::TLevelSnapshot& snapshot,
                const TIntrusivePtr<typename TBase::TSst>& sst,
                const TCalculate& calculate) const
        {
            return TBase::WithSstIterators(hullCtx, snapshot, sst, [&](auto& subsIt, auto& dbIt) {
                TCountingIterator<std::decay_t<decltype(subsIt)>, EIteratorKind::Sst> subs(subsIt, Stat);
                TCountingIterator<std::decay_t<decltype(dbIt)>, EIteratorKind::Db> db(dbIt, Stat);
                return calculate(subs, db);
            });
        }

        template <class TCalculate>
        decltype(auto) WithHeapIterator(
                const THullCtxPtr& hullCtx,
                const typename TBase::TLevelSnapshot& snapshot,
                const TCalculate& calculate) const
        {
            return TBase::WithHeapIterator(hullCtx, snapshot, [&](auto& heapIt) {
                TCountingIterator<std::decay_t<decltype(heapIt)>, EIteratorKind::Heap> heap(heapIt, Stat);
                return calculate(heap);
            });
        }
    };

} // NKikimr::NHullComp::NTesting
