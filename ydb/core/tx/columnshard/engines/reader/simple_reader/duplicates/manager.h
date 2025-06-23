#pragma once

#include "events.h"
#include "private_events.h"
#include "source_cache.h"
#include "splitter.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/default_fetching.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TSpecialReadContext;
class IDataSource;
class TPortionDataSource;
class TColumnFetchingContext;

class TFilterConstructor {
private:
    class TRowRange {
    private:
        YDB_READONLY_DEF(ui64, Begin);
        YDB_READONLY_DEF(ui64, End);

    public:
        TRowRange(const ui64 begin, const ui64 end)
            : Begin(begin)
            , End(end) {
            AFL_VERIFY(end >= begin);
        }

        std::partial_ordering operator<=>(const TRowRange& other) const {
            return std::tie(Begin, End) <=> std::tie(other.Begin, other.End);
        }
        bool operator==(const TRowRange& other) const {
            return (*this <=> other) == std::partial_ordering::equivalent;
        }
    };

private:
    std::shared_ptr<IFilterSubscriber> Callback;
    ui64 RowsCount;
    std::map<TRowRange, NArrow::TColumnFilter> FiltersByRange;

    bool IsReady() const {
        return !FiltersByRange.empty() && FiltersByRange.begin()->first.GetEnd() >= RowsCount;
    }

    void Complete() {
        AFL_VERIFY(!IsDone());
        AFL_VERIFY(IsReady());
        Callback->OnFilterReady(FiltersByRange.begin()->second);
        AFL_VERIFY(IsDone());
    }

public:
    void AddFilter(const TDuplicateMapInfo& info, const NArrow::TColumnFilter& filter) {
        FiltersByRange.emplace(TRowRange(info.GetOffset(), info.GetOffset() + info.GetRowsCount()), filter);

        while (FiltersByRange.size() > 1 && FiltersByRange.begin()->first.GetEnd() >= std::next(FiltersByRange.begin())->first.GetBegin()) {
            auto l = FiltersByRange.begin();
            auto r = std::next(FiltersByRange.begin());
            TRowRange range = TRowRange(l->first.GetBegin(), r->first.GetEnd());
            NArrow::TColumnFilter filter = l->second;
            filter.Append(r->second.Slice(r->first.GetBegin() - l->first.GetEnd(), r->first.GetEnd() - l->first.GetEnd()));
            FiltersByRange.erase(FiltersByRange.begin());
            FiltersByRange.erase(FiltersByRange.begin());
            FiltersByRange.emplace(range, std::move(filter));
        }

        if (IsReady()) {
            Complete();
        }
    }

    bool IsDone() const {
        return !!Callback;
    }

    TFilterConstructor(const std::shared_ptr<IFilterSubscriber>& callback, const std::shared_ptr<IDataSource>& source);
};

class TEvConstructFilters: public NActors::TEventLocal<TEvConstructFilters, NColumnShard::TEvPrivate::EvConstructFilters> {
private:
    using TDataBySource = THashMap<ui64, TSourceCache::TCacheItem>;
    YDB_READONLY_DEF(std::shared_ptr<IDataSource>, Source);
    YDB_READONLY_DEF(std::shared_ptr<TFilterConstructor>, Callback);
    YDB_READONLY_DEF(TDataBySource, ColumnData);
    TColumnDataSplitter Splitter;

public:
    TEvConstructFilters(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<TFilterConstructor>& callback, TDataBySource&& data,
        TColumnDataSplitter&& splitter)
        : Source(source)
        , Callback(callback)
        , ColumnData(std::move(data))
        , Splitter(std::move(splitter)) {
    }

    const TColumnDataSplitter& GetSplitter() const {
        return Splitter;
    }
};

class TEvFiltersConstructed: public NActors::TEventLocal<TEvFiltersConstructed, NColumnShard::TEvPrivate::EvFiltersConstructed> {
private:
    using TFilters = THashMap<TDuplicateMapInfo, NArrow::TColumnFilter>;
    YDB_READONLY_DEF(TFilters, Result);

public:
    TEvFiltersConstructed(TFilters&& result)
        : Result(std::move(result)) {
    }
};

class TDuplicateFilterConstructor: public NActors::TActor<TDuplicateFilterConstructor> {
private:
    class TSourceInfo {
    private:
        ui64 SourceIdx;
        std::shared_ptr<TPortionInfo> PortionInfo;

    public:
        TSourceInfo(const ui64 sourceIdx, const std::shared_ptr<TPortionInfo>& portionInfo)
            : SourceIdx(sourceIdx)
            , PortionInfo(portionInfo) {
        }

        std::shared_ptr<TPortionDataSource> Construct(const std::shared_ptr<TSpecialReadContext>& context) const;
    };

    class TSourceDataSubscriber: public TSourceCache::ISubscriber {
    private:
        TActorId Owner;
        std::shared_ptr<IDataSource> Source;
        std::shared_ptr<TFilterConstructor> Callback;
        TColumnDataSplitter Splitter;

        virtual void OnSourcesReady(TSourceCache::TSourcesData&& result) override;
        virtual void OnFailure(const TString& error) override {
            Y_UNUSED(error);   // FIXME
            TActivationContext::AsActorContext().Send(Owner, new NActors::TEvents::TEvPoison);
        }

    public:
        TSourceDataSubscriber(const TActorId& owner, const std::shared_ptr<IDataSource>& source,
            const std::shared_ptr<TFilterConstructor>& callback, TColumnDataSplitter&& splitter)
            : Owner(owner)
            , Source(source)
            , Callback(callback)
            , Splitter(std::move(splitter)) {
        }
    };

    class TFilterResultSubscriber: public TBuildDuplicateFilters::ISubscriber {
    private:
        TActorId Owner;
        THashMap<ui64, TDuplicateMapInfo> InfoBySource;

        virtual void OnResult(THashMap<ui64, NArrow::TColumnFilter>&& result) override;
        virtual void OnFailure(const TString& error) override {
            Y_UNUSED(error);   // FIXME
            TActivationContext::AsActorContext().Send(Owner, new NActors::TEvents::TEvPoison);
        }

    public:
        TFilterResultSubscriber(const TActorId& owner, THashMap<ui64, TDuplicateMapInfo>&& intervals)
            : Owner(owner)
            , InfoBySource(std::move(intervals)) {
        }
    };

private:
    TSourceCache* SourceCache;
    const TIntervalTree<TInterval<NArrow::TSimpleRow>, TSourceInfo> Intervals;
    TLRUCache<TDuplicateMapInfo, NArrow::TColumnFilter> FiltersCache;
    THashMap<TDuplicateMapInfo, std::vector<std::shared_ptr<TFilterConstructor>>> BuildingFilters;

private:
    TSourceCache Fetcher;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NPrivate::TEvFilterConstructionResult, Handle);
            hFunc(NPrivate::TEvDuplicateFilterDataFetched, Handle);
            hFunc(NPrivate::TEvDuplicateSourceCacheResult, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(TEvConstructFilters, Handle);
            hFunc(TEvFiltersConstructed, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const NPrivate::TEvFilterConstructionResult::TPtr&);
    void Handle(const NPrivate::TEvDuplicateFilterDataFetched::TPtr&);
    void Handle(const NPrivate::TEvDuplicateSourceCacheResult::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void AbortAndPassAway(const TString& reason);
    void StartAllocation(const ui64 sourceId, const std::shared_ptr<IDataSource>& requester);
    void StartMergingColumns(const ui32 intervalIdx);

public:
    TDuplicateFilterConstructor(const TSpecialReadContext& context);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
