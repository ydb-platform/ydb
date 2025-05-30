#pragma once

#include "events.h"
#include "interval_tree.h"
#include "merge.h"
#include "source_cache.h"
#include "splitter.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/default_fetching.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader::NSimple {
class TSpecialReadContext;
class IDataSource;
class TPortionDataSource;
class TColumnFetchingContext;
}   // namespace NKikimr::NOlap::NReader::NSimple

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TInternalFilterConstructor: TMoveOnly {
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

        ui64 NumRows() const {
            return End - Begin;
        }
    };

private:
    std::shared_ptr<IFilterSubscriber> Callback;
    ui64 RowsCount;
    std::map<TRowRange, NArrow::TColumnFilter> FiltersByRange;

    bool IsReady() const {
        return !FiltersByRange.empty() && FiltersByRange.begin()->first.GetBegin() == 0 && FiltersByRange.begin()->first.GetEnd() == RowsCount;
    }

    void Complete() {
        AFL_VERIFY(!IsDone());
        AFL_VERIFY(IsReady());
        Callback->OnFilterReady(std::move(FiltersByRange.begin()->second));
        Callback.reset();
        AFL_VERIFY(IsDone());
    }

public:
    void AddFilter(const TDuplicateMapInfo& info, const NArrow::TColumnFilter& filterExt) {
        AFL_VERIFY(!IsDone());
        AFL_VERIFY(filterExt.GetRecordsCountVerified() == info.GetRowsCount())("filter", filterExt.GetRecordsCountVerified())(
                                                              "info", info.GetRowsCount());
        FiltersByRange.emplace(TRowRange(info.GetOffset(), info.GetOffset() + info.GetRowsCount()), filterExt);

        while (FiltersByRange.size() > 1 && FiltersByRange.begin()->first.GetEnd() >= std::next(FiltersByRange.begin())->first.GetBegin()) {
            auto l = FiltersByRange.begin();
            auto r = std::next(FiltersByRange.begin());
            AFL_VERIFY(l->first.GetEnd() == r->first.GetBegin());
            TRowRange range = TRowRange(l->first.GetBegin(), r->first.GetEnd());
            NArrow::TColumnFilter filter = l->second;
            filter.Append(r->second);
            FiltersByRange.erase(FiltersByRange.begin());
            FiltersByRange.erase(FiltersByRange.begin());
            AFL_VERIFY(filter.GetRecordsCountVerified() == range.NumRows())("filter", filter.GetRecordsCountVerified())(
                                                               "range", range.NumRows());
            FiltersByRange.emplace(range, std::move(filter));
        }

        if (IsReady()) {
            Complete();
        }
    }

    bool IsDone() const {
        return !Callback;
    }

    void Abort(const TString& error) {
        Callback->OnFailure(error);
        Callback.reset();
    }

    TInternalFilterConstructor(const std::shared_ptr<IFilterSubscriber>& callback, const ui64 rowsCount);

    ~TInternalFilterConstructor() {
        AFL_VERIFY(IsDone());
    }
};

class TEvConstructFilters: public NActors::TEventLocal<TEvConstructFilters, NColumnShard::TEvPrivate::EvConstructFilters> {
private:
    using TDataBySource = THashMap<ui64, TSourceCache::TCacheItem>;
    YDB_READONLY_DEF(TDataBySource, ColumnData);
    YDB_READONLY_DEF(TEvRequestFilter::TPtr, OriginalRequest);
    TColumnDataSplitter Splitter;

public:
    TEvConstructFilters(const TEvRequestFilter::TPtr& originalRequest, TDataBySource&& data, TColumnDataSplitter&& splitter)
        : ColumnData(std::move(data))
        , OriginalRequest(originalRequest)
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

class TDuplicateManager: public NActors::TActor<TDuplicateManager> {
private:
    class TSourceDataSubscriber: public TSourceCache::ISubscriber {
    private:
        TActorId Owner;
        TColumnDataSplitter Splitter;
        TEvRequestFilter::TPtr OriginalRequest;

        virtual void OnSourcesReady(TSourceCache::TSourcesData&& result) override;
        virtual void OnFailure(const TString& error) override {
            Y_UNUSED(error);   // FIXME
            TActivationContext::AsActorContext().Send(Owner, new NActors::TEvents::TEvPoison);
        }

    public:
        TSourceDataSubscriber(const TActorId& owner, const TEvRequestFilter::TPtr& originalRequest, TColumnDataSplitter&& splitter)
            : Owner(owner)
            , Splitter(std::move(splitter))
            , OriginalRequest(originalRequest) {
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
    NColumnShard::TDuplicateFilteringCounters Counters;
    TSourceCache* SourceCache;
    const THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    const TIntervalTree<NArrow::TSimpleRow, ui64> Intervals;
    TLRUCache<TDuplicateMapInfo, NArrow::TColumnFilter> FiltersCache;
    THashMap<TDuplicateMapInfo, std::vector<std::shared_ptr<TInternalFilterConstructor>>> BuildingFilters;
    std::shared_ptr<NConveyor::TProcessGuard> ConveyorProcessGuard;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(TEvConstructFilters, Handle);
            hFunc(TEvFiltersConstructed, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const TEvConstructFilters::TPtr&);
    void Handle(const TEvFiltersConstructed::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        for (auto& [_, constructors] : BuildingFilters) {
            for (auto& constructor : constructors) {
                if (!constructor->IsDone()) {
                    constructor->Abort("aborted by actor system");
                }
            }
        }
        PassAway();
    }

    const std::shared_ptr<TPortionInfo>& GetPortionVerified(const ui64 portionId) const {
        const auto* portion = Portions.FindPtr(portionId);
        AFL_VERIFY(portion);
        return *portion;
    }

public:
    TDuplicateManager(const TSpecialReadContext& context);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
