#pragma once

#include <deque>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/layout_converter_common.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include "better_mkql_ensure.h"
namespace NKikimr::NMiniKQL {



template <typename T> using TMKQLDeque = std::deque<T, TMKQLAllocator<T>>;

using TFuturePage = NThreading::TFuture<std::optional<NYql::TChunkedBuffer>>;

enum class ESide { Probe, Build };

const char* AsString(ESide side);

template <typename T> struct TSides {
    T Build;
    T Probe;

    T& SelectSide(ESide side) {
        return side == ESide::Build ? Build : Probe;
    }

    const T& SelectSide(ESide side) const {
        return side == ESide::Build ? Build : Probe;
    }
};


/*
  usage:
  instead of copy pasting code and changing "build" to "probe", use TSides and ForEachSide to call same code twice.
  example:

void f(int buildSize, int probeSize, bool buildRequired, bool probeRequired){
    use(buildSize + (buildRequired ? 0 : transform(buildSize)));
    use(probeSize + (probeRequired ? 0 : transform(probeSize)));
}

vs

void f(TSides<int> sizes, TSides<bool> required) {
    for(ESide side: EachSide){
        use(sizes.SelectSide(side) + (required.SelectSide(side) ? 0 : transform(sizes.SelectSide(side))));    
    }
}

*/

inline std::array<ESide,2> EachSide = {ESide::Build, ESide::Probe};

template <typename Container> std::optional<typename Container::value_type> GetFrontOrNull(Container& cont) {
    if (cont.empty()) {
        return std::nullopt;
    } else {
        std::optional<typename Container::value_type> ret = std::move(*cont.begin());
        cont.erase(cont.begin());
        return ret;
    }
}

template <typename Container> std::optional<typename Container::value_type> GetBackOrNull(Container& cont) {
    if (cont.empty()) {
        return std::nullopt;
    } else {
        auto it = cont.end();
        --it;
        std::optional<typename Container::value_type> ret = std::move(*it);
        cont.erase(it);
        return ret;
    }
}

struct TBucket {
    bool IsSpilled() const {
        return SpilledPages.has_value();
    }

    bool Empty() const {
        bool inMemoryPagesEmpty = true;
        for (auto& page : InMemoryPages_) {
            inMemoryPagesEmpty &= page.Empty();
        }
        return inMemoryPagesEmpty && (!SpilledPages.has_value() || SpilledPages->empty()) && BuildingPage.Empty();
    }

    TPackResult BuildingPage;
    // Parallel to BuildingPage rows. Empty after the building page is detached.
    TMKQLVector<TArrowRowRef> BuildingPageRefs;
    std::optional<TMKQLVector<ISpiller::TKey>> SpilledPages;

    const TMKQLVector<TPackResult>& InMemoryPages() const {
        return InMemoryPages_;
    }
    const TMKQLVector<TMKQLVector<TArrowRowRef>>& InMemoryPageRefs() const {
        return InMemoryPageRefs_;
    }

    std::optional<TPackResult> ReleaseAtMostOnePage() {
        // Refs are released alongside the page to keep the parallel arrays in sync.
        if (!InMemoryPageRefs_.empty()) {
            InMemoryPageRefs_.pop_back();
        }
        return GetBackOrNull(InMemoryPages_);
    }
    TMKQLVector<TPackResult> ReleaseInMemoryPages() {
        return std::move(InMemoryPages_);
    }
    TMKQLVector<TMKQLVector<TArrowRowRef>> ReleaseInMemoryPageRefs() {
        return std::move(InMemoryPageRefs_);
    }

    bool DetatchBuildingPage() {
        return DetatchBuildingPageIfLimitReached<0>();
    }

    TMKQLVector<TPackResult> DetatchPages() {
        auto pages = std::move(InMemoryPages_);
        InMemoryPages_.clear();
        return pages;
    }
    TMKQLVector<TMKQLVector<TArrowRowRef>> DetatchPageRefs() {
        auto refs = std::move(InMemoryPageRefs_);
        InMemoryPageRefs_.clear();
        return refs;
    }


    template <int SizeLimit> bool DetatchBuildingPageIfLimitReached() {
        if (BuildingPage.AllocatedBytes() > SizeLimit) {
            InMemoryPages_.push_back(std::move(BuildingPage));
            InMemoryPages_.back().PackedTuples.shrink_to_fit();
            InMemoryPages_.back().Overflow.shrink_to_fit();
            InMemoryPageRefs_.push_back(std::move(BuildingPageRefs));
            BuildingPage.NTuples = 0;
            BuildingPageRefs.clear();
            return true;
        }
        return false;
    }
private:
TMKQLVector<TPackResult> InMemoryPages_;
// Parallel to InMemoryPages_; entry i contains refs for rows of InMemoryPages_[i].
TMKQLVector<TMKQLVector<TArrowRowRef>> InMemoryPageRefs_;
};

using TBuckets = TMKQLVector<TBucket>;
using TPairOfBuckets = TMKQLVector<TSides<TBucket>>;
bool IsBucketSpilled(const TSides<TBucket>& bucket);

enum class PageSpillingAlready {
    InMemoryYet,
    SpillingInProcess,

};

template <typename Value> struct TValueAndLocation {
    Value Val;
    ESide Side;
    int BucketIndex;
};



} // namespace NKikimr::NMiniKQL