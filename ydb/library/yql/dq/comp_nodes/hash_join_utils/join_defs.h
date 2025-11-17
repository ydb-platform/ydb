#pragma once

#include <deque>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/layout_converter_common.h>
#include <yql/essentials/minikql/mkql_alloc.h>

namespace NKikimr::NMiniKQL {

template <typename T> using TMKQLDeque = std::deque<T, TMKQLAllocator<T>>;

using TFuturePage = NThreading::TFuture<std::optional<NYql::TChunkedBuffer>>;

enum class ESide { Probe, Build };

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
    ForEachSide([&](ESide side){
        use(sizes.SelectSide(side) + (required.SelectSide(side) ? 0 : transform(sizes.SelectSide(side))));
    });
}

*/
void ForEachSide(std::invocable<ESide> auto fn) {
    fn(ESide::Build);
    fn(ESide::Probe);
}

struct TBucket {
    bool IsSpilled() const {
        return SpilledPages.has_value();
    }

    bool Empty() const {
        bool inMemoryPagesEmpty = true;
        for (auto& page : InMemoryPages) {
            inMemoryPagesEmpty &= page.Empty();
        }
        return inMemoryPagesEmpty && (!SpilledPages.has_value() || SpilledPages->empty()) && BuildingPage.Empty();
    }

    TPackResult BuildingPage;
    std::optional<TMKQLVector<ISpiller::TKey>> SpilledPages;
    TMKQLVector<TPackResult> InMemoryPages;

    bool DetatchBuildingPage() {
        return DetatchBuildingPageIfLimitReached<0>();
    }

    template <int SizeLimit> bool DetatchBuildingPageIfLimitReached() {
        if (BuildingPage.AllocatedBytes() > SizeLimit) {
            InMemoryPages.push_back(std::move(BuildingPage));
            InMemoryPages.back().PackedTuples.shrink_to_fit();
            InMemoryPages.back().Overflow.shrink_to_fit();
            BuildingPage.NTuples = 0;
            return true;
        }
        return false;
    }
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

// struct

} // namespace NKikimr::NMiniKQL