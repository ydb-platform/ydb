#pragma once

#include <deque>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/layout_converter_common.h>
#include <yql/essentials/minikql/mkql_alloc.h>

namespace NKikimr::NMiniKQL {

template <typename T> using TMKQLDeque = std::deque<T, TMKQLAllocator<T>>;

using TFuturePage = NThreading::TFuture<std::optional<NYql::TChunkedBuffer>>;

enum class ESide { Probe, Build };
#ifdef MKQL_ENSURE
#undef MKQL_ENSURE
#endif
#define MKQL_ENSURE(condition, message)                                   \
    do {                                                                  \
        if (Y_UNLIKELY(!(condition))) {                                   \
            PrintBackTrace();                                             \
            (THROW yexception() << __FUNCTION__ << "(): requirement "     \
                                << #condition << " failed. " << message); \
        }                                                                 \
    } while (0)

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
    ForEachSide([&](ESide side){
        use(sizes.SelectSide(side) + (required.SelectSide(side) ? 0 : transform(sizes.SelectSide(side))));
    });
}

*/
void ForEachSide(std::invocable<ESide> auto fn) {
    fn(ESide::Build);
    fn(ESide::Probe);
}

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
    std::optional<TMKQLVector<ISpiller::TKey>> SpilledPages;
    const TMKQLVector<TPackResult>& InMemoryPages() const {
        return InMemoryPages_;
    }
    std::optional<TPackResult> ReleaseAtMostOnePage() {
        return GetBackOrNull(InMemoryPages_);
    }
    TMKQLVector<TPackResult> ReleaseInMemoryPages() {
        return std::move(InMemoryPages_);
    }

    bool DetatchBuildingPage() {
        return DetatchBuildingPageIfLimitReached<0>();
    }


    void ForEachPage(std::invocable<TPackResult> auto fn) {
        for(auto& page: InMemoryPages_) {
            MKQL_ENSURE(!page.Empty(), "sanity check");
            fn(std::move(page));
        }
        InMemoryPages_.clear();
    }

    template <int SizeLimit> bool DetatchBuildingPageIfLimitReached() {
        if (BuildingPage.AllocatedBytes() > SizeLimit) {
            // MKQL_ENSURE(condition, message)
            MKQL_ENSURE(!BuildingPage.Empty(), "sanity check");
            InMemoryPages_.push_back(std::move(BuildingPage));
            InMemoryPages_.back().PackedTuples.shrink_to_fit();
            InMemoryPages_.back().Overflow.shrink_to_fit();
            BuildingPage.NTuples = 0;
            MKQL_ENSURE(BuildingPage.Empty(), "sanity check");
            return true;
        }
        return false;
    }
private:
TMKQLVector<TPackResult> InMemoryPages_;
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