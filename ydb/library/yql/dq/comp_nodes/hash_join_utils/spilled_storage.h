#pragma once
#include <library/cpp/threading/future/wait/wait.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_join_table.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/alloc.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/join_defs.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/mkql_bitmap.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {

NYql::TChunkedBuffer Serialize(TPackResult&& result);
NYql::TChunkedBuffer Serialize(const TMKQLBitMap& bits);

TPackResult Parse(NYql::TChunkedBuffer&& buff, const NPackedTuple::TTupleLayout* layout);
void Parse(NYql::TChunkedBuffer&& buffer, TMKQLBitMap& bits);

template <typename T>
NThreading::TFuture<ISpiller::TKey> Spill(ISpiller& spiller, T&& value) {
    return spiller.Put(Serialize(std::forward<T>(value)));
}

struct BlobIdAndBucketIndex {
    bool IsReady() const {
        return BlobId.IsReady();
    }

    NThreading::TFuture<ISpiller::TKey> BlobId;
    int BucketIndex;
};

struct TSpillerSettings {
    int Buckets;
    int BucketSizeBytes;
    int SpillingPagesAtTime;

    int BucketIndex(TSingleTuple tuple) const {
        return NPackedTuple::Hash(tuple.PackedData) & (static_cast<ui32>(Buckets) - 1);
    }
};

// constexpr TSpillerSettings RuntimeStorageSettings{.Buckets = 128, .BucketSizeBytes = (1<<19), .SpillingPagesAtTime =
// 3};
constexpr TSpillerSettings TestStorageSettings{.Buckets = 1 << Log2Buckets, .BucketSizeBytes = (1 << 16), .SpillingPagesAtTime = 8};

enum class EBucketAssign {
    Hash,
    RoundRobin,
};

enum ESpillResult {
    Spilling,
    FinishedSpilling,
    DontHavePages
};

inline ESpillResult Wait() {
    return ESpillResult::Spilling;
}

struct TSpillingPage {
    void StartSpilling(ISpiller& spiller) {
        if (!ProbeMatchBits.Empty()) {
            MatchWrite = Spill(spiller, ProbeMatchBits);
            ProbeMatchBits.Reset();
        }
        Write = Spill(spiller, std::move(Page));
    }

    TPackResult Page;
    NThreading::TFuture<ISpiller::TKey> Write;
    std::optional<NThreading::TFuture<ISpiller::TKey>> MatchWrite;
    TMKQLBitMap ProbeMatchBits;
    ESide Side;
    int BucketIndex;
};

template <TSpillerSettings Settings> class TBucketsSpiller {
    static_assert(Settings.Buckets > 0 && (Settings.Buckets & (Settings.Buckets - 1)) == 0);

    std::optional<int> FindInMemoryBucketWithMostPages() const {
        std::optional<int> resIndex;
        for (int index = 0; index < std::ssize(Buckets_); ++index) {
            if (!Buckets_[index].IsSpilled() && !Buckets_[index].InMemoryPages().empty()) {
                if (resIndex == std::nullopt) {
                    resIndex = index;
                } else {
                    if (Buckets_[*resIndex].InMemoryPages().size() < Buckets_[index].InMemoryPages().size()) {
                        resIndex = index;
                    }
                }
            }
        }
        return resIndex;
    }

    int TotalSpilledPages() const {
        int num = 0;
        for (auto& bucket : Buckets_) {
            num += bucket.SpilledPages.has_value() ? bucket.SpilledPages->size() : 0;
        }
        return num;
    }

    int TotalInMemoryPages() const {
        int num = 0;
        for (auto& bucket : Buckets_) {

            num += bucket.InMemoryPages().size();
            MKQL_ENSURE(bucket.BuildingPage.AllocatedBytes() < Settings.BucketSizeBytes, "sanity check");
            num += bucket.BuildingPage.AllocatedBytes() > 0;
        }
        return num;
    }

    int NextRoundRobinBucket() {
        const int index = RoundRobinCursor_;
        RoundRobinCursor_ = (RoundRobinCursor_ + 1) % Settings.Buckets;
        return index;
    }

  public:
    TBucketsSpiller(ISpiller::TPtr spiller, const NPackedTuple::TTupleLayout* layout,
                    EBucketAssign assign = EBucketAssign::Hash)
        : Buckets_(Settings.Buckets)
        , Spiller_(spiller)
        , Layout_(layout)
        , Assign_(assign)
    {}

    void AddRow(TSingleTuple tuple) {
        const int bucketIndex =
            Assign_ == EBucketAssign::RoundRobin ? NextRoundRobinBucket() : Settings.BucketIndex(tuple);
        TBucket& thisBucket = Buckets_[bucketIndex];
        thisBucket.BuildingPage.AppendTuple(tuple, Layout_);
        thisBucket.DetatchBuildingPageIfLimitReached<Settings.BucketSizeBytes>();
    }

    [[nodiscard]] ESpillResult SpillWhile(std::predicate auto condition) {
        while (condition() || SpillingPages_.has_value()) {
            if (SpillingPages_.has_value()) {
                for (auto& future : *SpillingPages_) {
                    if (!future.IsReady()) {
                        return Wait();
                    }
                }
                for (auto& future : *SpillingPages_) {
                    MKQL_ENSURE(future.BlobId.IsReady(), "no blocking wait");
                    MKQL_ENSURE(Buckets_[future.BucketIndex].IsSpilled(), "spilled page from in memory bucket?");
                    Buckets_[future.BucketIndex].SpilledPages->push_back(future.BlobId.ExtractValueSync());
                }
                SpillingPages_ = std::nullopt;
            } else {

                while (std::accumulate(Buckets_.begin(), Buckets_.end(), 0, [&](int pages, const TBucket& bucket) {
                           return pages + (bucket.IsSpilled() ? std::ssize(bucket.InMemoryPages()) : 0);
                       }) < Settings.SpillingPagesAtTime) {
                    std::optional<int> bucketIndex = FindInMemoryBucketWithMostPages();
                    if (!bucketIndex) {
                        return ESpillResult::DontHavePages;
                    }
                    Buckets_[*bucketIndex].SpilledPages.emplace();
                }
                SpillingPages_.emplace();

                int totalSpillingPages = Settings.SpillingPagesAtTime;
                for (int index = 0; index < std::ssize(Buckets_); ++index) {
                    auto& bucket = Buckets_[index];
                    while (bucket.IsSpilled() && !bucket.InMemoryPages().empty() && totalSpillingPages != 0) {
                        totalSpillingPages--;
                        SpillingPages_->push_back(
                            {.BlobId = Spill(*Spiller_, *bucket.ReleaseAtMostOnePage()),
                             .BucketIndex = index});
                    }
                }
                MKQL_ENSURE(totalSpillingPages == 0, "not enough pages for spilling?");
            }
        }
        MKQL_ENSURE(!condition(), "sanity check");
        return ESpillResult::FinishedSpilling;
    }

    TBuckets& GetBuckets() {
        MKQL_ENSURE(!SpillingPages_.has_value(), "accesing Buckets_ when their state is inconsistent");
        return Buckets_;
    }

    TBuckets Buckets_;
    ISpiller::TPtr Spiller_;
    std::optional<TMKQLVector<BlobIdAndBucketIndex>> SpillingPages_;
    const NPackedTuple::TTupleLayout* Layout_;
    EBucketAssign Assign_ = EBucketAssign::Hash;
    int RoundRobinCursor_ = 0;
};

template <TSpillerSettings Settings> class TProbeSpiller {
  public:
    struct EmptyBucket {};
    using Bucket = 
        std::variant<NJoinTable::TNeumannJoinTable, TSides<TBucket>>;
    
    static bool IsBucketSpilled(const Bucket& bucket) {
        return std::holds_alternative<TSides<TBucket>>(bucket);
    }
    struct State {
        TMKQLVector<Bucket> Buckets;
        TMKQLVector<TSpillingPage> InMemoryPages;
        TMKQLVector<TMKQLVector<ISpiller::TKey>> ProbeMatchKeys;
    };

    TProbeSpiller(ISpiller::TPtr spiller, const NPackedTuple::TTupleLayout* layout, State state)
        : State_(std::move(state))
        , Layout_(layout)
        , Spiller_(spiller)
    {
        BuildingMatchBits_.resize(State_.Buckets.size());
        BuildingMatchRows_.resize(State_.Buckets.size());
        State_.ProbeMatchKeys.resize(State_.Buckets.size());
        FlushBuildingPages();
    }

    [[nodiscard]] ESpillResult SpillWhile(std::predicate auto condition) {
        while (condition() || SpillingPages_.has_value()) {
            if (SpillingPages_.has_value()) {
                for (auto& page : *SpillingPages_) {
                    if (!page.Write.IsReady() || (page.MatchWrite && !page.MatchWrite->IsReady())) {
                        return Wait();
                    }
                }
                for (auto& page : *SpillingPages_) {
                    MKQL_ENSURE(page.Write.IsReady(), "no blocking wait");
                    TSides<TBucket>* thisBucket = std::get_if<TSides<TBucket>>(&State_.Buckets[page.BucketIndex]);
                    MKQL_ENSURE(thisBucket, "spilling page from in memory bucket?");
                    const ISpiller::TKey key = page.Write.ExtractValueSync();
                    thisBucket->SelectSide(page.Side).SpilledPages->push_back(key);
                    if (page.MatchWrite) {
                        MKQL_ENSURE(page.Side == ESide::Probe, "build page has probe match state");
                        State_.ProbeMatchKeys[page.BucketIndex].push_back(page.MatchWrite->ExtractValueSync());
                    }
                }
                SpillingPages_ = std::nullopt;
            } else {
                if (State_.InMemoryPages.size() < Settings.SpillingPagesAtTime) {
                    return ESpillResult::DontHavePages;
                }
                SpillingPages_.emplace();
                for (int index = 0; index < Settings.SpillingPagesAtTime; ++index) {
                    auto page = *GetBackOrNull(State_.InMemoryPages);
                    page.StartSpilling(*Spiller_);
                    SpillingPages_->push_back(std::move(page));
                }
            }
        }
        return ESpillResult::FinishedSpilling;
    }

    void AddRow(TValueAndLocation<TSingleTuple> tuple, std::optional<bool> matched = std::nullopt) {
        MKQL_ENSURE(tuple.Side == ESide::Probe, "this spiller is for probe rows");
        TSides<TBucket>* thisBucket = std::get_if<TSides<TBucket>>(&State_.Buckets[tuple.BucketIndex]);
        MKQL_ENSURE(thisBucket, "spilling row that should be looked up?");
        thisBucket->Probe.BuildingPage.AppendTuple(tuple.Val, Layout_);
        if (matched.has_value()) {
            const size_t row = BuildingMatchRows_[tuple.BucketIndex]++;
            BuildingMatchBits_[tuple.BucketIndex].Reserve(row + 1);
            BuildingMatchBits_[tuple.BucketIndex].Set(row, *matched);
        } else {
            MKQL_ENSURE(BuildingMatchRows_[tuple.BucketIndex] == 0,
                        "all rows in a page must use the same match-state format");
        }
        if (thisBucket->Probe.template DetatchBuildingPageIfLimitReached<Settings.BucketSizeBytes>()) {
            FlushBucketPages(tuple.BucketIndex, ESide::Probe);
        }
    }

    void FlushBuildingPages() {
        for (int index = 0; index < std::ssize(State_.Buckets); ++index) {
            if (std::get_if<TSides<TBucket>>(&State_.Buckets[index])) {
                for (ESide side : EachSide) {
                    FlushBucketPages(index, side);
                }
            }
        }
    }

    bool IsBucketSpilled(int index) const {
        return std::holds_alternative<TSides<TBucket>>(State_.Buckets[index]);
    }

    std::optional<int> FirstSpilledBucket() const {
        for (int index = 0; index < std::ssize(State_.Buckets); ++index) {
            if (IsBucketSpilled(index)) {
                return index;
            }
        }
        return std::nullopt;
    }

    State& GetState() {
        MKQL_ENSURE(!SpillingPages_.has_value(), "pages should've finished spilling earlier");
        return State_;
    }

  private:
    void FlushBucketPages(int index, ESide side) {
        TSides<TBucket>* buckets = std::get_if<TSides<TBucket>>(&State_.Buckets[index]);
        MKQL_ENSURE(buckets, "flushing pages from an in-memory table");
        TBucket& bucket = buckets->SelectSide(side);
        bucket.DetatchBuildingPage();
        auto pages = bucket.DetatchPages();
        for (TPackResult& page : pages) {
            TSpillingPage result{.Page = std::move(page), .Side = side, .BucketIndex = index};
            if (side == ESide::Probe && BuildingMatchRows_[index] != 0) {
                MKQL_ENSURE(pages.size() == 1, "match bits belong to exactly one building page");
                MKQL_ENSURE(BuildingMatchRows_[index] == static_cast<size_t>(result.Page.NTuples),
                            "match bitmap must contain one bit per tuple");
                result.ProbeMatchBits = std::move(BuildingMatchBits_[index]);
                BuildingMatchRows_[index] = 0;
            }
            State_.InMemoryPages.push_back(std::move(result));
        }
    }

    State State_;
    const NPackedTuple::TTupleLayout* Layout_;

    TMKQLVector<TMKQLBitMap> BuildingMatchBits_;
    TMKQLVector<size_t> BuildingMatchRows_;
    std::optional<TMKQLVector<TSpillingPage>> SpillingPages_;
    ISpiller::TPtr Spiller_;

};
} // namespace NKikimr::NMiniKQL
