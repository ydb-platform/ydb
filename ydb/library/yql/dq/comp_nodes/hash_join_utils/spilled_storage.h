#pragma once
#include <library/cpp/threading/future/wait/wait.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_join_table.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/alloc.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/join_defs.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {


void PopFront(NYql::TChunkedBuffer& buff);

NYql::TChunkedBuffer Serialize(TPackResult&& result);

TPackResult Parse(NYql::TChunkedBuffer&& buff, const NPackedTuple::TTupleLayout* layout);

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
        ui32 hash = NPackedTuple::Hash(tuple.PackedData);
        MKQL_ENSURE(std::popcount(static_cast<ui32>(Buckets) - 1) == std::bit_width(static_cast<ui32>(Buckets)) - 1,
                    "size of buckets should be power of two");
        return hash & (static_cast<ui32>(Buckets) - 1);
    }
};

// constexpr TSpillerSettings RuntimeStorageSettings{.Buckets = 128, .BucketSizeBytes = (1<<19), .SpillingPagesAtTime =
// 3};
constexpr TSpillerSettings TestStorageSettings{.Buckets = 64, .BucketSizeBytes = (1 << 16), .SpillingPagesAtTime = 8};

enum ESpillResult {
    Spilling,
    FinishedSpilling,
    DontHavePages
};


inline ESpillResult Wait() {
    return ESpillResult::Spilling;
}

NThreading::TFuture<ISpiller::TKey> SpillPage(ISpiller& spiller, TPackResult&& page);

template <TSpillerSettings Settings> class TBucketsSpiller {
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

  public:
    TBucketsSpiller(ISpiller::TPtr spiller, const NPackedTuple::TTupleLayout* layout)
        : Buckets_(Settings.Buckets)
        , Spiller_(spiller)
        , Layout_(layout)
    {}

    void AddRow(TSingleTuple tuple) {
        int bucketIndex = Settings.BucketIndex(tuple);
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
                            {.BlobId = SpillPage(*Spiller_, *bucket.ReleaseAtMostOnePage()),
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
        TMKQLVector<TValueAndLocation<TPackResult>> InMemoryPages;
    };

    TProbeSpiller(ISpiller::TPtr spiller, const NPackedTuple::TTupleLayout* layout, State state)
        : State_(std::move(state))
        , Layout_(layout)
        , Spiller_(spiller)
    {
        for(int index = 0; index < std::ssize(State_.Buckets); ++index) {
            TSides<TBucket>* thisBucket = std::get_if<TSides<TBucket>>(&State_.Buckets[index]);
            if (thisBucket) {
                ForEachSide([&](ESide side){
                    thisBucket->SelectSide(side).DetatchBuildingPage();
                    thisBucket->SelectSide(side).ForEachPage([&](TPackResult page){
                        State_.InMemoryPages.push_back(TValueAndLocation<TPackResult>{.Val = std::move(page), .Side = side, .BucketIndex = index});
                    });
                });
            }
        }
    }

    [[nodiscard]] ESpillResult SpillWhile(std::predicate auto condition) {
        while (condition() || SpillingPages_.has_value()) {
            if (SpillingPages_.has_value()) {
                for (auto& future : *SpillingPages_) {

                    if (!future.Val.IsReady()) {
                        return Wait();
                    }
                }
                for (auto& future : *SpillingPages_) {
                    MKQL_ENSURE(future.Val.IsReady(), "no blocking wait");
                    TSides<TBucket>* thisBucket = std::get_if<TSides<TBucket>>(&State_.Buckets[future.BucketIndex]);
                    MKQL_ENSURE(thisBucket, "spilling page from in memory bucket?");
                    thisBucket->SelectSide(future.Side)
                        .SpilledPages->push_back(future.Val.ExtractValueSync());
                }
                SpillingPages_ = std::nullopt;
            } else {
                if (State_.InMemoryPages.size() < Settings.SpillingPagesAtTime) {
                    return ESpillResult::DontHavePages;
                }
                SpillingPages_.emplace();
                for (int index = 0; index < Settings.SpillingPagesAtTime; ++index) {
                    auto page = *GetBackOrNull(State_.InMemoryPages);
                    SpillingPages_->push_back({.Val = SpillPage(*Spiller_, std::move(page.Val)), .Side = page.Side,
                                               .BucketIndex = page.BucketIndex});
                }
            }
        }
        return ESpillResult::FinishedSpilling;
    }

    void AddRow(TValueAndLocation<TSingleTuple> tuple) {
        MKQL_ENSURE(tuple.Side == ESide::Probe, "this spiller is for probe rows");
        TSides<TBucket>* thisBucket = std::get_if<TSides<TBucket>>(&State_.Buckets[tuple.BucketIndex]);
        MKQL_ENSURE(thisBucket, "spilling row that should be looked up?");
        thisBucket->Probe.BuildingPage.AppendTuple(tuple.Val, Layout_);
        if (thisBucket->Probe.template DetatchBuildingPageIfLimitReached<Settings.BucketSizeBytes>()) {
            thisBucket->Probe.ForEachPage([&](TPackResult page){
                State_.InMemoryPages.push_back(
                    {.Val = std::move(page), .Side = tuple.Side, .BucketIndex = tuple.BucketIndex});
            });
        }
    }

    bool IsBucketSpilled(int index) {
        return std::holds_alternative<TSides<TBucket>>(State_.Buckets[index]);
    }

    State& GetState() {
        MKQL_ENSURE(!SpillingPages_.has_value(), "pages should've finished spilling earlier");
        return State_;
    }

  private:
    State State_;
    const NPackedTuple::TTupleLayout* Layout_;

    std::optional<TMKQLVector<TValueAndLocation<NThreading::TFuture<ISpiller::TKey>>>> SpillingPages_;
    ISpiller::TPtr Spiller_;

};
} // namespace NKikimr::NMiniKQL
