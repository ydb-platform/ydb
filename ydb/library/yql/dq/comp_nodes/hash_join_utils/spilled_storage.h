#pragma once
#include <library/cpp/threading/future/wait/wait.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/alloc.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/join_defs.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {
// template<int MaxPageSize>

void PopFront(NYql::TChunkedBuffer& buff);

NYql::TChunkedBuffer Serialize(TPackResult&& result);

TPackResult Parse(NYql::TChunkedBuffer&& buff);

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
        MKQL_ENSURE(std::popcount(static_cast<ui32>(Buckets) - 1) == std::bit_width(static_cast<ui32>(Buckets)) - 1, "size of buckets should be power of two");
        return hash & (static_cast<ui32>(Buckets) - 1);
    }
};
// constexpr TSpillerSettings RuntimeStorageSettings{.Buckets = 128, .BucketSizeBytes = (1<<19), .SpillingPagesAtTime = 3};
constexpr TSpillerSettings TestStorageSettings{.Buckets = 4, .BucketSizeBytes = (1<<16), .SpillingPagesAtTime = 1};

enum ESpillResult {
    Spilling,
    FinishedSpilling,
    DontHavePages
};



// template<typename T>
// concept SpillStorage = requires( T t) {
//     {t.SpillWhile()} -> 
// };
inline ESpillResult Wait() {
    return ESpillResult::Spilling;
}

NThreading::TFuture<ISpiller::TKey> SpillPage(ISpiller& spiller, TPackResult&& page);

template<TSpillerSettings Settings>
class   TBucketsSpiller {
    std::optional<int> FindInMemoryBucketWithMostPages() const {
        std::optional<int> resIndex;
        for(int index = 0; index < std::ssize(Buckets_); ++index) {
            if (!Buckets_[index].IsSpilled() && !Buckets_[index].InMemoryPages.empty()) {
                if (resIndex == std::nullopt) {
                    resIndex = index;
                } else {
                    if (Buckets_[*resIndex].InMemoryPages.size() < Buckets_[index].InMemoryPages.size()) {
                        resIndex = index;
                    }
                }
            }
        }
        return resIndex;
    }
    int TotalSpilledPages() const {
        int num = 0;
        for(auto& bucket: Buckets_) {
            num += bucket.SpilledPages.has_value() ? bucket.SpilledPages->size() : 0;
        }
        return num;
    }
    int TotalInMemoryPages() const {
        int num = 0;
        for(auto& bucket: Buckets_) {
            // num += bucket.SpilledPages.has_value() ? bucket.SpilledPages->size() : 0;
            num += bucket.InMemoryPages.size();
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
        // return bucketIndex;
    }
    [[nodiscard]] ESpillResult SpillWhile(std::predicate auto condition)  {
        while(condition() || SpillingPages_.has_value()) {
            // Cout << std::format("free mem:{}\n", FreeMemory());
            if (SpillingPages_.has_value()) {
                // Cout << "Have spilling pages " << Endl;
                for(auto& future: *SpillingPages_) {
                    if (!future.IsReady()){
                        return Wait();
                    }
                }
                for(auto& future: *SpillingPages_) {
                    MKQL_ENSURE(future.BlobId.IsReady(), "no blocking wait");
                    MKQL_ENSURE(Buckets_[future.BucketIndex].IsSpilled(), "spilled page from in memory bucket?");
                    Buckets_[future.BucketIndex].SpilledPages->push_back(future.BlobId.ExtractValueSync());
                }
                SpillingPages_ = std::nullopt;
            } else {
                // Cout << std::format("Dont have spilling pages, total pages spilled: {}, in memory: {} ", TotalSpilledPages(), TotalInMemoryPages() ) << Endl;

                while (std::accumulate(Buckets_.begin(), Buckets_.end(), 0, [&](int pages, const TBucket& bucket) {
                    return pages + bucket.IsSpilled() ? std::ssize(bucket.InMemoryPages) : 0;
                }) < Settings.SpillingPagesAtTime) {
                    std::optional<int> bucketIndex = FindInMemoryBucketWithMostPages();
                    if (!bucketIndex) {
                        // Cout << "Dont have pages" << Endl;
                        return ESpillResult::DontHavePages;
                    }
                    Buckets_[*bucketIndex].SpilledPages.emplace();
                }
                SpillingPages_.emplace();
                // TMKQLVector<BlobIdAndBucketIndex> spilledPages;
                int totalSpillingPages = Settings.SpillingPagesAtTime;
                for(int index = 0; index < std::ssize(Buckets_); ++index) {
                    auto& bucket = Buckets_[index];
                    while (bucket.IsSpilled() && !bucket.InMemoryPages.empty() && totalSpillingPages != 0) {
                        totalSpillingPages--;
                        SpillingPages_->push_back({.BlobId = SpillPage(*Spiller_, std::move(bucket.InMemoryPages.back())), .BucketIndex = index});
                        bucket.InMemoryPages.pop_back();
                    }
                }
                MKQL_ENSURE(totalSpillingPages == 0, "not enough pages for spilling?");
            }
        }
        // Cout << 
        MKQL_ENSURE(!condition(), "sanitiy check");
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

template<TSpillerSettings Settings>
class TSimpleSpiller {
public:
    struct State {
        TPairOfBuckets SpilledBuckets_;
        TMKQLVector<TValueAndLocation<TPackResult>> InMemoryPages_;
    };
    TSimpleSpiller(ISpiller::TPtr spiller, const NPackedTuple::TTupleLayout* layout )
    : Layout_(layout), Spiller_(spiller) {
        State_.SpilledBuckets_.resize(Settings.Buckets);
    }
    [[nodiscard]] ESpillResult SpillWhile(std::predicate auto condition) {
        while(condition() || SpillingPages_.has_value()) {
            if (SpillingPages_.has_value()) {
                // NThreading::WaitAll(*SpillingPages_);
                for(auto& future: *SpillingPages_) {

                    if (!future.Val.IsReady()){
                        return Wait();
                    }
                }
                for(auto& future: *SpillingPages_) {
                    MKQL_ENSURE(future.Val.IsReady(), "no blocking wait");
                    MKQL_ENSURE(State_.SpilledBuckets_[future.BucketIndex].SelectSide(future.Side).IsSpilled(), "spilling page from in memory bucket?");
                    State_.SpilledBuckets_[future.BucketIndex].SelectSide(future.Side).SpilledPages->push_back(future.Val.ExtractValueSync());
                }
                SpillingPages_ = std::nullopt;
            } else {

                // int spillingThisTime = /
                if (State_.InMemoryPages_.size() < Settings.SpillingPagesAtTime) {
                    return ESpillResult::DontHavePages;
                }
                SpillingPages_.emplace();
                for(int index = 0; index < Settings.SpillingPagesAtTime; ++index) {
                    auto& page = State_.InMemoryPages_.back();
                    SpillingPages_->push_back({.Val = SpillPage(*Spiller_, std::move(page.Val)), .Side = page.Side, .BucketIndex = page.BucketIndex});
                    State_.InMemoryPages_.pop_back();
                }
            }
        }
        return ESpillResult::FinishedSpilling;
    }
    void AddRow(TValueAndLocation<TSingleTuple> tuple) {
        MKQL_ENSURE(tuple.Side == ESide::Probe, "this spiller is for probe rows");
        TBucket& thisBucket = State_.SpilledBuckets_[tuple.BucketIndex].Probe;
        MKQL_ENSURE(thisBucket.InMemoryPages.empty(), "buckets in TSimpleSpiller should not contain in memory pages");
        MKQL_ENSURE(thisBucket.IsSpilled(), "spilling row of in memory bucket?");
        thisBucket.BuildingPage.AppendTuple(tuple.Val, Layout_);
        if (thisBucket.template DetatchBuildingPageIfLimitReached<Settings.BucketSizeBytes>()) {
            for(auto& page: thisBucket.InMemoryPages) {
                State_.InMemoryPages_.push_back({.Val = std::move(page), .Side = tuple.Side, .BucketIndex = tuple.BucketIndex});
            }
        }
        // InMemoryPages_.push_front(std::move(page));
    }
    bool IsBucketSpilled(int index) {
        TSides<bool> answers;
        ForEachSide([&](ESide side){
            answers.SelectSide(side) = State_.SpilledBuckets_[index].SelectSide(side).IsSpilled();
        });
        MKQL_ENSURE(answers.Build == answers.Probe, "pair of buckets should be in 1 state");
        return answers.Build;
    }
    State& GetState() {
        MKQL_ENSURE(!SpillingPages_.has_value(), "pages shoul've finished spilling earlier");
        // for(auto& memPage: InMemoryPages_) {
        //     SpilledBuckets_[memPage.BucketIndex].SelectSide(memPage.Side).InMemoryPages.push_back(std::move(memPage.Val));
        // }
        return State_;
    }
private:
    State State_;
    // TPairOfBuckets SpilledBuckets_;
    // // TMKQLVector<TPackResult> BucketBuffers_;
    // TMKQLVector<TValueAndLocation<TPackResult>> InMemoryPages_;
    const NPackedTuple::TTupleLayout* Layout_;

    std::optional<TMKQLVector<TValueAndLocation<NThreading::TFuture<ISpiller::TKey>>>> SpillingPages_;
    ISpiller::TPtr Spiller_;
    // std::optional<TMKQLVector<typename Type>>
};
}
