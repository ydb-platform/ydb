#pragma once

#include <yql/essentials/minikql/computation/mkql_spiller_factory.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>

#include <util/generic/vector.h>

#include <algorithm>
#include <numeric>

namespace NKikimr::NMiniKQL {

class TControlledWriteSpiller : public ISpiller {
public:
    explicit TControlledWriteSpiller(ISpiller::TPtr underlying)
        : Underlying_(std::move(underlying))
    {
    }

    NThreading::TFuture<TKey> Put(NYql::TChunkedBuffer&& blob) override {
        auto promise = NThreading::NewPromise<TKey>();
        auto future = promise.GetFuture();
        const TKey key = Underlying_->Put(std::move(blob)).ExtractValueSync();
        PendingPuts_.push_back({.Promise = std::move(promise), .Key = key});
        MaxPendingPuts_ = std::max(MaxPendingPuts_, PendingPuts_.size());
        ++TotalPuts_;
        return future;
    }

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Get(TKey key) override {
        return Underlying_->Get(key);
    }

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Extract(TKey key) override {
        return Underlying_->Extract(key);
    }

    NThreading::TFuture<void> Delete(TKey key) override {
        return Underlying_->Delete(key);
    }

    void ReportAlloc(ui64 bytes) override {
        Underlying_->ReportAlloc(bytes);
    }

    void ReportFree(ui64 bytes) override {
        Underlying_->ReportFree(bytes);
    }

    size_t PendingPuts() const {
        return PendingPuts_.size();
    }

    size_t MaxPendingPuts() const {
        return MaxPendingPuts_;
    }

    size_t TotalPuts() const {
        return TotalPuts_;
    }

    void CompletePendingPuts() {
        auto pending = std::move(PendingPuts_);
        PendingPuts_.clear();
        for (auto& put : pending) {
            put.Promise.SetValue(put.Key);
        }
    }

private:
    struct TPendingPut {
        NThreading::TPromise<TKey> Promise;
        TKey Key;
    };

    ISpiller::TPtr Underlying_;
    TVector<TPendingPut> PendingPuts_;
    size_t MaxPendingPuts_ = 0;
    size_t TotalPuts_ = 0;
};

class TControlledWriteSpillerFactory : public ISpillerFactory {
public:
    explicit TControlledWriteSpillerFactory(std::shared_ptr<ISpillerFactory> underlyingFactory)
        : UnderlyingFactory_(std::move(underlyingFactory))
    {
    }

    ISpiller::TPtr CreateSpiller() override {
        auto spiller = std::make_shared<TControlledWriteSpiller>(UnderlyingFactory_->CreateSpiller());
        Spillers_.push_back(spiller);
        return spiller;
    }

    void SetTaskCounters(const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>& spillingTaskCounters) override {
        UnderlyingFactory_->SetTaskCounters(spillingTaskCounters);
    }

    void SetMemoryReportingCallbacks(ISpiller::TMemoryReportCallback reportAlloc,
                                     ISpiller::TMemoryReportCallback reportFree) override {
        UnderlyingFactory_->SetMemoryReportingCallbacks(std::move(reportAlloc), std::move(reportFree));
    }

    size_t PendingPuts() const {
        return std::accumulate(Spillers_.begin(), Spillers_.end(), size_t{0},
            [](size_t count, const auto& spiller) { return count + spiller->PendingPuts(); });
    }

    size_t MaxPendingPuts() const {
        return std::accumulate(Spillers_.begin(), Spillers_.end(), size_t{0},
            [](size_t maxPending, const auto& spiller) {
                return std::max(maxPending, spiller->MaxPendingPuts());
            });
    }

    size_t TotalPuts() const {
        return std::accumulate(Spillers_.begin(), Spillers_.end(), size_t{0},
            [](size_t count, const auto& spiller) { return count + spiller->TotalPuts(); });
    }

    void CompletePendingPuts() {
        for (auto& spiller : Spillers_) {
            spiller->CompletePendingPuts();
        }
    }

private:
    std::shared_ptr<ISpillerFactory> UnderlyingFactory_;
    TVector<std::shared_ptr<TControlledWriteSpiller>> Spillers_;
};

} // namespace NKikimr::NMiniKQL
