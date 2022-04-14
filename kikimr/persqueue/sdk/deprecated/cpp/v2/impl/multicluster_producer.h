#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include "internals.h"
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iproducer.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>
#include "channel.h"
#include "iproducer_p.h"

#include <library/cpp/threading/future/future.h>

#include <util/generic/intrlist.h>
#include <util/random/random.h>

#include <deque>

namespace NPersQueue {

// Structure to perform choose by weights
class TWeightIndex {
public:
    // Creates zero-weighted disabled index
    explicit TWeightIndex(size_t count);

    void SetWeight(size_t i, unsigned weight);

    void Enable(size_t i);
    void Disable(size_t i);

    // Chooses by random number in interval [0, WeightsSum())
    size_t Choose(unsigned randomNumber) const;

    size_t RandomChoose() const {
        return Choose(RandomNumber<unsigned>(WeightsSum()));
    }

    size_t EnabledCount() const {
        return EnabledCnt;
    }

    bool Enabled(size_t i) const {
        return i < IsEnabled.size() ? IsEnabled[i] : false;
    }

    unsigned WeightsSum() const {
        return Index.back();
    }

private:
    void UpdateIndex(size_t i, int weightDiff);

private:
    std::vector<unsigned> Weights;
    std::vector<unsigned> Index; // structure for choosing. Index[i] is sum of enabled weights with index <= i.
    std::vector<bool> IsEnabled;
    size_t EnabledCnt;
};

class TMultiClusterProducer: public IProducerImpl, public std::enable_shared_from_this<TMultiClusterProducer> {
public:
    TMultiClusterProducer(const TMultiClusterProducerSettings& settings, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger);
    ~TMultiClusterProducer();

    NThreading::TFuture<TProducerCreateResponse> Start(TInstant deadline) noexcept override;

    using IProducerImpl::Write;

    // Forbidden write
    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TProducerSeqNo seqNo, TData data) noexcept override;

    // Allowed write
    void Write(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) noexcept override;

    NThreading::TFuture<TError> IsDead() noexcept override;

    NThreading::TFuture<void> Destroyed() noexcept override;

    void Cancel() override;

private:
    struct TWriteInfo: public TIntrusiveListItem<TWriteInfo> {
        NThreading::TPromise<TProducerCommitResponse> ResponsePromise;
        NThreading::TFuture<TProducerCommitResponse> ResponseFuture;
        bool WaitingCallback = false;
        std::vector<size_t> History; // Number of DCs with failed or pending writes
        TData Data;
        bool LastHope = false; // This DC is the last hope: retry till success.
                               // If this flag is set, this write info is in the local subproducer queue.
    };

    struct TSubproducerInfo {
        size_t Number = std::numeric_limits<size_t>::max();
        std::shared_ptr<IProducerImpl> Producer;
        NThreading::TFuture<TProducerCreateResponse> StartFuture;
        NThreading::TFuture<TError> DeadFuture;
        TIntrusiveListWithAutoDelete<TWriteInfo, TDelete> LastHopePendingWrites; // Local queue with writes only to this DC.
        bool StartAnswered = false;
    };

private:
    std::shared_ptr<IProducerImpl> CreateSubproducer(const TProducerSettings& settings, size_t index, bool fallback = false);

    std::vector<TSubproducerInfo>& GetSubproducers(bool fallback = false) {
        return fallback ? FallbackSubproducers : Subproducers;
    }

    TWeightIndex& GetWeightIndex(bool fallback = false) {
        return fallback ? FallbackWeightIndex : WeightIndex;
    }

    static TString MakeSourceId(const TStringBuf& prefix, const TStringBuf& srcSourceId,
                                size_t producerIndex, bool fallback);
    void StartSubproducers(TInstant deadline, bool fallback = false);
    void StartSubproducer(TInstant deadline, size_t i, bool fallback = false);
    void OnProducerStarted(size_t i, bool fallback);
    void OnProducerDead(size_t i, bool fallback);
    void OnWriteResponse(TWriteInfo* wi);
    void ScheduleProducerRestart(size_t i, bool fallback);
    void OnNeedProducerRestart(size_t i, bool fallback); // scheduler action
    void Destroy(const TString& description);
    void Destroy(const TError& error);
    void DestroyWrites(const TError& error, TIntrusiveListWithAutoDelete<TWriteInfo, TDelete>& pendingWrites);
    void DelegateWrite(TWriteInfo* wi, size_t i, bool fallback = false);
    void ResendLastHopeQueue(size_t i);
    void SubscribeDestroyed();

protected:
    TMultiClusterProducerSettings Settings;
    NThreading::TPromise<TError> DeadPromise;
    NThreading::TPromise<TProducerCreateResponse> StartPromise;
    enum class EState {
        Created,
        Starting,
        Working,
        Dead,
    };
    EState State;
    TIntrusivePtr<ILogger> Logger;
    TWeightIndex WeightIndex;
    TWeightIndex FallbackWeightIndex;
    std::vector<TSubproducerInfo> Subproducers;
    std::vector<TSubproducerInfo> FallbackSubproducers;
    TIntrusiveListWithAutoDelete<TWriteInfo, TDelete> PendingWrites;
    NThreading::TPromise<void> ProducersDestroyed = NThreading::NewPromise<void>();
};

} // namespace NPersQueue
