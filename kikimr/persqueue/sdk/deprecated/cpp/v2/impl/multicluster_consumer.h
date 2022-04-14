#pragma once
#include "channel_p.h"
#include "scheduler.h"
#include "iconsumer_p.h"
#include "internals.h"

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>

namespace NPersQueue {

class TPQLibPrivate;

class TMultiClusterConsumer: public IConsumerImpl, public std::enable_shared_from_this<TMultiClusterConsumer> {
public:
    struct TSubconsumerInfo;
    struct TCookieMappingItem;
    struct TCookieMapping;

public:
    TMultiClusterConsumer(const TConsumerSettings& settings, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger);
    ~TMultiClusterConsumer();

    NThreading::TFuture<TConsumerCreateResponse> Start(TInstant deadline) noexcept override;
    NThreading::TFuture<TError> IsDead() noexcept override;

    void GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept override;
    void Commit(const TVector<ui64>& cookies) noexcept override;
    void RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept override;

    void Cancel() override;

private:
    void PatchSettings();
    void StartClusterDiscovery(TInstant deadline);
    void OnClusterDiscoveryDone(TInstant deadline);
    void OnStartTimeout();
    void OnSubconsumerStarted(size_t subconsumerIndex);

    void CheckReadyResponses();
    bool ProcessReadyResponses(size_t subconsumerIndex); // False if consumer has been destroyed.
    void RequestSubconsumers(); // Make requests inflight in every subconsumer equal to our superconsumer inflight.

    bool TranslateConsumerMessage(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex); // False if consumer has been destroyed.
    bool TranslateConsumerMessageLock(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex);
    bool TranslateConsumerMessageRelease(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex);
    bool TranslateConsumerMessageData(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex);
    bool TranslateConsumerMessageCommit(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex);
    bool TranslateConsumerMessageError(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex); // Calls destroy on consumer
    bool TranslateConsumerMessageStatus(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex);

    void Destroy(const TError& description);
    void Destroy(const TString& description, NErrorCode::EErrorCode code = NErrorCode::ERROR);
    void Destroy();

    static TError MakeError(const TString& description, NErrorCode::EErrorCode code = NErrorCode::ERROR);
    static TConsumerCreateResponse MakeCreateResponse(const TError& description);
    static TConsumerMessage MakeResponse(const TError& description);

    void ScheduleClusterDiscoveryRetry(TInstant deadline);

private:
    TConsumerSettings Settings;
    TIntrusivePtr<ILogger> Logger;
    TString SessionId;

    NThreading::TPromise<TConsumerCreateResponse> StartPromise = NThreading::NewPromise<TConsumerCreateResponse>();
    NThreading::TPromise<TError> IsDeadPromise = NThreading::NewPromise<TError>();

    enum class EState {
        Created, // Before Start() was created.
        WaitingClusterDiscovery, // After Start() was called and before we received CDS response.
        StartingSubconsumers, // After we received CDS response and before consumer actually started.
        Working, // When one or more consumers had started.
        Dead,
    };
    EState State = EState::Created;

    TIntrusivePtr<TScheduler::TCallbackHandler> StartDeadlineCallback;
    TChannelImplPtr ClusterDiscoverer;
    TConsumerChannelOverCdsImpl::TResultPtr ClusterDiscoverResult;
    size_t ClusterDiscoveryAttemptsDone = 0;

    std::vector<TSubconsumerInfo> Subconsumers;
    size_t CurrentSubconsumer = 0; // Index of current subconsumer to take responses from

    THolder<TCookieMapping> CookieMapping;
    THashMap<TString, size_t> OldTopicName2Subconsumer; // Used in lock session for requesting partition status.

    std::deque<NThreading::TPromise<TConsumerMessage>> Requests;
};

} // namespace NPersQueue
