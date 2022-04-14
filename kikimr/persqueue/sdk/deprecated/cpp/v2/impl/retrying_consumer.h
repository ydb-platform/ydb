#pragma once

#include "consumer.h"
#include "scheduler.h"
#include "internals.h"
#include "persqueue_p.h"
#include "iconsumer_p.h"

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include <kikimr/yndx/api/grpc/persqueue.grpc.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <deque>

namespace NPersQueue {

// @brief consumer implementation which transparently retries all connectivity errors
class TRetryingConsumer: public IConsumerImpl, public std::enable_shared_from_this<TRetryingConsumer> {
    // @brief locked partitions info
    struct TLockInfo {
        THashSet<ui64> Cookies; // related cookies
        ui64 Gen = 0;
        ui64 OriginalGen = 0; // original generation
        ui64 ReadOffset = 0; // read offset specified by client
        bool Locked = false;
    };

    struct TCookieInfo {
        THashSet<TLockInfo*> Locks; // related locks
        ui64 OriginalCookie = 0;  // zero means invalid cookie
        ui64 UserCookie = 0;
    };

public:
    TRetryingConsumer(const TConsumerSettings& settings, std::shared_ptr<void> destroyEventRef,
                      TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger);

    ~TRetryingConsumer() noexcept override;

    NThreading::TFuture<TConsumerCreateResponse> Start(TInstant deadline = TInstant::Max()) noexcept override;

    using IConsumerImpl::GetNextMessage;
    void GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept override;

    void Commit(const TVector<ui64>& cookies) noexcept override;

    void RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept override;

    NThreading::TFuture<TError> IsDead() noexcept override;

    void Cancel() override;

    NThreading::TFuture<void> Destroyed() noexcept override;

private:
    void OnStartDeadline();
    void OnConsumerDead(const TError& error);
    void ScheduleReconnect();
    void DoReconnect(TInstant deadline);
    void StartProcessing(const NThreading::TFuture<TConsumerCreateResponse>& f);
    void SubscribeDestroyed();
    void Destroy(const TError& error);
    void Destroy(const TString& description, NErrorCode::EErrorCode code = NErrorCode::ERROR);
    void DoRequest();
    void ProcessResponse(TConsumerMessage&& message);
    void FastResponse(TConsumerMessage&& message);
    void FastCommit(const TVector<ui64>& cookies);
    void UpdateReadyToRead(const NPersQueue::TLockInfo& readyToRead, const TString& topic, ui32 partition, ui64 generation);

private:
    TConsumerSettings Settings;
    TIntrusivePtr<ILogger> Logger;
    std::shared_ptr<IConsumerImpl> Consumer;
    TString SessionId;

    NThreading::TFuture<TConsumerCreateResponse> StartFuture;
    NThreading::TPromise<TConsumerCreateResponse> StartPromise;
    NThreading::TPromise<TError> IsDeadPromise;
    NThreading::TPromise<void> ConsumerDestroyedPromise;

    // requests which are waiting for response
    std::deque<NThreading::TPromise<TConsumerMessage>> PendingRequests;
    // ready messages for returning to clients immediately
    std::deque<TConsumerMessage> ReadyResponses;
    // active cookies
    std::deque<TCookieInfo> Cookies;
    THashMap<ui64, ui64> CommittingCookies;
    // active data per topic, partition
    THashMap<std::pair<TString, ui64>, TLockInfo> Locks;  // topic, partition -> LockInfo

    // number of unsuccessful retries after last error
    ui64 GenCounter;
    ui64 CookieCounter;
    unsigned ReconnectionAttemptsDone;
    // destroying process started
    bool Stopping;
    // reconnecting in process
    bool Reconnecting;
};
}
