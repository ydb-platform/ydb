#pragma once
#include "iconsumer_p.h"

#include <kikimr/public/sdk/cpp/client/ydb_persqueue/persqueue.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <queue>

namespace NPersQueue {

class TYdbSdkCompatibilityConsumer : public IConsumerImpl,
                                     public std::enable_shared_from_this<TYdbSdkCompatibilityConsumer>
{
public:
    TYdbSdkCompatibilityConsumer(const TConsumerSettings& settings,
                                 std::shared_ptr<void> destroyEventRef,
                                 TIntrusivePtr<TPQLibPrivate> pqLib,
                                 TIntrusivePtr<ILogger> logger,
                                 NYdb::NPersQueue::TPersQueueClient& client);
    ~TYdbSdkCompatibilityConsumer();

    void Init() override;

    NThreading::TFuture<TConsumerCreateResponse> Start(TInstant) noexcept override;
    NThreading::TFuture<TError> IsDead() noexcept override;
    void GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept override;
    void Commit(const TVector<ui64>& cookies) noexcept override;
    void Cancel() override;
    void RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept override;

private:
    class TReadSessionEventVisitor;

    NYdb::NPersQueue::TReadSessionSettings MakeReadSessionSettings();
    void SubscribeToNextEvent();
    void OnReadSessionEvent();
    void AnswerToRequests();
    void Destroy(const TError& description);
    void Destroy(const TString& description, NErrorCode::EErrorCode code = NErrorCode::ERROR);

    void HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent&& event);
    void HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TCommitAcknowledgementEvent&& event);
    void HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent&& event);
    void HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent&& event);
    void HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamStatusEvent&& event);
    void HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent&& event);
    void HandleEvent(NYdb::NPersQueue::TSessionClosedEvent&& event);

    void AddResponse(TReadResponse&& response, NThreading::TPromise<TLockInfo>&& readyToRead);
    void AddResponse(TReadResponse&& response);

private:
    const TConsumerSettings Settings;
    NYdb::NPersQueue::TReadSessionSettings YdbSdkSettings;
    TIntrusivePtr<ILogger> Logger;
    TString SessionId;
    NYdb::NPersQueue::TPersQueueClient& Client;
    std::shared_ptr<NYdb::NPersQueue::IReadSession> ReadSession;
    NThreading::TPromise<TError> DeadPromise = NThreading::NewPromise<TError>();
    std::queue<NThreading::TPromise<TConsumerMessage>> Requests;
    std::queue<TConsumerMessage> Responses;
    bool SubscribedToNextEvent = false;
    THashMap<ui64, std::pair<NYdb::NPersQueue::TDeferredCommit, ui64>> CookieToOffsets; // Cookie -> { commit, partition stream id }.
    TMap<std::pair<ui64, ui64>, ui64> MaxOffsetToCookie; // { partition stream id, max offset of cookie } -> cookie.
    THashMap<ui64, THashSet<ui64>> CookiesRequestedToCommit; // Partition stream id -> cookies that user requested to commit.
    THashMap<ui64, THashSet<ui64>> PartitionStreamToCookies; // Partition stream id -> cookies.
    ui64 NextCookie = 1;
    // Commits for graceful release partition after commit.
    THashMap<ui64, TDisjointIntervalTree<ui64>> PartitionStreamToUncommittedOffsets; // Partition stream id -> set of offsets.
    THashMap<ui64, NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent> UnconfirmedDestroys; // Partition stream id -> destroy events.
    THashMap<ui64, NYdb::NPersQueue::TPartitionStream::TPtr> CurrentPartitionStreams;
};

} // namespace NPersQueue
