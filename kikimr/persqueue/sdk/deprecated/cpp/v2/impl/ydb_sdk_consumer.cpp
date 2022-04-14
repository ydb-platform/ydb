#include "ydb_sdk_consumer.h"
#include "persqueue_p.h"

#include <ydb/library/persqueue/topic_parser_public/topic_parser.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>
#include <util/generic/is_in.h>
#include <util/string/builder.h>

namespace NPersQueue {

static TError MakeError(const TString& description, NErrorCode::EErrorCode code) {
    TError error;
    error.SetDescription(description);
    error.SetCode(code);
    return error;
}

static TString MakeLegacyTopicName(const NYdb::NPersQueue::TPartitionStream::TPtr& partitionStream) {
    // rt3.man--account--topic
    return BuildFullTopicName(partitionStream->GetTopicPath(), partitionStream->GetCluster());
}

static NErrorCode::EErrorCode ToErrorCode(NYdb::EStatus status) {
    switch (status) {
    case NYdb::EStatus::STATUS_UNDEFINED:
        return NErrorCode::ERROR;
    case NYdb::EStatus::SUCCESS:
        return NErrorCode::OK;
    case NYdb::EStatus::BAD_REQUEST:
        return NErrorCode::BAD_REQUEST;
    case NYdb::EStatus::UNAUTHORIZED:
        return NErrorCode::ACCESS_DENIED;
    case NYdb::EStatus::INTERNAL_ERROR:
        return NErrorCode::ERROR;
    case NYdb::EStatus::ABORTED:
        [[fallthrough]];
    case NYdb::EStatus::UNAVAILABLE:
        return NErrorCode::ERROR;
    case NYdb::EStatus::OVERLOADED:
        return NErrorCode::OVERLOAD;
    case NYdb::EStatus::SCHEME_ERROR:
        return NErrorCode::UNKNOWN_TOPIC;
    case NYdb::EStatus::GENERIC_ERROR:
        [[fallthrough]];
    case NYdb::EStatus::TIMEOUT:
        [[fallthrough]];
    case NYdb::EStatus::BAD_SESSION:
        [[fallthrough]];
    case NYdb::EStatus::PRECONDITION_FAILED:
        [[fallthrough]];
    case NYdb::EStatus::ALREADY_EXISTS:
        [[fallthrough]];
    case NYdb::EStatus::NOT_FOUND:
        [[fallthrough]];
    case NYdb::EStatus::SESSION_EXPIRED:
        [[fallthrough]];
    case NYdb::EStatus::CANCELLED:
        [[fallthrough]];
    case NYdb::EStatus::UNDETERMINED:
        [[fallthrough]];
    case NYdb::EStatus::UNSUPPORTED:
        [[fallthrough]];
    case NYdb::EStatus::SESSION_BUSY:
        [[fallthrough]];
    case NYdb::EStatus::TRANSPORT_UNAVAILABLE:
        [[fallthrough]];
    case NYdb::EStatus::CLIENT_RESOURCE_EXHAUSTED:
        [[fallthrough]];
    case NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED:
        [[fallthrough]];
    case NYdb::EStatus::CLIENT_INTERNAL_ERROR:
        [[fallthrough]];
    case NYdb::EStatus::CLIENT_CANCELLED:
        [[fallthrough]];
    case NYdb::EStatus::CLIENT_UNAUTHENTICATED:
        [[fallthrough]];
    case NYdb::EStatus::CLIENT_CALL_UNIMPLEMENTED:
        [[fallthrough]];
    case NYdb::EStatus::CLIENT_OUT_OF_RANGE:
        [[fallthrough]];
    case NYdb::EStatus::CLIENT_DISCOVERY_FAILED:
        [[fallthrough]];
    case NYdb::EStatus::CLIENT_LIMITS_REACHED:
        return NErrorCode::ERROR;
    }
}

TYdbSdkCompatibilityConsumer::TYdbSdkCompatibilityConsumer(const TConsumerSettings& settings,
                                                           std::shared_ptr<void> destroyEventRef,
                                                           TIntrusivePtr<TPQLibPrivate> pqLib,
                                                           TIntrusivePtr<ILogger> logger,
                                                           NYdb::NPersQueue::TPersQueueClient& client)
    : IConsumerImpl(std::move(destroyEventRef), std::move(pqLib))
    , Settings(settings)
    , Logger(std::move(logger))
    , Client(client)
{
}

TYdbSdkCompatibilityConsumer::~TYdbSdkCompatibilityConsumer() {
    Destroy("Destructor called");
}

void TYdbSdkCompatibilityConsumer::Init() {
    YdbSdkSettings = MakeReadSessionSettings(); // weak_from_this() is used here.
}

static const THashSet<TString> FederationClusters = {
    "iva",
    "man",
    "myt",
    "sas",
    "vla",
};

static TString FindClusterFromEndpoint(const TString& endpoint) {
    size_t pos = endpoint.find(".logbroker.yandex.net");
    if (pos == TString::npos) {
        pos = endpoint.find(".logbroker-prestable.yandex.net");
        if (pos == TString::npos) {
            return {}; // something strange or cross dc cloud cluster.
        }
    }
    TString prefix = endpoint.substr(0, pos);
    if (IsIn(FederationClusters, prefix)) {
        return prefix;
    }
    return {}; // no cluster in cross dc federation.
}

NYdb::NPersQueue::TReadSessionSettings TYdbSdkCompatibilityConsumer::MakeReadSessionSettings() {
    NYdb::NPersQueue::TReadSessionSettings settings;
    for (const TString& topic : Settings.Topics) {
        settings.AppendTopics(topic);
    }
    settings.ConsumerName(Settings.ClientId);
    settings.MaxMemoryUsageBytes(Settings.MaxMemoryUsage);
    if (Settings.MaxTimeLagMs) {
        settings.MaxTimeLag(TDuration::MilliSeconds(Settings.MaxTimeLagMs));
    }
    if (Settings.ReadTimestampMs) {
        settings.StartingMessageTimestamp(TInstant::MilliSeconds(Settings.ReadTimestampMs));
    }
    if (Settings.PartitionGroups.size()) {
        Y_ENSURE(settings.Topics_.size() == 1);
        for (ui64 group : Settings.PartitionGroups) {
            settings.Topics_[0].AppendPartitionGroupIds(group);
        }
    }
    if (Settings.ReconnectOnFailure || Settings.ReadFromAllClusterSources) { // ReadFromAllClusterSources implies ReconnectOnFailure and MaxAttempts == inf.
        size_t maxRetries = Settings.MaxAttempts;
        if (Settings.ReadFromAllClusterSources) {
            // Compatibility.
            maxRetries = std::numeric_limits<size_t>::max();
        }

        if (Settings.UseV2RetryPolicyInCompatMode) {
            settings.RetryPolicy(
                    NYdb::NPersQueue::IRetryPolicy::GetExponentialBackoffPolicy(
                            Settings.ReconnectionDelay,
                            Settings.ReconnectionDelay,
                            Settings.MaxReconnectionDelay,
                            maxRetries,
                            TDuration::Max(),
                            2.0,
                            NYdb::NPersQueue::GetRetryErrorClassV2
            ));
        } else {
            settings.RetryPolicy(
                    NYdb::NPersQueue::IRetryPolicy::GetExponentialBackoffPolicy(
                            Settings.ReconnectionDelay,
                            Settings.ReconnectionDelay,
                            Settings.MaxReconnectionDelay,
                            maxRetries
            ));
        }
    } else {
        settings.RetryPolicy(NYdb::NPersQueue::IRetryPolicy::GetNoRetryPolicy());
    }
    if (Settings.ReadFromAllClusterSources) {
        settings.ReadAll();
    } else {
        if (TString cluster = FindClusterFromEndpoint(Settings.Server.Address)) {
            if (Settings.ReadMirroredPartitions) {
                settings.ReadMirrored(cluster);
            } else {
                settings.ReadOriginal({ cluster });
            }
        } else {
            settings.ReadAll();
        }
    }
    if (Settings.DisableCDS) {
        settings.DisableClusterDiscovery(true);
    }

    settings.EventHandlers_.HandlersExecutor(NYdb::NPersQueue::CreateThreadPoolExecutorAdapter(PQLib->GetQueuePool().GetQueuePtr(this)));
    {
        auto weakThis = weak_from_this();
        settings.EventHandlers_.SessionClosedHandler(
            [weakThis](const NYdb::NPersQueue::TSessionClosedEvent& event) {
                if (auto sharedThis = weakThis.lock()) {
                    const TString description = event.GetIssues().ToString();
                    sharedThis->Destroy(description, ToErrorCode(event.GetStatus()));
                }
            }
        );
    }

    return settings;
}

NThreading::TFuture<TConsumerCreateResponse> TYdbSdkCompatibilityConsumer::Start(TInstant) noexcept {
    ReadSession = Client.CreateReadSession(YdbSdkSettings);
    SessionId = ReadSession->GetSessionId();
    DEBUG_LOG("Create read session", "", SessionId);
    SubscribeToNextEvent();
    TReadResponse resp;
    resp.MutableInit();
    return NThreading::MakeFuture<TConsumerCreateResponse>(TConsumerCreateResponse(std::move(resp)));
}

NThreading::TFuture<TError> TYdbSdkCompatibilityConsumer::IsDead() noexcept {
    return DeadPromise.GetFuture();
}

void TYdbSdkCompatibilityConsumer::Destroy(const TError& description) {
    if (DeadPromise.HasValue()) {
        return;
    }

    WARN_LOG("Destroying consumer: " << description, "", SessionId);

    while (!Requests.empty()) {
        NPersQueue::TReadResponse resp;
        *resp.MutableError() = description;
        Requests.front().SetValue(TConsumerMessage(std::move(resp)));
        Requests.pop();
    }

    if (ReadSession) {
        ReadSession->Close(TDuration::Zero());
        ReadSession = nullptr;
    }

    DeadPromise.SetValue(description);

    DestroyPQLibRef();
}

void TYdbSdkCompatibilityConsumer::Destroy(const TString& description, NErrorCode::EErrorCode code) {
    Destroy(MakeError(description, code));
}

void TYdbSdkCompatibilityConsumer::Cancel() {
    Destroy(GetCancelReason());
}

void TYdbSdkCompatibilityConsumer::GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept {
    if (DeadPromise.HasValue()) {
        NPersQueue::TReadResponse resp;
        *resp.MutableError() = DeadPromise.GetFuture().GetValue();
        promise.SetValue(TConsumerMessage(std::move(resp)));
        return;
    }
    Requests.push(promise);
    AnswerToRequests();
    SubscribeToNextEvent();
}

void TYdbSdkCompatibilityConsumer::Commit(const TVector<ui64>& cookies) noexcept {
    NPersQueue::TReadResponse resp;
    for (ui64 cookie : cookies) {
        if (cookie >= NextCookie) {
            Destroy(TStringBuilder() << "Wrong cookie " << cookie, NErrorCode::WRONG_COOKIE);
            return;
        }
        auto offsetsIt = CookieToOffsets.find(cookie);
        if (offsetsIt != CookieToOffsets.end()) {
            offsetsIt->second.first.Commit();
            CookiesRequestedToCommit[offsetsIt->second.second].emplace(cookie);
            CookieToOffsets.erase(offsetsIt);
        } else {
            resp.MutableCommit()->AddCookie(cookie);
        }
    }
    if (resp.HasCommit()) {
        AddResponse(std::move(resp));
    }
}

void TYdbSdkCompatibilityConsumer::RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept {
    Y_UNUSED(topic);
    Y_UNUSED(partition);
    auto partitionStreamIt = CurrentPartitionStreams.find(generation);
    if (partitionStreamIt != CurrentPartitionStreams.end()) {
        partitionStreamIt->second->RequestStatus();
    }
}

void TYdbSdkCompatibilityConsumer::AnswerToRequests() {
    TVector<NYdb::NPersQueue::TReadSessionEvent::TEvent> events;
    do {
        events = ReadSession->GetEvents();
        for (NYdb::NPersQueue::TReadSessionEvent::TEvent& event : events) {
            if (ReadSession) {
                std::visit([this](auto&& ev) { return HandleEvent(std::move(ev)); }, event);
            }
        }
    } while (!events.empty() && ReadSession);

    while (!Requests.empty() && !Responses.empty()) {
        Requests.front().SetValue(std::move(Responses.front()));
        Requests.pop();
        Responses.pop();
    }
}

void TYdbSdkCompatibilityConsumer::SubscribeToNextEvent() {
    if (!SubscribedToNextEvent && !Requests.empty() && ReadSession) {
        SubscribedToNextEvent = true;
        auto weakThis = weak_from_this();
        auto future = ReadSession->WaitEvent();
        Cerr << "SUBSCRIBING " << ReadSession->GetSessionId() << " future" << future.StateId()->Value() << "\n";
        PQLib->Subscribe(future, this, [weakThis](const NThreading::TFuture<void>&) {
            if (auto sharedThis = weakThis.lock()) {
                sharedThis->OnReadSessionEvent();
            }
        });
    }
}

void TYdbSdkCompatibilityConsumer::OnReadSessionEvent() {
    if (DeadPromise.HasValue()) {
        return;
    }

    SubscribedToNextEvent = false;
    AnswerToRequests();
    SubscribeToNextEvent();
}

void TYdbSdkCompatibilityConsumer::HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent&& event) {
    if (Settings.UseLockSession) {
        auto& offsetSet = PartitionStreamToUncommittedOffsets[event.GetPartitionStream()->GetPartitionStreamId()];
        // Messages could contain holes in offset, but later commit ack will tell us right border.
        // So we can easily insert the whole interval with holes included.
        // It will be removed from set by specifying proper right border.
        offsetSet.InsertInterval(event.GetMessages().front().GetOffset(), event.GetMessages().back().GetOffset() + 1);
    }
    const ui64 cookie = NextCookie++;
    const auto& partitionStream = event.GetPartitionStream();
    auto& offsets = CookieToOffsets[cookie];
    offsets.first.Add(event);
    offsets.second = partitionStream->GetPartitionStreamId();
    PartitionStreamToCookies[partitionStream->GetPartitionStreamId()].emplace(cookie);
    NPersQueue::TReadResponse resp;
    auto& dataResp = *resp.MutableData();
    dataResp.SetCookie(cookie);
    auto& batchResp = *dataResp.AddMessageBatch();
    batchResp.SetTopic(MakeLegacyTopicName(partitionStream));
    batchResp.SetPartition(partitionStream->GetPartitionId());
    ui64 maxOffset = 0;
    for (auto&& msg : event.GetMessages()) {
        auto& msgResp = *batchResp.AddMessage();
        msgResp.SetOffset(msg.GetOffset());
        maxOffset = Max(maxOffset, msg.GetOffset());
        msgResp.SetData(msg.GetData());
        auto& metaResp = *msgResp.MutableMeta();
        metaResp.SetSourceId(msg.GetMessageGroupId());
        metaResp.SetSeqNo(msg.GetSeqNo());
        metaResp.SetCreateTimeMs(msg.GetCreateTime().MilliSeconds());
        metaResp.SetWriteTimeMs(msg.GetWriteTime().MilliSeconds());
        metaResp.SetIp(msg.GetIp());
        for (auto&& [k, v] : msg.GetMeta()->Fields) {
            auto& kvResp = *metaResp.MutableExtraFields()->AddItems();
            kvResp.SetKey(k);
            kvResp.SetValue(v);
        }
    }
    MaxOffsetToCookie[std::make_pair(partitionStream->GetPartitionStreamId(), maxOffset)] = cookie;
    AddResponse(std::move(resp));
}
void TYdbSdkCompatibilityConsumer::HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TCommitAcknowledgementEvent&& event) {
    const ui64 partitionStreamId = event.GetPartitionStream()->GetPartitionStreamId();
    if (Settings.UseLockSession) {
        auto& offsetSet = PartitionStreamToUncommittedOffsets[partitionStreamId];
        if (offsetSet.EraseInterval(0, event.GetCommittedOffset())) { // Remove some offsets.
            if (offsetSet.Empty()) { // No offsets left.
                auto unconfirmedDestroyIt = UnconfirmedDestroys.find(partitionStreamId);
                if (unconfirmedDestroyIt != UnconfirmedDestroys.end()) {
                    // Confirm and forget about this partition stream.
                    unconfirmedDestroyIt->second.Confirm();
                    UnconfirmedDestroys.erase(unconfirmedDestroyIt);
                    PartitionStreamToUncommittedOffsets.erase(partitionStreamId);
                }
            }
        }
    }
    const auto offsetPair = std::make_pair(partitionStreamId, event.GetCommittedOffset() - 1);
    auto cookieIt = MaxOffsetToCookie.lower_bound(offsetPair);
    std::vector<ui64> cookies;
    auto end = cookieIt;
    auto begin = end;
    if (cookieIt != MaxOffsetToCookie.end() && cookieIt->first == offsetPair) {
        cookies.push_back(cookieIt->second);
        ++end;
    }
    while (cookieIt != MaxOffsetToCookie.begin()) {
        --cookieIt;
        if (cookieIt->first.first == partitionStreamId) {
            cookies.push_back(cookieIt->second);
            begin = cookieIt;
        } else {
            break;
        }
    }
    if (begin != end) {
        MaxOffsetToCookie.erase(begin, end);
    }

    NPersQueue::TReadResponse resp;
    auto& respCommit = *resp.MutableCommit();
    auto& partitionStreamCookies = PartitionStreamToCookies[partitionStreamId];
    auto& committedCookies = CookiesRequestedToCommit[partitionStreamId];
    for (auto committedCookieIt = cookies.rbegin(), committedCookieEnd = cookies.rend(); committedCookieIt != committedCookieEnd; ++committedCookieIt) {
        const ui64 cookie = *committedCookieIt;
        respCommit.AddCookie(cookie);
        partitionStreamCookies.erase(cookie);
        committedCookies.erase(cookie);
    }
    AddResponse(std::move(resp));
}

void TYdbSdkCompatibilityConsumer::HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent&& event) {
    if (Settings.UseLockSession) {
        Y_VERIFY(PartitionStreamToUncommittedOffsets[event.GetPartitionStream()->GetPartitionStreamId()].Empty());

        NPersQueue::TReadResponse resp;
        auto& lockResp = *resp.MutableLock();
        const auto& partitionStream = event.GetPartitionStream();
        lockResp.SetTopic(MakeLegacyTopicName(partitionStream));
        lockResp.SetPartition(partitionStream->GetPartitionId());
        lockResp.SetReadOffset(event.GetCommittedOffset());
        lockResp.SetEndOffset(event.GetEndOffset());
        lockResp.SetGeneration(partitionStream->GetPartitionStreamId());
        NThreading::TPromise<TLockInfo> confirmPromise = NThreading::NewPromise<TLockInfo>();
        confirmPromise.GetFuture().Subscribe([event = std::move(event)](const NThreading::TFuture<TLockInfo>& infoFuture) mutable {
            const TLockInfo& info = infoFuture.GetValue();
            event.Confirm(info.ReadOffset, info.CommitOffset);
        }); // It doesn't matter in what thread this callback will be called.
        AddResponse(std::move(resp), std::move(confirmPromise));
    } else {
        event.Confirm();
    }
}

void TYdbSdkCompatibilityConsumer::HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent&& event) {
    const ui64 partitionStreamId = event.GetPartitionStream()->GetPartitionStreamId();
    CurrentPartitionStreams[partitionStreamId] = event.GetPartitionStream();
    if (Settings.UseLockSession) {
        const auto partitionStream = event.GetPartitionStream();

        Y_VERIFY(UnconfirmedDestroys.find(partitionStreamId) == UnconfirmedDestroys.end());
        if (PartitionStreamToUncommittedOffsets[partitionStreamId].Empty() || Settings.BalanceRightNow) {
            PartitionStreamToUncommittedOffsets.erase(partitionStreamId);
            event.Confirm();
        } else {
            UnconfirmedDestroys.emplace(partitionStreamId, std::move(event));
        }

        NPersQueue::TReadResponse resp;
        auto& releaseResp = *resp.MutableRelease();
        releaseResp.SetTopic(MakeLegacyTopicName(partitionStream));
        releaseResp.SetPartition(partitionStream->GetPartitionId());
        releaseResp.SetCanCommit(true);
        releaseResp.SetGeneration(partitionStream->GetPartitionStreamId());
        AddResponse(std::move(resp));
    } else {
        event.Confirm();
    }
}

void TYdbSdkCompatibilityConsumer::HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamStatusEvent&& event) {
    NPersQueue::TReadResponse resp;
    auto& statusResp = *resp.MutablePartitionStatus();
    const auto& partitionStream = event.GetPartitionStream();
    statusResp.SetGeneration(partitionStream->GetPartitionStreamId());
    statusResp.SetTopic(MakeLegacyTopicName(partitionStream));
    statusResp.SetPartition(partitionStream->GetPartitionId());
    statusResp.SetCommittedOffset(event.GetCommittedOffset());
    statusResp.SetEndOffset(event.GetEndOffset());
    statusResp.SetWriteWatermarkMs(event.GetWriteWatermark().MilliSeconds());
    AddResponse(std::move(resp));
}

void TYdbSdkCompatibilityConsumer::HandleEvent(NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent&& event) {
    const ui64 partitionStreamId = event.GetPartitionStream()->GetPartitionStreamId();
    CurrentPartitionStreams.erase(partitionStreamId);

    {
        NPersQueue::TReadResponse resp;
        auto& respCommit = *resp.MutableCommit();
        for (ui64 cookie : CookiesRequestedToCommit[partitionStreamId]) {
            respCommit.AddCookie(cookie);
        }
        if (resp.GetCommit().CookieSize()) {
            AddResponse(std::move(resp));
        }
        CookiesRequestedToCommit.erase(partitionStreamId);
    }

    {
        for (ui64 cookie : PartitionStreamToCookies[partitionStreamId]) {
            CookieToOffsets.erase(cookie);
        }
        PartitionStreamToCookies.erase(partitionStreamId);
    }

    {
        auto begin = MaxOffsetToCookie.lower_bound(std::make_pair(partitionStreamId, 0)); // The first available offset.
        if (begin != MaxOffsetToCookie.end() && begin->first.first == partitionStreamId) {
            auto end = begin;
            while (end != MaxOffsetToCookie.end() && end->first.first == partitionStreamId) {
                ++end;
            }
            MaxOffsetToCookie.erase(begin, end);
        }
    }

    if (Settings.UseLockSession) {
        PartitionStreamToUncommittedOffsets.erase(partitionStreamId);
        UnconfirmedDestroys.erase(partitionStreamId);

        if (event.GetReason() != NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent::EReason::DestroyConfirmedByUser) {
            NPersQueue::TReadResponse resp;
            auto& releaseResp = *resp.MutableRelease();
            const auto& partitionStream = event.GetPartitionStream();
            releaseResp.SetTopic(MakeLegacyTopicName(partitionStream));
            releaseResp.SetPartition(partitionStream->GetPartitionId());
            releaseResp.SetCanCommit(false);
            releaseResp.SetGeneration(partitionStream->GetPartitionStreamId());
            AddResponse(std::move(resp));
        }
    }
}

void TYdbSdkCompatibilityConsumer::HandleEvent(NYdb::NPersQueue::TSessionClosedEvent&& event) {
    Destroy(event.GetIssues().ToString(), ToErrorCode(event.GetStatus()));
}

void TYdbSdkCompatibilityConsumer::AddResponse(TReadResponse&& response, NThreading::TPromise<TLockInfo>&& readyToRead) {
    Responses.emplace(std::move(response), std::move(readyToRead));
}

void TYdbSdkCompatibilityConsumer::AddResponse(TReadResponse&& response) {
    Responses.emplace(std::move(response));
}

} // namespace NPersQueue
