#include "multicluster_consumer.h"
#include "persqueue_p.h"

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>

#include <library/cpp/containers/intrusive_rb_tree/rb_tree.h>

#include <util/generic/guid.h>
#include <util/generic/strbuf.h>

namespace NPersQueue {

struct TMultiClusterConsumer::TSubconsumerInfo {
    TSubconsumerInfo(std::shared_ptr<IConsumerImpl> consumer);

    std::shared_ptr<IConsumerImpl> Consumer;
    NThreading::TFuture<TConsumerCreateResponse> StartFuture;
    NThreading::TFuture<TError> DeadFuture;
    bool StartResponseWasGot = false; // We call methods of this consumer only after this flag was set.

    std::deque<NThreading::TFuture<TConsumerMessage>> PendingRequests;
};

namespace {

struct TConsumerCookieKey;
struct TUserCookieMappingItem;
struct TConsumerCookieMappingItem;

struct TConsumerCookieKey {
    size_t Subconsumer = 0;
    ui64 ConsumerCookie = 0;

    TConsumerCookieKey(size_t subconsumer, ui64 consumerCookie)
        : Subconsumer(subconsumer)
        , ConsumerCookie(consumerCookie)
    {
    }

    bool operator<(const TConsumerCookieKey& other) const {
        return std::make_pair(Subconsumer, ConsumerCookie) < std::make_pair(other.Subconsumer, other.ConsumerCookie);
    }
};

struct TUserCookieCmp {
    static ui64 GetKey(const TUserCookieMappingItem& item);

    static bool Compare(ui64 l, const TUserCookieMappingItem& r) {
        return l < GetKey(r);
    }

    static bool Compare(const TUserCookieMappingItem& l, ui64 r) {
        return GetKey(l) < r;
    }

    static bool Compare(const TUserCookieMappingItem& l, const TUserCookieMappingItem& r) {
        return GetKey(l) < GetKey(r);
    }
};

struct TConsumerCookieCmp {
    static TConsumerCookieKey GetKey(const TConsumerCookieMappingItem& item);

    static bool Compare(const TConsumerCookieKey& l, const TConsumerCookieMappingItem& r) {
        return l < GetKey(r);
    }

    static bool Compare(const TConsumerCookieMappingItem& l, const TConsumerCookieKey& r) {
        return GetKey(l) < r;
    }

    static bool Compare(const TConsumerCookieMappingItem& l, const TConsumerCookieMappingItem& r) {
        return GetKey(l) < GetKey(r);
    }
};

struct TUserCookieMappingItem : public TRbTreeItem<TUserCookieMappingItem, TUserCookieCmp> {
};

struct TConsumerCookieMappingItem : public TRbTreeItem<TConsumerCookieMappingItem, TConsumerCookieCmp> {
};

bool IsRetryable(const grpc::Status& status) {
    switch (status.error_code()) {
    case grpc::OK:
    case grpc::CANCELLED:
    case grpc::INVALID_ARGUMENT:
    case grpc::NOT_FOUND:
    case grpc::ALREADY_EXISTS:
    case grpc::PERMISSION_DENIED:
    case grpc::UNAUTHENTICATED:
    case grpc::FAILED_PRECONDITION:
    case grpc::ABORTED:
    case grpc::OUT_OF_RANGE:
    case grpc::UNIMPLEMENTED:
        return false;

    case grpc::UNKNOWN:
    case grpc::DEADLINE_EXCEEDED:
    case grpc::RESOURCE_EXHAUSTED:
    case grpc::INTERNAL:
    case grpc::UNAVAILABLE:
    case grpc::DATA_LOSS:
    case grpc::DO_NOT_USE:
        return true;
    }
}

} // namespace

struct TMultiClusterConsumer::TCookieMappingItem : public TUserCookieMappingItem, public TConsumerCookieMappingItem {
    TCookieMappingItem(size_t subconsumer, ui64 consumerCookie, ui64 userCookie)
        : Subconsumer(subconsumer)
        , ConsumerCookie(consumerCookie)
        , UserCookie(userCookie)
    {
    }

    size_t Subconsumer = 0;
    ui64 ConsumerCookie = 0;
    ui64 UserCookie = 0;
};

namespace {

ui64 TUserCookieCmp::GetKey(const TUserCookieMappingItem& item) {
    return static_cast<const TMultiClusterConsumer::TCookieMappingItem&>(item).UserCookie;
}

TConsumerCookieKey TConsumerCookieCmp::GetKey(const TConsumerCookieMappingItem& item) {
    const auto& src = static_cast<const TMultiClusterConsumer::TCookieMappingItem&>(item);
    return { src.Subconsumer, src.ConsumerCookie };
}

} // namespace

struct TMultiClusterConsumer::TCookieMapping {
    using TUserCookieTree = TRbTree<TUserCookieMappingItem, TUserCookieCmp>;
    using TConsumerCookieTree = TRbTree<TConsumerCookieMappingItem, TConsumerCookieCmp>;

    struct TConsumerCookieDestroy : public TConsumerCookieTree::TDestroy {
        void operator()(TConsumerCookieMappingItem& item) const {
            TDestroy::operator()(item); // Remove from tree
            delete static_cast<TCookieMappingItem*>(&item);
        }
    };

    ~TCookieMapping() {
        UserCookieTree.ForEachNoOrder(TUserCookieTree::TDestroy());
        UserCookieTree.Init();

        ConsumerCookieTree.ForEachNoOrder(TConsumerCookieDestroy());
        ConsumerCookieTree.Init();
    }

    ui64 AddMapping(size_t subconsumer, ui64 cookie) { // Returns user cookie
        auto* newItem = new TCookieMappingItem(subconsumer, cookie, NextCookie++);
        UserCookieTree.Insert(newItem);
        ConsumerCookieTree.Insert(newItem);
        return newItem->UserCookie;
    }

    TCookieMappingItem* FindMapping(ui64 userCookie) {
        return DownCast(UserCookieTree.Find(userCookie));
    }

    TCookieMappingItem* FindMapping(size_t subconsumer, ui64 cookie) {
        return DownCast(ConsumerCookieTree.Find(TConsumerCookieKey(subconsumer, cookie)));
    }

    void RemoveUserCookieMapping(TCookieMappingItem* item) {
        Y_ASSERT(item);
        static_cast<TUserCookieMappingItem*>(item)->UnLink();
    }

    void RemoveMapping(TCookieMappingItem* item) {
        Y_ASSERT(item);
        Y_ASSERT(!static_cast<TUserCookieMappingItem*>(item)->ParentTree());
        static_cast<TUserCookieMappingItem*>(item)->UnLink(); // Just in case
        static_cast<TConsumerCookieMappingItem*>(item)->UnLink();
        delete item;
    }

private:
    template <class T>
    static TCookieMappingItem* DownCast(T* item) {
        if (item) {
            return static_cast<TCookieMappingItem*>(item);
        } else {
            return nullptr;
        }
    }

    TUserCookieTree UserCookieTree;
    TConsumerCookieTree ConsumerCookieTree;
    ui64 NextCookie = 1;
};

TMultiClusterConsumer::TMultiClusterConsumer(const TConsumerSettings& settings, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger)
    : IConsumerImpl(std::move(destroyEventRef), std::move(pqLib))
    , Settings(settings)
    , Logger(std::move(logger))
    , SessionId(CreateGuidAsString())
    , CookieMapping(MakeHolder<TCookieMapping>())
{
    PatchSettings();
}

TMultiClusterConsumer::~TMultiClusterConsumer() {
    Destroy();
}

void TMultiClusterConsumer::PatchSettings() {
    if (!Settings.ReconnectOnFailure) {
        WARN_LOG("Ignoring ReconnectOnFailure=false option for multicluster consumer", "", SessionId);
        Settings.ReconnectOnFailure = true;
    }
    if (Settings.MaxAttempts != std::numeric_limits<unsigned>::max()) {
        WARN_LOG("Ignoring MaxAttempts option for multicluster consumer", "", SessionId);
        Settings.MaxAttempts = std::numeric_limits<unsigned>::max();
    }
}

NThreading::TFuture<TConsumerCreateResponse> TMultiClusterConsumer::Start(TInstant deadline) noexcept {
    Y_VERIFY(State == EState::Created);
    StartClusterDiscovery(deadline);
    return StartPromise.GetFuture();
}

NThreading::TFuture<TError> TMultiClusterConsumer::IsDead() noexcept {
    return IsDeadPromise.GetFuture();
}

void TMultiClusterConsumer::ScheduleClusterDiscoveryRetry(TInstant deadline) {
    const TInstant now = TInstant::Now();
    if (ClusterDiscoveryAttemptsDone >= Settings.MaxAttempts || now >= deadline) {
        Destroy(TStringBuilder() << "Failed " << ClusterDiscoveryAttemptsDone << " cluster discovery attempts", NErrorCode::CREATE_TIMEOUT);
        return;
    }

    if (!IsRetryable(ClusterDiscoverResult->first)) {
        Destroy(TStringBuilder() << "Cluster discovery failed: " << static_cast<int>(ClusterDiscoverResult->first.error_code()));
        return;
    }

    TDuration delay = Min(Settings.MaxReconnectionDelay, ClusterDiscoveryAttemptsDone * Settings.ReconnectionDelay, deadline - now);
    std::weak_ptr<TMultiClusterConsumer> weakRef = shared_from_this();
    PQLib->GetScheduler().Schedule(delay, this, [weakRef, deadline]() {
        if (auto self = weakRef.lock()) {
            self->StartClusterDiscovery(deadline);
        }
    });
}

void TMultiClusterConsumer::StartClusterDiscovery(TInstant deadline) {
    if (State == EState::Dead) {
        return;
    }

    State = EState::WaitingClusterDiscovery;
    ++ClusterDiscoveryAttemptsDone;
    DEBUG_LOG("Starting cluster discovery", "", SessionId);

    auto discoverer = MakeIntrusive<TConsumerChannelOverCdsImpl>(Settings, PQLib.Get(), Logger);
    discoverer->Start();
    ClusterDiscoverResult = discoverer->GetResultPtr();
    ClusterDiscoverer = std::move(discoverer);

    std::weak_ptr<TMultiClusterConsumer> self = shared_from_this();
    auto handler = [self, deadline](const auto&) {
        if (auto selfShared = self.lock()) {
            selfShared->OnClusterDiscoveryDone(deadline);
        }
    };
    PQLib->Subscribe(ClusterDiscoverer->GetChannel(), this, handler);
}

void TMultiClusterConsumer::OnClusterDiscoveryDone(TInstant deadline) {
    if (State != EState::WaitingClusterDiscovery) {
        return;
    }

    ClusterDiscoverer = nullptr; // delete
    if (!ClusterDiscoverResult->first.ok()) {
        INFO_LOG("Failed to discover clusters. Grpc error: " << static_cast<int>(ClusterDiscoverResult->first.error_code()), "", SessionId);
        ScheduleClusterDiscoveryRetry(deadline); // Destroys if we shouldn't retry.
        return;
    }

    if (static_cast<size_t>(ClusterDiscoverResult->second.read_sessions_clusters_size()) != Settings.Topics.size()) {
        Destroy("Got unexpected cluster discovery result");
        return;
    }

    if (deadline != TInstant::Max()) {
        const TInstant now = TInstant::Now();
        if (now > deadline) {
            Destroy("Start timeout", NErrorCode::CREATE_TIMEOUT);
            return;
        }

        std::weak_ptr<TMultiClusterConsumer> self = shared_from_this();
        auto onStartTimeout = [self] {
            if (auto selfShared = self.lock()) {
                selfShared->OnStartTimeout();
            }
        };
        StartDeadlineCallback =
            PQLib->GetScheduler().Schedule(deadline, this, onStartTimeout);
    }

    State = EState::StartingSubconsumers;
    DEBUG_LOG("Starting subconsumers", "", SessionId);

    // Group topics by clusters.
    THashMap<TString, TVector<TString>> clusterToTopics;
    for (size_t topicIndex = 0, topicsCount = Settings.Topics.size(); topicIndex < topicsCount; ++topicIndex) {
        const Ydb::PersQueue::ClusterDiscovery::ReadSessionClusters& clusters = ClusterDiscoverResult->second.read_sessions_clusters(topicIndex);
        const TString& topic = Settings.Topics[topicIndex];
        for (const Ydb::PersQueue::ClusterDiscovery::ClusterInfo& clusterInfo : clusters.clusters()) {
            clusterToTopics[clusterInfo.endpoint()].push_back(topic);
        }
    }

    if (clusterToTopics.empty()) {
        Destroy("Got empty endpoint set from cluster discovery");
        return;
    }

    // Start consumer on every cluster with its topics set.
    for (auto&& [clusterEndpoint, topics] : clusterToTopics) {
        // Settings.
        TConsumerSettings settings = Settings;
        settings.Server = ApplyClusterEndpoint(settings.Server, clusterEndpoint);
        settings.ReadFromAllClusterSources = false;
        settings.Topics = std::move(topics);
        settings.Unpack = false; // Unpack is being done in upper level decompressing consumer.
        settings.MaxMemoryUsage = Max(settings.MaxMemoryUsage / clusterToTopics.size(), static_cast<size_t>(1));
        if (settings.MaxUncommittedSize > 0) { // Limit is enabled.
            settings.MaxUncommittedSize = Max(settings.MaxUncommittedSize / clusterToTopics.size(), static_cast<size_t>(1));
        }
        if (settings.MaxUncommittedCount > 0) { // Limit is enabled.
            settings.MaxUncommittedCount = Max(settings.MaxUncommittedCount / clusterToTopics.size(), static_cast<size_t>(1));
        }
        const size_t subconsumerIndex = Subconsumers.size();

        // Create subconsumer.
        Subconsumers.push_back(PQLib->CreateRawRetryingConsumer(settings, DestroyEventRef, Logger));

        // Subscribe on start.
        std::weak_ptr<TMultiClusterConsumer> self = shared_from_this();
        auto handler = [self, subconsumerIndex](const auto&) {
            if (auto selfShared = self.lock()) {
                selfShared->OnSubconsumerStarted(subconsumerIndex);
            }
        };
        PQLib->Subscribe(Subconsumers.back().StartFuture, this, handler);
    }
}

void TMultiClusterConsumer::OnStartTimeout() {
    if (State == EState::Dead || State == EState::Working) {
        return;
    }

    StartDeadlineCallback = nullptr;
    Destroy("Start timeout", NErrorCode::CREATE_TIMEOUT);
}


void TMultiClusterConsumer::OnSubconsumerStarted(size_t subconsumerIndex) {
    if (State == EState::Dead) {
        return;
    }

    auto& subconsumerInfo = Subconsumers[subconsumerIndex];
    const auto& result = subconsumerInfo.StartFuture.GetValue();
    if (result.Response.HasError()) {
        WARN_LOG("Got error on starting subconsumer: " << result.Response.GetError(), "", SessionId);
        Destroy(result.Response.GetError());
        return;
    }

    // Process start response
    subconsumerInfo.StartResponseWasGot = true;
    INFO_LOG("Subconsumer " << subconsumerIndex << " started session with id " << result.Response.GetInit().GetSessionId(), "", SessionId);

    // Move to new state
    if (State == EState::StartingSubconsumers) {
        State = EState::Working;
        if (StartDeadlineCallback) {
            StartDeadlineCallback->TryCancel();
            StartDeadlineCallback = nullptr;
        }

        {
            TReadResponse resp;
            resp.MutableInit()->SetSessionId(SessionId);
            StartPromise.SetValue(TConsumerCreateResponse(std::move(resp)));
        }
    } else {
        RequestSubconsumers(); // Make requests for new subconsumer.
    }
}

void TMultiClusterConsumer::Commit(const TVector<ui64>& cookies) noexcept {
    if (State != EState::Working) {
        Destroy("Requesting commit, but consumer is not in working state", NErrorCode::BAD_REQUEST);
        return;
    }

    TVector<TVector<ui64>> subconsumersCookies(Subconsumers.size());
    for (ui64 userCookie : cookies) {
        TCookieMappingItem* mapping = CookieMapping->FindMapping(userCookie);
        if (!mapping) {
            Destroy(TStringBuilder() << "Wrong cookie " << userCookie, NErrorCode::WRONG_COOKIE);
            return;
        }
        Y_ASSERT(mapping->Subconsumer < Subconsumers.size());
        subconsumersCookies[mapping->Subconsumer].push_back(mapping->ConsumerCookie);
        CookieMapping->RemoveUserCookieMapping(mapping); // Avoid double commit. This will ensure error on second commit with the same cookie.
    }

    for (size_t subconsumerIndex = 0; subconsumerIndex < Subconsumers.size(); ++subconsumerIndex) {
        const TVector<ui64>& subconsumerCommitRequest = subconsumersCookies[subconsumerIndex];
        if (!subconsumerCommitRequest.empty()) {
            Subconsumers[subconsumerIndex].Consumer->Commit(subconsumerCommitRequest);
        }
    }
}

void TMultiClusterConsumer::GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept {
    if (State != EState::Working) {
        promise.SetValue(MakeResponse(MakeError("Requesting next message, but consumer is not in working state", NErrorCode::BAD_REQUEST)));
        return;
    }

    Requests.push_back(promise);
    CheckReadyResponses();
    RequestSubconsumers();
}

void TMultiClusterConsumer::RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept {
    if (State != EState::Working) {
        Destroy("Requesting partition status, but consumer is not in working state", NErrorCode::BAD_REQUEST);
        return;
    }

    const auto subconsumerIdIt = OldTopicName2Subconsumer.find(topic);
    if (subconsumerIdIt != OldTopicName2Subconsumer.end()) {
        Y_ASSERT(subconsumerIdIt->second < Subconsumers.size());
        Subconsumers[subconsumerIdIt->second].Consumer->RequestPartitionStatus(topic, partition, generation);
    } else {
        WARN_LOG("Requested partition status for topic \"" << topic << "\" (partition " << partition << ", generation " << generation << "), but there is no such lock session. Ignoring request", "", SessionId);
    }
}

void TMultiClusterConsumer::CheckReadyResponses() {
    const size_t prevCurrentSubconsumer = CurrentSubconsumer;

    do {
        if (!ProcessReadyResponses(CurrentSubconsumer)) {
            break;
        }

        // Next subconsumer in round robin way.
        ++CurrentSubconsumer;
        if (CurrentSubconsumer == Subconsumers.size()) {
            CurrentSubconsumer = 0;
        }
    } while (CurrentSubconsumer != prevCurrentSubconsumer && !Requests.empty());
}

bool TMultiClusterConsumer::ProcessReadyResponses(size_t subconsumerIndex) {
    if (Subconsumers.empty()) {
        Y_VERIFY(State == EState::Dead);
        return false;
    }
    Y_VERIFY(subconsumerIndex < Subconsumers.size());
    TSubconsumerInfo& consumerInfo = Subconsumers[subconsumerIndex];
    while (!consumerInfo.PendingRequests.empty() && !Requests.empty() && consumerInfo.PendingRequests.front().HasValue()) {
        if (!TranslateConsumerMessage(Requests.front(), consumerInfo.PendingRequests.front().ExtractValue(), subconsumerIndex)) {
            return false;
        }
        Requests.pop_front();
        consumerInfo.PendingRequests.pop_front();
    }
    return true;
}

bool TMultiClusterConsumer::TranslateConsumerMessage(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex) {
    switch (subconsumerResponse.Type) {
    case EMT_LOCK:
        return TranslateConsumerMessageLock(promise, std::move(subconsumerResponse), subconsumerIndex);
    case EMT_RELEASE:
        return TranslateConsumerMessageRelease(promise, std::move(subconsumerResponse), subconsumerIndex);
    case EMT_DATA:
        return TranslateConsumerMessageData(promise, std::move(subconsumerResponse), subconsumerIndex);
    case EMT_ERROR:
        return TranslateConsumerMessageError(promise, std::move(subconsumerResponse), subconsumerIndex);
    case EMT_STATUS:
        return TranslateConsumerMessageStatus(promise, std::move(subconsumerResponse), subconsumerIndex);
    case EMT_COMMIT:
        return TranslateConsumerMessageCommit(promise, std::move(subconsumerResponse), subconsumerIndex);
    }
}

bool TMultiClusterConsumer::TranslateConsumerMessageData(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex) {
    auto* data = subconsumerResponse.Response.MutableData();
    data->SetCookie(CookieMapping->AddMapping(subconsumerIndex, data->GetCookie()));
    promise.SetValue(std::move(subconsumerResponse));
    return true;
}

bool TMultiClusterConsumer::TranslateConsumerMessageCommit(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex) {
    for (ui64& consumerCookie : *subconsumerResponse.Response.MutableCommit()->MutableCookie()) {
        auto* mapping = CookieMapping->FindMapping(subconsumerIndex, consumerCookie);
        if (!mapping) {
            Destroy(TStringBuilder() << "Received unknown cookie " << consumerCookie << " commit");
            return false;
        }
        Y_VERIFY(mapping);
        consumerCookie = mapping->UserCookie;
        CookieMapping->RemoveMapping(mapping); // Invalidates mapping
    }
    promise.SetValue(std::move(subconsumerResponse));
    return true;
}

bool TMultiClusterConsumer::TranslateConsumerMessageLock(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex) {
    OldTopicName2Subconsumer[subconsumerResponse.Response.GetLock().GetTopic()] = subconsumerIndex;
    promise.SetValue(std::move(subconsumerResponse));
    return true;
}

bool TMultiClusterConsumer::TranslateConsumerMessageRelease(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex) {
    Y_UNUSED(subconsumerIndex);
    promise.SetValue(std::move(subconsumerResponse));
    return true;
}

bool TMultiClusterConsumer::TranslateConsumerMessageError(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex) {
    WARN_LOG("Got error from subconsumer " << subconsumerIndex << ": " << subconsumerResponse.Response.GetError(), "", SessionId);
    Destroy(subconsumerResponse.Response.GetError());
    Y_UNUSED(promise);
    return false;
}

bool TMultiClusterConsumer::TranslateConsumerMessageStatus(NThreading::TPromise<TConsumerMessage>& promise, TConsumerMessage&& subconsumerResponse, size_t subconsumerIndex) {
    Y_UNUSED(subconsumerIndex);
    promise.SetValue(std::move(subconsumerResponse));
    return true;
}

void TMultiClusterConsumer::RequestSubconsumers() {
    const size_t inflight = Requests.size();
    if (!inflight) {
        return;
    }
    TMaybe<std::weak_ptr<TMultiClusterConsumer>> maybeSelf;
    for (size_t subconsumerIndex = 0; subconsumerIndex < Subconsumers.size(); ++subconsumerIndex) {
        TSubconsumerInfo& consumerInfo = Subconsumers[subconsumerIndex];
        if (!consumerInfo.StartResponseWasGot) {
            continue; // Consumer hasn't started yet.
        }
        while (consumerInfo.PendingRequests.size() < inflight) {
            if (!maybeSelf) {
                maybeSelf.ConstructInPlace(shared_from_this()); // Don't construct it if we don't need it.
            }
            consumerInfo.PendingRequests.push_back(consumerInfo.Consumer->GetNextMessage());
            PQLib->Subscribe(consumerInfo.PendingRequests.back(),
                             this,
                             [self = *maybeSelf, subconsumerIndex](const auto&) {
                                 if (auto selfShared = self.lock()) {
                                     selfShared->ProcessReadyResponses(subconsumerIndex);
                                 }
                             });
        }
    }
}

void TMultiClusterConsumer::Destroy(const TError& description) {
    if (State == EState::Dead) {
        return;
    }
    State = EState::Dead;

    WARN_LOG("Destroying consumer with error description: " << description, "", SessionId);

    StartPromise.TrySetValue(MakeCreateResponse(description));

    if (StartDeadlineCallback) {
        StartDeadlineCallback->TryCancel();
    }

    for (auto& reqPromise : Requests) {
        reqPromise.SetValue(MakeResponse(description));
    }
    Requests.clear();
    Subconsumers.clear();

    IsDeadPromise.SetValue(description);

    DestroyPQLibRef();
}

void TMultiClusterConsumer::Destroy(const TString& description, NErrorCode::EErrorCode code) {
    Destroy(MakeError(description, code));
}

void TMultiClusterConsumer::Destroy() {
    Destroy(GetCancelReason());
}

void TMultiClusterConsumer::Cancel() {
    Destroy(GetCancelReason());
}

TError TMultiClusterConsumer::MakeError(const TString& description, NErrorCode::EErrorCode code) {
    TError error;
    error.SetDescription(description);
    error.SetCode(code);
    return error;
}

TConsumerCreateResponse TMultiClusterConsumer::MakeCreateResponse(const TError& description) {
    TReadResponse res;
    res.MutableError()->CopyFrom(description);
    return TConsumerCreateResponse(std::move(res));
}

TConsumerMessage TMultiClusterConsumer::MakeResponse(const TError& description) {
    TReadResponse res;
    res.MutableError()->CopyFrom(description);
    return TConsumerMessage(std::move(res));
}

TMultiClusterConsumer::TSubconsumerInfo::TSubconsumerInfo(std::shared_ptr<IConsumerImpl> consumer)
    : Consumer(std::move(consumer))
    , StartFuture(Consumer->Start())
    , DeadFuture(Consumer->IsDead())
{
}

} // namespace NPersQueue
