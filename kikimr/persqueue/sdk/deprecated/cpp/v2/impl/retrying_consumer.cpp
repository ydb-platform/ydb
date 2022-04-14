#include "retrying_consumer.h"

namespace {
    bool IsRetryable(const NPersQueue::TError& err) {
        switch (err.code()) {
        case NPersQueue::NErrorCode::INITIALIZING:
        case NPersQueue::NErrorCode::OVERLOAD:
        case NPersQueue::NErrorCode::READ_TIMEOUT:
        case NPersQueue::NErrorCode::TABLET_IS_DROPPED:
        case NPersQueue::NErrorCode::CREATE_TIMEOUT:
        case NPersQueue::NErrorCode::ERROR:
        case NPersQueue::NErrorCode::CLUSTER_DISABLED:
            return true;
        default:
            return false;
        }
    }
}

namespace NPersQueue {

TRetryingConsumer::TRetryingConsumer(const TConsumerSettings& settings,
                                     std::shared_ptr<void> destroyEventRef,
                                     TIntrusivePtr<TPQLibPrivate> pqLib,
                                     TIntrusivePtr<ILogger> logger)
    : IConsumerImpl(std::move(destroyEventRef), std::move(pqLib))
    , Settings(settings)
    , Logger(std::move(logger))
    , IsDeadPromise(NThreading::NewPromise<TError>())
    , ConsumerDestroyedPromise(NThreading::NewPromise<void>())
    , GenCounter(1)
    , CookieCounter(1)
    , ReconnectionAttemptsDone(0)
    , Stopping(false)
    , Reconnecting(false)
{
    Y_VERIFY(Settings.ReconnectOnFailure, "ReconnectOnFailure should be set.");
    Y_ENSURE(Settings.MaxAttempts != 0, "MaxAttempts setting can't be zero.");
    Y_ENSURE(Settings.ReconnectionDelay != TDuration::Zero(), "ReconnectionDelay setting can't be zero.");
    Y_ENSURE(Settings.MaxReconnectionDelay != TDuration::Zero(), "MaxReconnectionDelay setting can't be zero.");
    Y_ENSURE(Settings.StartSessionTimeout != TDuration::Zero(), "StartSessionTimeout setting can't be zero.");

    Settings.ReconnectOnFailure = false; // reset flag, to use the settings for creating sub consumer
}

TRetryingConsumer::~TRetryingConsumer() noexcept {
    Cancel();
}

NThreading::TFuture<TConsumerCreateResponse> TRetryingConsumer::Start(TInstant deadline) noexcept {
    StartPromise = NThreading::NewPromise<TConsumerCreateResponse>();
    if (deadline != TInstant::Max()) {
        std::weak_ptr<TRetryingConsumer> weakRef = shared_from_this();
        auto onDeadline = [weakRef] {
            auto self = weakRef.lock();
            if (nullptr != self) {
                self->OnStartDeadline();
            }
        };
        PQLib->GetScheduler().Schedule(deadline, this, onDeadline);
    }
    DoReconnect(deadline);
    return StartPromise.GetFuture();
}

NThreading::TFuture<TError> TRetryingConsumer::IsDead() noexcept {
    return IsDeadPromise.GetFuture();
}

void TRetryingConsumer::GetNextMessage(NThreading::TPromise<TConsumerMessage>& promise) noexcept {
    if (!StartPromise.Initialized() || Stopping) {
        TReadResponse res;
        res.MutableError()->SetDescription("consumer is not ready");
        res.MutableError()->SetCode(NErrorCode::ERROR);
        promise.SetValue(TConsumerMessage{std::move(res)});
        return;
    }

    if (!ReadyResponses.empty()) {
        Y_ASSERT(PendingRequests.empty());
        promise.SetValue(std::move(ReadyResponses.front()));
        ReadyResponses.pop_front();
        return;
    }

    PendingRequests.push_back(promise);

    if (Consumer && StartFuture.HasValue()) {
        DoRequest();
    }
}

static void FormatCookies(TStringBuilder& ret, const TVector<ui64>& cookies) {
    ret << "{";
    for (size_t i = 0; i < cookies.size(); ++i) {
        if (i > 0) {
            ret << ", ";
        }
        ret << cookies[i];
    }
    ret << "}";
}

static TString FormatCommitForLog(const TStringBuf reason, const TVector<ui64>& cookies, const TVector<ui64>& originalCookies, const TVector<ui64>& committedCookies) {
    TStringBuilder ret;
    ret << "Commit cookies by retrying consumer" << reason << ". User cookies: ";
    FormatCookies(ret, cookies);
    ret << ". Subconsumer cookies to commit: ";
    FormatCookies(ret, originalCookies);
    ret << ". Skipped cookies: ";
    FormatCookies(ret, committedCookies);
    return std::move(ret);
}

void TRetryingConsumer::Commit(const TVector<ui64>& cookies) noexcept {
    if (!Consumer) {
        if (!StartPromise.Initialized() || Stopping) {
            Destroy("Not ready", NErrorCode::BAD_REQUEST);
        } else {
            // just response that cookies were commited
            FastCommit(cookies);
            DEBUG_LOG(FormatCommitForLog(". Consumer is not initialied", cookies, {}, cookies), "", SessionId);
        }
        return;
    }
    if (Settings.CommitsDisabled) {
        Destroy("Commits are disabled", NErrorCode::BAD_REQUEST);
        return;
    }

    TVector<ui64> originalCookies(Reserve(cookies.size()));
    // cookies which can be treated as committed
    TVector<ui64> commitedCookies;
    ui64 minCookie = CookieCounter - Cookies.size();
    for (auto cookie : cookies) {
        if (cookie >= minCookie && cookie < CookieCounter) {
            auto& cookieInfo = Cookies[cookie - minCookie];
            Y_VERIFY(cookieInfo.UserCookie == cookie);
            if (0 != cookieInfo.OriginalCookie) {
                CommittingCookies[cookieInfo.OriginalCookie] = cookie;
                originalCookies.push_back(cookieInfo.OriginalCookie);
                cookieInfo.OriginalCookie = 0;
                for (auto* lock : cookieInfo.Locks) {
                    lock->Cookies.erase(cookie);
                }
            } else {
                commitedCookies.push_back(cookie);
            }
        } else if (cookie >= CookieCounter) {
            Destroy("Unknown cookie", NErrorCode::BAD_REQUEST);
            break;
        } else {
            commitedCookies.push_back(cookie);
        }
    }
    if (!originalCookies.empty()) {
        Consumer->Commit(originalCookies);
    }
    if (!commitedCookies.empty()) {
        FastCommit(commitedCookies);
    }
    DEBUG_LOG(FormatCommitForLog("", cookies, originalCookies, commitedCookies), "", SessionId);
    // clean commited cookies
    while (!Cookies.empty() && 0 == Cookies.front().OriginalCookie) {
        Cookies.pop_front();
    }
}

void TRetryingConsumer::RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept {
    if (!Consumer) {
        // client should receive message about release lock
        return;
    }
    auto lockIt = Locks.find(std::make_pair(topic, partition));
    if (lockIt == Locks.end() || !lockIt->second.Locked) {
        WARN_LOG("Requesting partition status on partition without lock. Topic: " << topic << ". Partition: " << partition, "", SessionId);
    } else if (lockIt->second.Gen != generation) {
        WARN_LOG("Requesting partition status on partition with wrong generation of lock. Topic: " << topic << ". Partition: " << partition << ". Generation: " << generation, "", SessionId);
    } else {
        Consumer->RequestPartitionStatus(topic, partition, lockIt->second.OriginalGen);
    }
}

void TRetryingConsumer::Cancel() {
    Destroy(GetCancelReason());
}

NThreading::TFuture<void> TRetryingConsumer::Destroyed() noexcept {
    return ConsumerDestroyedPromise.GetFuture();
}

void TRetryingConsumer::OnStartDeadline() {
    if (!StartPromise.HasValue()) {
        TError error;
        error.SetDescription("Start timeout.");
        error.SetCode(NErrorCode::CREATE_TIMEOUT);
        Destroy(error);
    }
}

void TRetryingConsumer::OnConsumerDead(const TError& error) {
    WARN_LOG("Subconsumer is dead: " << error, "", SessionId);
    ScheduleReconnect();
}

void TRetryingConsumer::ScheduleReconnect() {
    if (Stopping || Reconnecting) {
        return;
    }
    Reconnecting = true;
    if (!Locks.empty()) {
        // need to notify client that all locks are expired
        for (auto&& [key, lock] : Locks) {
            if (lock.Locked) {
                TReadResponse response;
                auto* release = response.MutableRelease();
                release->set_generation(lock.Gen);
                release->set_topic(key.first);
                release->set_partition(key.second);
                release->set_can_commit(false);
                FastResponse(TConsumerMessage(std::move(response)));
            }
        }
    }
    if (!CommittingCookies.empty()) {
        // need to notify client that all cookies are commited, because these cookies aren't valid anymore
        TReadResponse response;
        auto* cookies = response.MutableCommit()->MutableCookie();
        cookies->Reserve(CommittingCookies.size());
        for (const auto& cookiePair : CommittingCookies) {
            cookies->Add(cookiePair.second);
        }
        FastResponse(TConsumerMessage(std::move(response)));
    }

    Cookies.clear();
    CommittingCookies.clear();
    Locks.clear();
    Consumer = nullptr;

    if (ReconnectionAttemptsDone >= Settings.MaxAttempts) {
        Destroy(TStringBuilder() << "Failed " << ReconnectionAttemptsDone << " reconnection attempts");
        return;
    }

    ++ReconnectionAttemptsDone;

    TDuration delay = Min(Settings.MaxReconnectionDelay, ReconnectionAttemptsDone * Settings.ReconnectionDelay);
    std::weak_ptr<TRetryingConsumer> weakRef = shared_from_this();
    PQLib->GetScheduler().Schedule(delay, this, [weakRef]() {
        auto self = weakRef.lock();
        if (nullptr != self) {
            self->DoReconnect(self->Settings.StartSessionTimeout.ToDeadLine());
            self->Reconnecting = false;
        }
    });
}

void TRetryingConsumer::DoReconnect(TInstant deadline) {
    if (Stopping) {
        return;
    }
    DEBUG_LOG("Create subconsumer", "", SessionId);
    Consumer = PQLib->CreateRawConsumer(Settings, DestroyEventRef, Logger);
    std::weak_ptr<TRetryingConsumer> weak(shared_from_this());
    StartFuture = Consumer->Start(deadline);
    PQLib->Subscribe(StartFuture, this, [weak](const NThreading::TFuture<TConsumerCreateResponse>& f) {
        auto self = weak.lock();
        if (nullptr != self) {
            self->StartProcessing(f);
        }
    });

    PQLib->Subscribe(Consumer->IsDead(), this, [weak](const NThreading::TFuture<TError>& error) {
        auto self = weak.lock();
        if (nullptr != self) {
            self->OnConsumerDead(error.GetValue());
        }
    });
}

void TRetryingConsumer::StartProcessing(const NThreading::TFuture<TConsumerCreateResponse>& f) {
    Y_VERIFY(f.HasValue());
    if (f.GetValue().Response.HasError()) {
        WARN_LOG("Cannot create subconsumer: " << f.GetValue().Response.GetError(), "", SessionId);
        if (IsRetryable(f.GetValue().Response.GetError())) {
            ScheduleReconnect();
        } else {
            Destroy(f.GetValue().Response.GetError());
        }
    } else {
        if (!SessionId) {
            SessionId = f.GetValue().Response.GetInit().GetSessionId();
        }
        ReconnectionAttemptsDone = 0;
        // need to schedule again all pending requests which are running before connection lost
        for (size_t cnt = PendingRequests.size(); 0 != cnt; --cnt) {
            DoRequest();
        }
        StartPromise.TrySetValue(f.GetValue());
    }
}

void TRetryingConsumer::SubscribeDestroyed() {
    NThreading::TPromise<void> promise = ConsumerDestroyedPromise;
    auto handler = [promise](const auto&) mutable {
        promise.SetValue();
    };
    if (Consumer) {
        WaitExceptionOrAll(DestroyedPromise.GetFuture(), Consumer->Destroyed()).Subscribe(handler);
    } else {
        DestroyedPromise.GetFuture().Subscribe(handler);
    }

    DestroyPQLibRef();
}

void TRetryingConsumer::Destroy(const TError& error) {
    if (Stopping) {
        return;
    }

    Stopping = true;

    SubscribeDestroyed();

    TConsumerMessage message = [&]() {
        TReadResponse response;
        *response.MutableError() = error;
        return TConsumerMessage(std::move(response));
    }();

    for (auto& r : PendingRequests) {
        r.TrySetValue(message);
    }
    IsDeadPromise.TrySetValue(error);
    if (StartPromise.Initialized()) {
        StartPromise.TrySetValue(TConsumerCreateResponse(std::move(message.Response)));
    }
    PendingRequests.clear();
    ReadyResponses.clear();
    Cookies.clear();
    CommittingCookies.clear();
    Locks.clear();
    Consumer = nullptr;
    Reconnecting = false;
}

void TRetryingConsumer::Destroy(const TString& description, NErrorCode::EErrorCode code) {
    TError error;
    error.SetDescription(description);
    error.SetCode(code);
    Destroy(error);
}

void TRetryingConsumer::DoRequest() {
    Y_VERIFY(Consumer);
    std::weak_ptr<TRetryingConsumer> weak(shared_from_this());
    PQLib->Subscribe(Consumer->GetNextMessage(), this, [weak](NThreading::TFuture<TConsumerMessage>& f) mutable {
        auto self = weak.lock();
        if (nullptr != self) {
            self->ProcessResponse(f.ExtractValueSync());
        }
    });
}

void TRetryingConsumer::ProcessResponse(TConsumerMessage&& message) {
    switch (message.Type) {
    case NPersQueue::EMessageType::EMT_DATA:
        {
            auto* data = message.Response.mutable_data();
            TCookieInfo* cookie = nullptr;
            if (!Settings.CommitsDisabled) {
                Cookies.push_back(TCookieInfo{{}, data->cookie(), CookieCounter++});
                cookie = &Cookies.back();
                data->set_cookie(cookie->UserCookie);
                DEBUG_LOG("Got data from subconsumer. Cookie: " << cookie->OriginalCookie << ". User cookie: " << cookie->UserCookie, "", SessionId);
            }

            for (auto& b : data->message_batch()) {
                auto lockIt = Locks.find(std::make_pair(b.topic(), b.partition()));
                if (lockIt != Locks.end()) {
                    Y_VERIFY(lockIt->second.Locked);
                    if (!Settings.CommitsDisabled) {
                        Y_ASSERT(nullptr != cookie);
                        cookie->Locks.emplace(&lockIt->second);
                        lockIt->second.Cookies.emplace(cookie->UserCookie);
                    }

                    // Validate that all offsets are >= LockInfo.ReadOffset as expected.
                    if (lockIt->second.ReadOffset != 0) {
                        for (const auto& message : b.message()) {
                            if (message.offset() < lockIt->second.ReadOffset) {
                                Destroy(
                                    TStringBuilder()
                                        << "Fatal error: expected offsets for topic " << b.topic()
                                        << " partition " << b.partition() << " >= " << lockIt->second.ReadOffset
                                        << ", but got offset " << message.offset()
                                );
                                return;
                            }
                        }
                    }
                }
            }
            // TODO (bulatman) what about batched_data?
        }
        break;
    case NPersQueue::EMessageType::EMT_LOCK:
        {
            auto* lock = message.Response.mutable_lock();
            auto& lockInfo = Locks[std::make_pair(lock->topic(), lock->partition())];
            Y_VERIFY(!lockInfo.Locked);
            lockInfo.Locked = true;
            lockInfo.Gen = GenCounter++;
            lockInfo.OriginalGen = lock->generation();

            lock->set_generation(lockInfo.Gen);

            DEBUG_LOG("Got lock from subconsumer on (" << lock->topic() << ", " << lock->partition() << "). Generation: " << lockInfo.OriginalGen << ". User generation: " << lockInfo.Gen, "", SessionId);

            std::weak_ptr<TRetryingConsumer> weak = shared_from_this();
            PQLib->Subscribe(message.ReadyToRead.GetFuture(), this,
                [weak, topic = lock->topic(), partition = lock->partition(), generation = lock->generation()](const NThreading::TFuture<NPersQueue::TLockInfo>& lockInfo) {
                auto self = weak.lock();
                if (nullptr != self) {
                    self->UpdateReadyToRead(lockInfo.GetValue(), topic, partition, generation);
                }
            });
        }
        break;
    case NPersQueue::EMessageType::EMT_RELEASE:
        {
            auto* release = message.Response.mutable_release();
            const bool softRelease = release->can_commit();
            auto lockIt = Locks.find(std::make_pair(release->topic(), release->partition()));
            const bool lockInfoFound = lockIt != Locks.end();
            DEBUG_LOG("Got release from subconsumer on (" << release->topic() << ", " << release->partition() << "). Can commit: " << softRelease << ". Has lock info: " << lockInfoFound << ". Generation: " << release->generation() << ". User generation: " << (lockInfoFound ? lockIt->second.Gen : 0), "", SessionId);
            if (lockInfoFound) { // It is normal situation when client receives Release(canCommit=true) and then Release(canCommit=false).
                auto& lockInfo = lockIt->second;
                Y_VERIFY(lockInfo.OriginalGen == release->generation(), "lock generation mismatch");
                release->set_generation(lockInfo.Gen);

                if (softRelease) {
                    lockInfo.Locked = false;
                } else {
                    for (auto cookie : lockInfo.Cookies) {
                        auto& cookieInfo = Cookies[cookie + Cookies.size() - CookieCounter];
                        Y_VERIFY(cookieInfo.UserCookie == cookie);
                        // Lock is not valid anymore.
                        cookieInfo.Locks.erase(&lockInfo);
                    }
                    Locks.erase(lockIt);
                }
            } else {
                return;
            }
        }
        break;
    case NPersQueue::EMessageType::EMT_STATUS:
        {
            auto* status = message.Response.mutable_partition_status();
            auto lockIt = Locks.find(std::make_pair(status->topic(), status->partition()));
            Y_VERIFY(lockIt != Locks.end() && lockIt->second.Locked && lockIt->second.OriginalGen == status->generation());
            status->set_generation(lockIt->second.Gen);
        }
        break;
    case NPersQueue::EMessageType::EMT_COMMIT:
        {
            auto* cookies = message.Response.mutable_commit()->mutable_cookie();
            // convert cookies
            for (int i = 0; i < cookies->size(); ++i) {
                auto it = CommittingCookies.find(cookies->Get(i));
                Y_VERIFY(it != CommittingCookies.end(), "unknown commited cookie!");
                cookies->Set(i, it->second);
                CommittingCookies.erase(it);
            }
        }
        break;

    case NPersQueue::EMessageType::EMT_ERROR:
        // check error, if retryable, need to recreate consumer and read message again
        const bool retryable = IsRetryable(message.Response.error());
        DEBUG_LOG("Got error from subconsumer: " << message.Response.error() << ". Retryable: " << retryable, "", SessionId);
        if (retryable) {
            ScheduleReconnect();
            return;
        }
        break;
    };

    if (!PendingRequests.empty()) {
        PendingRequests.front().SetValue(std::move(message));
        PendingRequests.pop_front();
    } else {
        ReadyResponses.push_back(std::move(message));
    }
}

void TRetryingConsumer::FastResponse(TConsumerMessage&& message) {
    if (!PendingRequests.empty()) {
        PendingRequests.front().SetValue(std::move(message));
        PendingRequests.pop_front();
    } else {
        ReadyResponses.push_back(std::move(message));
    }
}

void TRetryingConsumer::FastCommit(const TVector<ui64>& cookies) {
    TReadResponse response;
    auto* cookiesMessage = response.MutableCommit()->MutableCookie();
    cookiesMessage->Reserve(cookies.size());
    for (auto cookie : cookies) {
        cookiesMessage->Add(cookie);
    }
    FastResponse(TConsumerMessage(std::move(response)));
}

void TRetryingConsumer::UpdateReadyToRead(const NPersQueue::TLockInfo& readyToRead, const TString& topic, ui32 partition, ui64 generation) {
    const auto lockIt = Locks.find(std::make_pair(topic, partition));
    if (lockIt != Locks.end() && lockIt->second.Locked && lockIt->second.Gen == generation) {
        lockIt->second.ReadOffset = readyToRead.ReadOffset;
    }
}

}
