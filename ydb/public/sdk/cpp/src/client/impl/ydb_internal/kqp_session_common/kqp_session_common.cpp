#include "kqp_session_common.h"

#include <ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <util/string/cast.h>

namespace NYdb::inline Dev {

using std::string;

ui64 GetNodeIdFromSession(const std::string& sessionId) {
    if (sessionId.empty()) {
        return 0;
    }

    try {
        NKikimr::NOperationId::TOperationId opId(sessionId);
        const auto& nodeIds = opId.GetValue("node_id");
        if (nodeIds.size() != 1) {
            return 0;
        }

        return FromStringWithDefault<ui64>(*nodeIds[0], 0);

    } catch (...) {
        return 0;
    }
    return 0;
}

TKqpSessionCommon::TKqpSessionCommon(
    const std::string& sessionId, const std::string& endpoint,
    bool isOwnedBySessionPool)
    : Lock_()
    , SessionId_(sessionId)
    , EndpointKey_(endpoint, GetNodeIdFromSession(sessionId))
    , IsOwnedBySessionPool_(isOwnedBySessionPool)
    , State_(S_STANDALONE)
    , TimeToTouch_(TInstant::Now())
    , TimeInPast_(TInstant::Now())
    , CloseHandler_(nullptr)
    , NeedUpdateActiveCounter_(false)
{}

TKqpSessionCommon::~TKqpSessionCommon() {
    Unlink();
}

const std::string& TKqpSessionCommon::GetId() const {
    return SessionId_;
}

const string& TKqpSessionCommon::GetEndpoint() const {
    return EndpointKey_.GetEndpoint();
}

const TEndpointKey& TKqpSessionCommon::GetEndpointKey() const {
    return EndpointKey_;
}

// Can be called from interceptor, need lock
void TKqpSessionCommon::MarkBroken() {
    std::lock_guard guard(Lock_);
    if (State_ == EState::S_ACTIVE) {
        NeedUpdateActiveCounter_ = true;
    }
    State_ = EState::S_BROKEN;
}

void TKqpSessionCommon::MarkAsClosing() {
    std::lock_guard guard(Lock_);
    if (State_ == EState::S_ACTIVE) {
        NeedUpdateActiveCounter_ = true;
    }

    State_ = EState::S_CLOSING;
}

void TKqpSessionCommon::MarkActive() {
    State_ = EState::S_ACTIVE;
    NeedUpdateActiveCounter_ = false;
}

void TKqpSessionCommon::MarkIdle() {
    State_ = EState::S_IDLE;
    NeedUpdateActiveCounter_ = false;
}

bool TKqpSessionCommon::IsOwnedBySessionPool() const {
    return IsOwnedBySessionPool_;
}

TKqpSessionCommon::EState TKqpSessionCommon::GetState() const {
    // See comments in InjectSessionStatusInterception about lock
    std::lock_guard guard(const_cast<TAdaptiveLock&>(Lock_));
    return State_;
}

void TKqpSessionCommon::SetNeedUpdateActiveCounter(bool flag) {
    NeedUpdateActiveCounter_ = flag;
}

bool TKqpSessionCommon::NeedUpdateActiveCounter() const {
    return NeedUpdateActiveCounter_;
}

// We need lock here because this method can be called from different thread
// if client makes simultaneous calls on one session.
// It should be possible to rewrite this part.
void TKqpSessionCommon::ScheduleTimeToTouch(TDuration interval,
    bool updateTimeInPast)
{
    auto now = TInstant::Now();
    std::lock_guard guard(Lock_);
    if (updateTimeInPast) {
            TimeInPast_ = now;
    }
    TimeToTouch_.store(now + interval, std::memory_order_relaxed);
}

void TKqpSessionCommon::ScheduleTimeToTouchFast(TDuration interval,
    bool updateTimeInPast)
{
    auto now = TInstant::Now();
    if (updateTimeInPast) {
        TimeInPast_ = now;
    }
    TimeToTouch_.store(now + interval, std::memory_order_relaxed);
}

TInstant TKqpSessionCommon::GetTimeToTouchFast() const {
    return TimeToTouch_.load(std::memory_order_relaxed);
}

TInstant TKqpSessionCommon::GetTimeInPastFast() const {
    return TimeInPast_;
}

// SetTimeInterval/GetTimeInterval, are not atomic!
void TKqpSessionCommon::SetTimeInterval(TDuration interval) {
    TimeInterval_ = interval;
}

TDuration TKqpSessionCommon::GetTimeInterval() const {
    return TimeInterval_;
}

void TKqpSessionCommon::UpdateServerCloseHandler(IServerCloseHandler* handler) {
    CloseHandler_.store(handler);
}

void TKqpSessionCommon::CloseFromServer(std::weak_ptr<ISessionClient> client) noexcept {
    auto strong = client.lock();
    if (!strong) {
        // Session closed on the server after stopping client - do nothing
        // moreover pool maybe destoyed now
        return;
    }

    IServerCloseHandler* h = CloseHandler_.load();
    if (h) {
        h->OnCloseSession(this, strong);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::function<void(TKqpSessionCommon*)> TKqpSessionCommon::GetSmartDeleter(
    std::shared_ptr<ISessionClient> client)
{
    return [client](TKqpSessionCommon* sessionImpl) {
        switch (sessionImpl->GetState()) {
            case TKqpSessionCommon::S_STANDALONE:
            case TKqpSessionCommon::S_BROKEN:
            case TKqpSessionCommon::S_CLOSING:
                client->DeleteSession(sessionImpl);
            break;
            case TKqpSessionCommon::S_IDLE:
            case TKqpSessionCommon::S_ACTIVE: {
                if (!client->ReturnSession(sessionImpl)) {
                    client->DeleteSession(sessionImpl);
                }
                break;
            }
            default:
            break;
        }
    };
}

} // namespace NYdb
