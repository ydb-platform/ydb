#include "kqp_session_common.h"

#include <ydb/public/lib/operation_id/operation_id.h>

namespace NYdb {

using std::string;

ui64 GetNodeIdFromSession(const TStringType& sessionId) {
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
    const TStringType& sessionId, const TStringType& endpoint,
    bool isOwnedBySessionPool)
    : Lock_()
    , SessionId_(sessionId)
    , EndpointKey_(endpoint, GetNodeIdFromSession(sessionId))
    , IsOwnedBySessionPool_(isOwnedBySessionPool)
    , State_(S_STANDALONE)
    , TimeToTouch_(TInstant::Now())
    , TimeInPast_(TInstant::Now())
    , NeedUpdateActiveCounter_(false)
{}

TKqpSessionCommon::~TKqpSessionCommon() {
    Unlink();
}

const TStringType& TKqpSessionCommon::GetId() const {
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
    with_lock(Lock_) {
        if (State_ == EState::S_ACTIVE) {
            NeedUpdateActiveCounter_ = true;
        }
        State_ = EState::S_BROKEN;
    }
}

void TKqpSessionCommon::MarkAsClosing() {
    with_lock(Lock_) {
        if (State_ == EState::S_ACTIVE) {
            NeedUpdateActiveCounter_ = true;
        }

        State_ = EState::S_CLOSING;
    }
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
    with_lock(Lock_) {
        return State_;
    }
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
    with_lock(Lock_) {
        if (updateTimeInPast) {
             TimeInPast_ = now;
        }
        TimeToTouch_ = now + interval;
    }
}

void TKqpSessionCommon::ScheduleTimeToTouchFast(TDuration interval,
    bool updateTimeInPast)
{
    auto now = TInstant::Now();
    if (updateTimeInPast) {
        TimeInPast_ = now;
    }
    TimeToTouch_ = now + interval;
}

TInstant TKqpSessionCommon::GetTimeToTouchFast() const {
    return TimeToTouch_;
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
