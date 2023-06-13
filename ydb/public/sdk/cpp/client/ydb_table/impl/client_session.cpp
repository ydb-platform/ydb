#include "client_session.h"
#include "data_query.h"

#include <util/string/cast.h>

namespace NYdb {
namespace NTable {

ui64 GetNodeIdFromSession(const TString& sessionId) {
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

TSession::TImpl::TImpl(const TString& sessionId, const TString& endpoint, bool useQueryCache, ui32 queryCacheSize, bool isOwnedBySessionPool)
    : SessionId_(sessionId)
    , EndpointKey_(endpoint, GetNodeIdFromSession(sessionId))
    , State_(S_STANDALONE)
    , UseQueryCache_(useQueryCache)
    , QueryCache_(queryCacheSize)
    , Lock_()
    , TimeToTouch_(TInstant::Now())
    , TimeInPast_(TInstant::Now())
    , NeedUpdateActiveCounter_(false)
    , IsOwnedBySessionPool_(isOwnedBySessionPool)
{}

TSession::TImpl::~TImpl() {
    Unlink();
}

const TString& TSession::TImpl::GetId() const {
    return SessionId_;
}

const TString& TSession::TImpl::GetEndpoint() const {
    return EndpointKey_.GetEndpoint();
}

const TEndpointKey& TSession::TImpl::GetEndpointKey() const {
    return EndpointKey_;
}

// Can be called from interceptor, need lock
void TSession::TImpl::MarkBroken() {
    with_lock(Lock_) {
        if (State_ == EState::S_ACTIVE) {
            NeedUpdateActiveCounter_ = true;
        }
        State_ = EState::S_BROKEN;
    }
}

void TSession::TImpl::MarkAsClosing() {
    with_lock(Lock_) {
        if (State_ == EState::S_ACTIVE) {
            NeedUpdateActiveCounter_ = true;
        }

        State_ = EState::S_CLOSING;
    }
}

void TSession::TImpl::MarkStandalone() {
    State_ = EState::S_STANDALONE;
    NeedUpdateActiveCounter_ = false;
}

void TSession::TImpl::MarkActive() {
    State_ = EState::S_ACTIVE;
    NeedUpdateActiveCounter_ = false;
}

void TSession::TImpl::MarkIdle() {
    State_ = EState::S_IDLE;
    NeedUpdateActiveCounter_ = false;
}

bool TSession::TImpl::IsOwnedBySessionPool() const {
    return IsOwnedBySessionPool_;
}

TSession::TImpl::EState TSession::TImpl::GetState() const {
    // See comments in InjectSessionStatusInterception about lock
    with_lock(Lock_) {
        return State_;
    }
}

void TSession::TImpl::SetNeedUpdateActiveCounter(bool flag) {
    NeedUpdateActiveCounter_ = flag;
}

bool TSession::TImpl::NeedUpdateActiveCounter() const {
    return NeedUpdateActiveCounter_;
}

void TSession::TImpl::InvalidateQueryInCache(const TString& key) {
    if (!UseQueryCache_) {
        return;
    }

    with_lock(Lock_) {
        auto it = QueryCache_.Find(key);
        if (it != QueryCache_.End()) {
            QueryCache_.Erase(it);
        }
    }
}

void TSession::TImpl::InvalidateQueryCache() {
    if (!UseQueryCache_) {
        return;
    }

    with_lock(Lock_) {
        QueryCache_.Clear();
    }
}

TMaybe<TSession::TImpl::TDataQueryInfo> TSession::TImpl::GetQueryFromCache(const TString& query, bool allowMigration) {
    if (!UseQueryCache_) {
        return {};
    }

    auto key = EncodeQuery(query, allowMigration);

    with_lock(Lock_) {
        auto it = QueryCache_.Find(key);
        if (it != QueryCache_.End()) {
            return *it;
        }
    }

    return Nothing();
}

void TSession::TImpl::AddQueryToCache(const TDataQuery& query) {
    if (!UseQueryCache_) {
        return;
    }

    const auto& id = query.Impl_->GetId();
    if (id.empty()) {
        return;
    }

    auto key = query.Impl_->GetTextHash();
    TDataQueryInfo queryInfo(id, query.Impl_->GetParameterTypes());

    with_lock(Lock_) {
        auto it = QueryCache_.Find(key);
        if (it != QueryCache_.End()) {
            *it = queryInfo;
        } else {
            QueryCache_.Insert(key, queryInfo);
        }
    }
}

// We need lock here because this method can be called from different thread if client
// makes simultaneous calls on one session. It should be possible to rewrite this part.
void TSession::TImpl::ScheduleTimeToTouch(TDuration interval, bool updateTimeInPast) {
    auto now = TInstant::Now();
    with_lock(Lock_) {
        if (updateTimeInPast) {
             TimeInPast_ = now;
        }
        TimeToTouch_ = now + interval;
    }
}

void TSession::TImpl::ScheduleTimeToTouchFast(TDuration interval, bool updateTimeInPast) {
    auto now = TInstant::Now();
    if (updateTimeInPast) {
        TimeInPast_ = now;
    }
    TimeToTouch_ = now + interval;
}

TInstant TSession::TImpl::GetTimeToTouchFast() const {
    return TimeToTouch_;
}

TInstant TSession::TImpl::GetTimeInPastFast() const {
    return TimeInPast_;
}

// SetTimeInterval/GetTimeInterval, are not atomic!
void TSession::TImpl::SetTimeInterval(TDuration interval) {
    TimeInterval_ = interval;
}

TDuration TSession::TImpl::GetTimeInterval() const {
    return TimeInterval_;
}

const TLRUCache<TString, TSession::TImpl::TDataQueryInfo>& TSession::TImpl::GetQueryCacheUnsafe() const {
    return QueryCache_;
}

} // namespace NTable
} // namespace NYdb
