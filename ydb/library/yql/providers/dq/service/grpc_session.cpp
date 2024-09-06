#include "grpc_session.h"

#include <ydb/library/yql/utils/log/log.h>

namespace NYql::NDqs {

TSession::~TSession() {
    TGuard<TMutex> lock(Mutex);
    for (auto [actorId, _] : ActorId2QueryNo) {
        ActorSystem->Send(actorId, new NActors::TEvents::TEvPoison());
    }
}

void TSession::DeleteRequest(const NActors::TActorId& actorId)
{
    TGuard<TMutex> lock(Mutex);
    auto it = ActorId2QueryNo.find(actorId);
    if (it != ActorId2QueryNo.end()) {
        if (it->second) {
            QueryNo2ActorId.erase(it->second);
        }
        ActorId2QueryNo.erase(it);
    }
}

void TSession::AddRequest(const NActors::TActorId& actorId, uint64_t querySeqNo)
{
    TGuard<TMutex> lock(Mutex);
    ActorId2QueryNo[actorId] = querySeqNo;
    if (querySeqNo) {
        QueryNo2ActorId[querySeqNo] = actorId;
    }
}

NActors::TActorId TSession::FindActorId(uint64_t queryNo) {
    TGuard<TMutex> lock(Mutex);
    auto it = QueryNo2ActorId.find(queryNo);
    if (it != QueryNo2ActorId.end()) {
        return it->second;
    }
    return {};
}

TSessionStorage::TSessionStorage(
    NActors::TActorSystem* actorSystem,
    const NMonitoring::TDynamicCounters::TCounterPtr& sessionsCounter)
    : ActorSystem(actorSystem)
    , SessionsCounter(sessionsCounter)
{ }

void TSessionStorage::CloseSession(const TString& sessionId)
{
    TGuard<TMutex> lock(SessionMutex);
    auto it = Sessions.find(sessionId);
    if (it == Sessions.end()) {
        return;
    }
    SessionsByLastUpdate.erase(it->second.Iterator);
    Sessions.erase(it);
    *SessionsCounter = Sessions.size();
}

std::shared_ptr<TSession> TSessionStorage::GetSession(const TString& sessionId)
{
    Clean(TInstant::Now() - TDuration::Minutes(10));

    TGuard<TMutex> lock(SessionMutex);
    auto it = Sessions.find(sessionId);
    if (it == Sessions.end()) {
        return std::shared_ptr<TSession>();
    } else {
        SessionsByLastUpdate.erase(it->second.Iterator);
        SessionsByLastUpdate.push_back({TInstant::Now(), sessionId});
        it->second.Iterator = SessionsByLastUpdate.end();
        it->second.Iterator--;
        return it->second.Session;
    }
}

bool TSessionStorage::OpenSession(const TString& sessionId, const TString& username)
{
    TGuard<TMutex> lock(SessionMutex);
    if (Sessions.contains(sessionId)) {
        return false;
    }

    SessionsByLastUpdate.push_back({TInstant::Now(), sessionId});
    auto it = SessionsByLastUpdate.end(); --it;

    Sessions[sessionId] = TSessionAndIterator {
        std::make_shared<TSession>(username, ActorSystem),
        it
    };

    *SessionsCounter = Sessions.size();

    return true;
}

void TSessionStorage::Clean(TInstant before) {
    TGuard<TMutex> lock(SessionMutex);
    for (TSessionsByLastUpdate::iterator it = SessionsByLastUpdate.begin();
         it != SessionsByLastUpdate.end(); )
    {
        if (it->LastUpdate < before) {
            YQL_CLOG(INFO, ProviderDq) << "Drop session by timeout " << it->SessionId;
            Sessions.erase(it->SessionId);
            it = SessionsByLastUpdate.erase(it);
        } else {
            break;
        }
    }

    *SessionsCounter = Sessions.size();
}

void TSessionStorage::PrintInfo() const {
    YQL_CLOG(INFO, ProviderDq) << "SessionsByLastUpdate: " << SessionsByLastUpdate.size();
    YQL_CLOG(DEBUG, ProviderDq) << "Sessions: " << Sessions.size();
    ui64 currenSessionsCounter = *SessionsCounter;
    YQL_CLOG(DEBUG, ProviderDq) << "SessionsCounter: " << currenSessionsCounter;
}

} // namespace NYql::NDqs
