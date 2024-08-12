#pragma once
#include <ydb/library/actors/core/actorsystem.h>

namespace NYql::NDqs {

class TSession {
public:
    TSession(const TString& username, NActors::TActorSystem* actorSystem)
        : Username(username)
        , ActorSystem(actorSystem)
    { }

    ~TSession();

    const TString& GetUsername() const {
        return Username;
    }

    void DeleteRequest(const NActors::TActorId& id);
    void AddRequest(const NActors::TActorId& id, uint64_t queryNo);
    NActors::TActorId FindActorId(uint64_t queryNo);

private:
    TMutex Mutex;
    const TString Username;
    NActors::TActorSystem* ActorSystem;
    THashMap<NActors::TActorId, uint64_t> ActorId2QueryNo;
    THashMap<uint64_t, NActors::TActorId> QueryNo2ActorId;
};

class TSessionStorage {
public:
    TSessionStorage(
        NActors::TActorSystem* actorSystem,
        const NMonitoring::TDynamicCounters::TCounterPtr& sessionsCounter);
    void CloseSession(const TString& sessionId);
    std::shared_ptr<TSession> GetSession(const TString& sessionId);
    bool OpenSession(const TString& sessionId, const TString& username);
    void Clean(TInstant before);
    void PrintInfo() const;

private:
    NActors::TActorSystem* ActorSystem;
    NMonitoring::TDynamicCounters::TCounterPtr SessionsCounter;
    TMutex SessionMutex;
    struct TTimeAndSessionId {
        TInstant LastUpdate;
        TString SessionId;
    };
    using TSessionsByLastUpdate = TList<TTimeAndSessionId>;
    TSessionsByLastUpdate SessionsByLastUpdate;
    struct TSessionAndIterator {
        std::shared_ptr<TSession> Session;
        TSessionsByLastUpdate::iterator Iterator;
    };
    THashMap<TString, TSessionAndIterator> Sessions;
};

} // namespace NYql::NDqs
