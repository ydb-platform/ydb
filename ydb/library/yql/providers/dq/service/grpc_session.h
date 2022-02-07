#pragma once
#include <library/cpp/actors/core/actorsystem.h>

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
    void AddRequest(const NActors::TActorId& id);

private:
    TMutex Mutex;
    const TString Username;
    NActors::TActorSystem* ActorSystem;
    THashSet<NActors::TActorId> Requests;
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
