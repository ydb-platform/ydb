#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>

namespace NKikimr::NKqp {

using TNodeId = ui32;

struct TKqpProxyRequest {
    TActorId Sender;
    ui64 SenderCookie = 0;
    TString TraceId;
    ui32 EventType;
    TString SessionId;
    TKqpDbCountersPtr DbCounters;

    TKqpProxyRequest(const TActorId& sender, ui64 senderCookie, const TString& traceId,
        ui32 eventType)
        : Sender(sender)
        , SenderCookie(senderCookie)
        , TraceId(traceId)
        , EventType(eventType)
        , SessionId()
    {}

    void SetSessionId(const TString& sessionId, TKqpDbCountersPtr dbCounters) {
        SessionId = sessionId;
        DbCounters = dbCounters;
    }
};


class TKqpProxyRequestTracker {
    ui64 RequestId;
    THashMap<ui64, TKqpProxyRequest> PendingRequests;

public:
    TKqpProxyRequestTracker()
        : RequestId(1)
    {}

    ui64 RegisterRequest(const TActorId& sender, ui64 senderCookie, const TString& traceId, ui32 eventType) {
        ui64 NewRequestId = ++RequestId;
        PendingRequests.emplace(NewRequestId, TKqpProxyRequest(sender, senderCookie, traceId, eventType));
        return NewRequestId;
    }

    const TKqpProxyRequest* FindPtr(ui64 requestId) const {
        return PendingRequests.FindPtr(requestId);
    }

    void SetSessionId(ui64 requestId, const TString& sessionId, TKqpDbCountersPtr dbCounters) {
        TKqpProxyRequest* ptr = PendingRequests.FindPtr(requestId);
        ptr->SetSessionId(sessionId, dbCounters);
    }

    void Erase(ui64 requestId) {
        PendingRequests.erase(requestId);
    }
};

template<typename TValue>
struct TProcessResult {
    Ydb::StatusIds::StatusCode YdbStatus;
    TString Error;
    TValue Value;
    bool ResourceExhausted = false;
};

struct TKqpSessionInfo {
    enum ESessionState : ui32 {
        IDLE = 1,
        EXECUTING = 2
    };

    TString SessionId;
    TActorId WorkerId;
    TString Database;
    TKqpDbCountersPtr DbCounters;
    TInstant ShutdownStartedAt;
    std::vector<i32> ReadyPos;
    NActors::TMonotonic IdleTimeout;
    // position in the idle list.
    std::list<TKqpSessionInfo*>::iterator IdlePos;
    TNodeId AttachedNodeId;
    TActorId AttachedRpcId;
    bool PgWire;
    TString QueryText;
    bool Ready = true;
    TString ClientApplicationName;
    TString ClientSID;
    TString ClientHost;
    TString UserAgent;
    TString SdkBuildInfo;
    TString ClientPID;
    ui32 QueryCount = 0;
    TInstant SessionStartedAt;
    TInstant StateChangeAt;
    TInstant QueryStartAt;

    ESessionState State = ESessionState::IDLE;

    struct TFieldsMap {
        ui64 bitmap = 0;

        bool NeedField(ui32 tag) const {
            return bitmap & (1ull << tag);
        }

        explicit TFieldsMap(const ::google::protobuf::RepeatedField<ui32>& columns) {
            for(const auto& column: columns) {
                Y_ABORT_UNLESS(column <= 63);

                bitmap |= (1ull << column);
            }
        }
    };

    TKqpSessionInfo(const TString& sessionId, const TActorId& workerId,
        const TString& database, TKqpDbCountersPtr dbCounters, std::vector<i32>&& pos,
        NActors::TMonotonic idleTimeout, std::list<TKqpSessionInfo*>::iterator idlePos, bool pgWire,
        TInstant sessionStartedAt)
        : SessionId(sessionId)
        , WorkerId(workerId)
        , Database(database)
        , DbCounters(dbCounters)
        , ShutdownStartedAt()
        , ReadyPos(std::move(pos))
        , IdleTimeout(std::move(idleTimeout))
        , IdlePos(idlePos)
        , AttachedNodeId(0)
        , PgWire(pgWire)
        , SessionStartedAt(std::move(sessionStartedAt))
    {
    }

    void SerializeTo(::NKikimrKqp::TSessionInfo* proto, const TFieldsMap& fieldsMap) const;
};

class TLocalSessionsRegistry {
    THashMap<TString, TKqpSessionInfo> LocalSessions;
    std::map<TString, TKqpSessionInfo*> OrderedSessions;
    THashMap<TActorId, TString> TargetIdIndex;
    THashSet<TString> ShutdownInFlightSessions;
    THashMap<TString, ui32> SessionsCountPerDatabase;
    std::vector<std::vector<TString>> ReadySessions;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    std::list<TKqpSessionInfo*> IdleSessions;
    // map rpc node to local sessions
    THashMap<TNodeId, THashSet<const TKqpSessionInfo*>> AttachedNodesIndex;

public:
    TLocalSessionsRegistry(TIntrusivePtr<IRandomProvider> randomProvider)
        : ReadySessions(2)
        , RandomProvider(randomProvider)
    {}

    bool AttachSession(const TKqpSessionInfo* sessionInfo, TNodeId nodeId, TActorId rpcActor) {
        const_cast<TKqpSessionInfo*>(sessionInfo)->AttachedNodeId = nodeId;
        const_cast<TKqpSessionInfo*>(sessionInfo)->AttachedRpcId = rpcActor;
        auto& actors = AttachedNodesIndex[nodeId];
        return actors.insert(sessionInfo).second;
    }

    void AttachQueryText(const TKqpSessionInfo* sessionInfo, const TString& queryText) {
        const_cast<TKqpSessionInfo*>(sessionInfo)->QueryText = queryText;
        const_cast<TKqpSessionInfo*>(sessionInfo)->QueryCount++;
        const_cast<TKqpSessionInfo*>(sessionInfo)->State = TKqpSessionInfo::EXECUTING;
        auto curNow = TInstant::Now();
        const_cast<TKqpSessionInfo*>(sessionInfo)->QueryStartAt = curNow;
        const_cast<TKqpSessionInfo*>(sessionInfo)->StateChangeAt = curNow;
    }

    void DetachQueryText(const TKqpSessionInfo* sessionInfo) {
        const_cast<TKqpSessionInfo*>(sessionInfo)->QueryText = TString();
        const_cast<TKqpSessionInfo*>(sessionInfo)->State = TKqpSessionInfo::IDLE;
        auto curNow = TInstant::Now();
        const_cast<TKqpSessionInfo*>(sessionInfo)->QueryStartAt = TInstant::Zero();
        const_cast<TKqpSessionInfo*>(sessionInfo)->StateChangeAt = curNow;
    }

    TKqpSessionInfo* Create(const TString& sessionId, const TActorId& workerId,
        const TString& database, TKqpDbCountersPtr dbCounters, bool supportsBalancing,
        TDuration idleDuration, bool pgWire = false)
    {
        std::vector<i32> pos(2, -1);
        pos[0] = ReadySessions[0].size();
        ReadySessions[0].push_back(sessionId);

        if (supportsBalancing) {
            pos[1] = ReadySessions[1].size();
            ReadySessions[1].push_back(sessionId);
        }

        NActors::TMonotonic sessionStartedAt = NActors::TActivationContext::Monotonic();
        auto startedAt = TInstant::Now();
        auto result = LocalSessions.emplace(sessionId,
            TKqpSessionInfo(sessionId, workerId, database, dbCounters, std::move(pos),
                sessionStartedAt + idleDuration, IdleSessions.end(), pgWire, startedAt));
        OrderedSessions.emplace(sessionId, &result.first->second);
        SessionsCountPerDatabase[database]++;
        Y_ABORT_UNLESS(result.second, "Duplicate session id!");
        TargetIdIndex.emplace(workerId, sessionId);
        StartIdleCheck(&(result.first->second), idleDuration);
        return &result.first->second;
    }

    const THashSet<TString>& GetShutdownInFlight() const {
        return ShutdownInFlightSessions;
    }

    TKqpSessionInfo* StartShutdownSession(const TString& sessionId) {
        ShutdownInFlightSessions.emplace(sessionId);
        auto ptr = LocalSessions.FindPtr(sessionId);
        ptr->ShutdownStartedAt = TAppData::TimeProvider->Now();
        RemoveSessionFromLists(ptr);
        return ptr;
    }

    bool IsSessionIdle(const TKqpSessionInfo* sessionInfo) const {
        return sessionInfo->IdlePos != IdleSessions.end();
    }

    const TKqpSessionInfo* GetIdleSession(const NActors::TMonotonic& now) {
        if (IdleSessions.empty()) {
            return nullptr;
        }

        const TKqpSessionInfo* candidate = (*IdleSessions.begin());
        if (candidate->IdleTimeout > now) {
            return nullptr;
        }

        return candidate;
    }

    void StartIdleCheck(const TKqpSessionInfo* sessionInfo, const TDuration idleDuration) {
        if (!sessionInfo) {
            return;
        }

        if (sessionInfo->PgWire) {
            return;
        }

        TKqpSessionInfo* info = const_cast<TKqpSessionInfo*>(sessionInfo);

        info->IdleTimeout = NActors::TActivationContext::Monotonic() + idleDuration;
        if (info->IdlePos != IdleSessions.end()) {
            IdleSessions.erase(info->IdlePos);
        }

        info->IdlePos = IdleSessions.insert(IdleSessions.end(), info);
    }

    void StopIdleCheck(const TKqpSessionInfo* sessionInfo) {
        if (!sessionInfo) {
            return;
        }

        if (sessionInfo->PgWire) {
            return;
        }

        TKqpSessionInfo* info = const_cast<TKqpSessionInfo*>(sessionInfo);
        if (info->IdlePos != IdleSessions.end()) {
            IdleSessions.erase(info->IdlePos);
            info->IdlePos = IdleSessions.end();
        }
    }

    TKqpSessionInfo* PickSessionToShutdown(bool force, ui32 minReasonableToKick) {
        auto& sessions = force ? ReadySessions.at(0) : ReadySessions.at(1);
        if (!sessions.empty() && sessions.size() >= minReasonableToKick) {
            ui64 idx = RandomProvider->GenRand() % sessions.size();
            return StartShutdownSession(sessions[idx]);
        }

        return nullptr;
    }

    THashMap<TString, TKqpSessionInfo>::const_iterator begin() const {
        return LocalSessions.begin();
    }

    THashMap<TString, TKqpSessionInfo>::const_iterator end() const {
        return LocalSessions.end();
    }

    size_t GetShutdownInFlightSize() const {
        return ShutdownInFlightSessions.size();
    }

    std::map<TString, TKqpSessionInfo*>::const_iterator GetOrderedLowerBound(const TString& continuation) const {
        return OrderedSessions.lower_bound(continuation);
    }

    std::map<TString, TKqpSessionInfo*>::const_iterator GetOrderedEnd() const {
        return OrderedSessions.end();
    }

    std::pair<TNodeId, TActorId> Erase(const TString& sessionId) {
        auto it = LocalSessions.find(sessionId);
        auto result = std::make_pair<TNodeId, TActorId>(0, TActorId());
        if (it != LocalSessions.end()) {
            result.second = it->second.AttachedRpcId;
            auto counter = SessionsCountPerDatabase.find(it->second.Database);
            if (counter != SessionsCountPerDatabase.end()) {
                counter->second--;
                if (counter->second == 0) {
                    SessionsCountPerDatabase.erase(counter);
                }
            }

            StopIdleCheck(&(it->second));
            RemoveSessionFromLists(&(it->second));
            ShutdownInFlightSessions.erase(sessionId);
            TargetIdIndex.erase(it->second.WorkerId);

            if (const auto nodeId = it->second.AttachedNodeId) {
                auto attIt = AttachedNodesIndex.find(nodeId);
                if (attIt != AttachedNodesIndex.end()) {
                    attIt->second.erase(&(it->second));
                    if (attIt->second.empty()) {
                        result.first = nodeId;
                        AttachedNodesIndex.erase(attIt);
                    }
                }
            }

            OrderedSessions.erase(sessionId);
            LocalSessions.erase(it);
        }

        return result;
    }

    bool IsPendingShutdown(const TString& sessionId) const {
        return ShutdownInFlightSessions.find(sessionId) != ShutdownInFlightSessions.end();
   }

    bool CheckDatabaseLimits(const TString& database, ui32 databaseLimit) {
        auto it = SessionsCountPerDatabase.find(database);
        if (it == SessionsCountPerDatabase.end()){
            return true;
        }

        if (it->second + 1 <= databaseLimit) {
            return true;
        }

        return false;
    }

    size_t size() const {
        return LocalSessions.size();
    }

    const TKqpSessionInfo* FindPtr(const TString& sessionId) const {
        return LocalSessions.FindPtr(sessionId);
    }

    const THashSet<const TKqpSessionInfo*>& FindSessions(const TNodeId& nodeId) const {
        auto it = AttachedNodesIndex.find(nodeId);
        if (it == AttachedNodesIndex.end()) {
            static THashSet<const TKqpSessionInfo*> empty;
            return empty;
        }
        return it->second;
    }

    std::pair<TNodeId, TActorId> Erase(const TActorId& targetId) {
        auto result = std::make_pair<TNodeId, TActorId>(0, TActorId());

        auto it = TargetIdIndex.find(targetId);
        if (it != TargetIdIndex.end()){
            result = Erase(it->second);
        }

        return result;
    }

    template<typename TCb>
    void ForEachNode(TCb&& cb) {
        for (const auto& n : AttachedNodesIndex) {
            cb(n.first);
        }
    }

private:
    void RemoveSessionFromLists(TKqpSessionInfo* ptr) {
        for(ui32 i = 0; i < ptr->ReadyPos.size(); ++i) {
            i32& pos = ptr->ReadyPos.at(i);
            auto& sessions = ReadySessions.at(i);
            if (pos != -1 && pos + 1 != static_cast<i32>(sessions.size())) {
                auto& lastPos = LocalSessions.at(sessions.back()).ReadyPos.at(i);
                Y_ABORT_UNLESS(lastPos + 1 == static_cast<i32>(sessions.size()));
                std::swap(sessions[pos], sessions[lastPos]);
                lastPos = pos;
            }

            if (pos != -1) {
                sessions.pop_back();
                pos = -1;
            }
        }
    }
};

class TResourcePoolsCache {
    struct TPoolInfo {
        NResourcePool::TPoolSettings Config;
        std::optional<NACLib::TSecurityObject> SecurityObject;
    };

public:
    std::optional<TPoolInfo> GetPoolInfo(const TString& database, const TString& poolId) const {
        auto it = PoolsCache.find(GetPoolKey(database, poolId));
        if (it == PoolsCache.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void UpdatePoolInfo(const TString& database, const TString& poolId, const std::optional<NResourcePool::TPoolSettings>& config, const std::optional<NACLib::TSecurityObject>& securityObject) {
        const TString& poolKey = GetPoolKey(database, poolId);
        if (!config) {
            PoolsCache.erase(poolKey);
            return;
        }

        auto& poolInfo = PoolsCache[poolKey];
        poolInfo.Config = *config;
        poolInfo.SecurityObject = securityObject;
    }

private:
    static TString GetPoolKey(const TString& database, const TString& poolId) {
        return CanonizePath(TStringBuilder() << database << "/" << poolId);
    }

private:
    std::unordered_map<TString, TPoolInfo> PoolsCache;
};

}  // namespace NKikimr::NKqp
