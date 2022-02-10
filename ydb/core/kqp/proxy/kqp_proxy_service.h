#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/protos/kqp.pb.h>

#include <library/cpp/actors/core/actorid.h>

#include <util/datetime/base.h>

namespace NKikimr::NKqp {

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
        : RequestId(0)
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
    TString SessionId;
    TActorId WorkerId;
    TString Database;
    TKqpDbCountersPtr DbCounters;
    ui32 UseFrequency;
    TInstant LastRequestAt;
    TInstant CreatedAt;
    TInstant ShutdownStartedAt;

    TKqpSessionInfo(const TString& sessionId, const TActorId& workerId,
        const TString& database, TKqpDbCountersPtr dbCounters)
        : SessionId(sessionId)
        , WorkerId(workerId)
        , Database(database)
        , DbCounters(dbCounters)
        , UseFrequency(0)
        , ShutdownStartedAt()
    {
        auto now = TAppData::TimeProvider->Now();
        LastRequestAt = now;
        CreatedAt = now;
    }
};

struct TSimpleResourceStats {
    double Mean;
    double Deviation;
    ui64 CV;

    TSimpleResourceStats(double mean, double deviation, ui64 cv)
        : Mean(mean)
        , Deviation(deviation)
        , CV(cv)
    {}
};

struct TPeerStats {
    TSimpleResourceStats SessionCount;
    TSimpleResourceStats Cpu;

    TPeerStats(TSimpleResourceStats sessionsCount, TSimpleResourceStats cpu)
        : SessionCount(sessionsCount)
        , Cpu(cpu)
    {}
};


TSimpleResourceStats CalcPeerStats(
    const TVector<NKikimrKqp::TKqpProxyNodeResources>& data, const TString& selfDataCenterId, bool localDatacenterPolicy,
    std::function<double(const NKikimrKqp::TKqpProxyNodeResources& entry)> ExtractValue);
TPeerStats CalcPeerStats(const TVector<NKikimrKqp::TKqpProxyNodeResources>& data, const TString& selfDataCenterId, bool localDatacenterPolicy); 

}  // namespace NKikimr::NKqp
