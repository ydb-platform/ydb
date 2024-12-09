#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <util/generic/vector.h>

namespace NFq {

struct TopicSessionClientStatistic {
    NActors::TActorId ReadActorId;
    ui32 PartitionId = 0;
    i64 UnreadRows = 0;         // Current value
    i64 UnreadBytes = 0;        // Current value
    ui64 Offset = 0;            // Current value
    ui64 ReadBytes = 0;         // Increment / filtered
    bool IsWaiting = false;     // Current value
    i64 ReadLagMessages = 0;    // Current value
    ui64 InitialOffset = 0;
    void Add(const TopicSessionClientStatistic& stat) {
        UnreadRows = stat.UnreadRows;
        UnreadBytes = stat.UnreadBytes;
        Offset = stat.Offset;
        ReadBytes += stat.ReadBytes;
        IsWaiting = stat.IsWaiting;
        ReadLagMessages = stat.ReadLagMessages;
        InitialOffset = stat.InitialOffset;
    }
    void Clear() {
        ReadBytes = 0;
    }
};

struct TopicSessionCommonStatistic {
    ui64 UnreadBytes = 0;   // Current value
    ui64 RestartSessionByOffsets = 0;
    ui64 ReadBytes = 0;     // Increment
    ui64 ReadEvents = 0;    // Increment
    ui64 LastReadedOffset = 0;
    TDuration ParseAndFilterLatency;
    void Add(const TopicSessionCommonStatistic& stat) {
        UnreadBytes = stat.UnreadBytes;
        RestartSessionByOffsets = stat.RestartSessionByOffsets;
        ReadBytes += stat.ReadBytes;
        ReadEvents += stat.ReadEvents;
        LastReadedOffset = stat.LastReadedOffset;
        ParseAndFilterLatency = stat.ParseAndFilterLatency != TDuration::Zero() ? stat.ParseAndFilterLatency : ParseAndFilterLatency;
    }
    void Clear() {
        ReadBytes = 0;
        ReadEvents = 0;
    }
};

struct TopicSessionParams {
    TString Endpoint;
    TString Database;
    TString TopicPath;
    ui64 PartitionId = 0;
};

struct TopicSessionStatistic {
    TopicSessionParams SessionKey; 
    TVector<TopicSessionClientStatistic> Clients;
    TopicSessionCommonStatistic Common;
};

} // namespace NFq
