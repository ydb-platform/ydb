#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <util/generic/vector.h>

namespace NFq {

struct TopicSessionClientStatistic {
    NActors::TActorId ReadActorId;
    ui32 PartitionId = 0;
    i64 UnreadRows = 0;     // Current value
    i64 UnreadBytes = 0;    // Current value
    ui64 Offset = 0;        // Current value
    ui64 ReadBytes = 0;     // Increment / filtered
    void Add(const TopicSessionClientStatistic& stat) {
        UnreadRows = stat.UnreadRows;
        UnreadBytes = stat.UnreadBytes;
        Offset = stat.Offset;
        ReadBytes += stat.ReadBytes;
    }
};

struct TopicSessionCommonStatistic {
    ui64 UnreadBytes = 0;   // Current value
    ui64 ReadBytes = 0;     // Increment
    ui64 ReadEvents = 0;    // Increment
    void Add(const TopicSessionCommonStatistic& stat) {
        UnreadBytes = stat.UnreadBytes;
        ReadBytes += stat.ReadBytes;
        ReadEvents += stat.ReadEvents;
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
