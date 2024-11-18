#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <util/generic/vector.h>

namespace NFq {

struct TopicSessionClientStatistic {
    NActors::TActorId ReadActorId;
    ui32 PartitionId = 0;
    i64 UnreadRows = 0;
    i64 UnreadBytes = 0;
    ui64 Offset = 0;
};

struct TopicSessionCommonStatistic {
    ui64 UnreadBytes = 0;
    ui64 RestartSessionByOffsets = 0;
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
