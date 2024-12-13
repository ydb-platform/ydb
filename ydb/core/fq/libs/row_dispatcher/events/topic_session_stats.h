#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <util/generic/vector.h>

namespace NFq {

struct TTopicSessionClientStatistic {
    NActors::TActorId ReadActorId;
    ui32 PartitionId = 0;
    i64 UnreadRows = 0;         // Current value
    i64 UnreadBytes = 0;        // Current value
    ui64 Offset = 0;            // Current value
    ui64 ReadBytes = 0;         // Increment / filtered
    bool IsWaiting = false;     // Current value
    i64 ReadLagMessages = 0;    // Current value
    ui64 InitialOffset = 0;
    void Add(const TTopicSessionClientStatistic& stat) {
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

struct TParserStatistic {
    TDuration ParserLatency;

    void Add(const TParserStatistic& stat) {
        ParserLatency = stat.ParserLatency != TDuration::Zero() ? stat.ParserLatency : ParserLatency;
    }
};

struct TFiltersStatistic {
    TDuration FilterLatency;

    void Add(const TFiltersStatistic& stat) {
        FilterLatency = stat.FilterLatency != TDuration::Zero() ? stat.FilterLatency : FilterLatency;
    }
};

struct TFormatHandlerStatistic {
    TDuration ParseAndFilterLatency;

    TParserStatistic ParserStats;
    TFiltersStatistic FilterStats;

    void Add(const TFormatHandlerStatistic& stat) {
        ParseAndFilterLatency = stat.ParseAndFilterLatency != TDuration::Zero() ? stat.ParseAndFilterLatency : ParseAndFilterLatency;

        ParserStats.Add(stat.ParserStats);
        FilterStats.Add(stat.FilterStats);
    }
};

struct TTopicSessionCommonStatistic {
    ui64 UnreadBytes = 0;   // Current value
    ui64 RestartSessionByOffsets = 0;
    ui64 ReadBytes = 0;     // Increment
    ui64 ReadEvents = 0;    // Increment
    ui64 LastReadedOffset = 0;

    std::unordered_map<TString, TFormatHandlerStatistic> FormatHandlers;

    void Add(const TTopicSessionCommonStatistic& stat) {
        UnreadBytes = stat.UnreadBytes;
        RestartSessionByOffsets = stat.RestartSessionByOffsets;
        ReadBytes += stat.ReadBytes;
        ReadEvents += stat.ReadEvents;
        LastReadedOffset = stat.LastReadedOffset;

        for (const auto& [formatName, foramtStats] : stat.FormatHandlers) {
            FormatHandlers[formatName].Add(foramtStats);
        }
    }

    void Clear() {
        ReadBytes = 0;
        ReadEvents = 0;
    }
};

struct TTopicSessionParams {
    TString ReadGroup;
    TString Endpoint;
    TString Database;
    TString TopicPath;
    ui64 PartitionId = 0;
};

struct TTopicSessionStatistic {
    TTopicSessionParams SessionKey; 
    std::vector<TTopicSessionClientStatistic> Clients;
    TTopicSessionCommonStatistic Common;
};

} // namespace NFq
