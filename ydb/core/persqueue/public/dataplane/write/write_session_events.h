#pragma once

#include "events.h"

#include <ydb/core/persqueue/public/pq_rl_helpers.h>

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>

#include <optional>

namespace NKikimr::NPQ::NDataplane::NWrite {

struct TWriteSessionProtocolOpts {
    TString Name;
    TString CounterName;
    TString SessionSpanName;
    TString ChildSpanNameSuffix;
    bool AttachRequestContextToPartitionWriter = false;
    bool SetDisableDeduplicationWhenUnused = false;
    ui32 CodecCounterIndexOffset = 0;
};

struct TWriteSessionSettings {
    NActors::TActorId Owner;
    ui64 Cookie = 0;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    std::optional<TString> ClientDC;
    NPersQueue::TTopicsListController TopicsController;
    TWriteSessionProtocolOpts Protocol;
    TString UserAgent;
    TString SdkBuildInfo;
    std::optional<TString> DatabaseName;
    TString SerializedToken;
    std::optional<TString> YdbToken;
    std::optional<TString> TraceId;
    std::optional<TString> RequestType;
    NWilson::TTraceId WilsonTraceId;
    TRlContext RlContext;
};

} // namespace NKikimr::NPQ::NDataplane::NWrite
