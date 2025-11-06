#pragma once

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NPQ {

struct TPartitionFetchRequest {
    TString Topic;
    ui32 Partition;
    ui64 Offset;
    ui64 MaxBytes;
    ui64 ReadTimestampMs;

    TPartitionFetchRequest(const TString& topic, ui32 partition, ui64 offset, ui64 maxBytes, ui64 readTimestampMs = 0)
        : Topic(topic)
        , Partition(partition)
        , Offset(offset)
        , MaxBytes(maxBytes)
        , ReadTimestampMs(readTimestampMs)
    {}

    TPartitionFetchRequest() = default;
};

struct TFetchRequestSettings {
    TString Database;
    TString Consumer;
    TVector<TPartitionFetchRequest> Partitions;
    ui64 MaxWaitTimeMs = 0;
    ui64 TotalMaxBytes = 0;

    bool RuPerRequest = false;
    ui64 RequestId = 0;

    TRlContext RlCtx = {};
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken = {};
};

NActors::IActor* CreatePQFetchRequestActor(const TFetchRequestSettings& settings, const NActors::TActorId& schemeCache,
                                           const NActors::TActorId& requester);

} // namespace NKikimr::NPQ
