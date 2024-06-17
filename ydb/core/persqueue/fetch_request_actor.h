#pragma once

#include <ydb/core/persqueue/pq_rl_helpers.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/aclib/aclib.h>

#include "user_info.h"

namespace NKikimr::NPQ {

struct TPartitionFetchRequest {
    TString Topic;
    TString ClientId;
    ui32 Partition;
    ui64 Offset;
    ui64 MaxBytes;
    ui64 ReadTimestampMs;

    TPartitionFetchRequest(const TString& topic, ui32 partition, ui64 offset, ui64 maxBytes, ui64 readTimestampMs = 0, const TString& clientId = NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER)
        : Topic(topic)
        , ClientId(clientId)
        , Partition(partition)
        , Offset(offset)
        , MaxBytes(maxBytes)
        , ReadTimestampMs(readTimestampMs)
    {}

    TPartitionFetchRequest() = default;
};

struct TFetchRequestSettings {
    TString Database;
    TVector<TPartitionFetchRequest> Partitions;
    TMaybe<NACLib::TUserToken> User;
    ui64 MaxWaitTimeMs;
    ui64 TotalMaxBytes;
    TRlContext RlCtx;
    bool RuPerRequest;

    ui64 RequestId = 0;
    TFetchRequestSettings(
            const TString& database, const TVector<TPartitionFetchRequest>& partitions, ui64 maxWaitTimeMs, ui64 totalMaxBytes, TRlContext rlCtx,
            const TMaybe<NACLib::TUserToken>& user = {}, ui64 requestId = 0, bool ruPerRequest = false
    )
        : Database(database)
        , Partitions(partitions)
        , User(user)
        , MaxWaitTimeMs(maxWaitTimeMs)
        , TotalMaxBytes(totalMaxBytes)
        , RlCtx(rlCtx)
        , RuPerRequest(ruPerRequest)
        , RequestId(requestId)
    {}
};

NActors::IActor* CreatePQFetchRequestActor(const TFetchRequestSettings& settings, const NActors::TActorId& schemeCache,
                                           const NActors::TActorId& requester);

} // namespace NKikimr::NPQ
