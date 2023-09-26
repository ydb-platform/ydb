#pragma once

#include <library/cpp/actors/core/actor.h>
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
    TVector<TPartitionFetchRequest> Partitions;
    TMaybe<NACLib::TUserToken> User;
    ui64 MaxWaitTimeMs;
    ui64 TotalMaxBytes;

    ui64 RequestId = 0;
    TFetchRequestSettings(
            const TString& database, const TVector<TPartitionFetchRequest>& partitions, ui64 maxWaitTimeMs, ui64 totalMaxBytes,
            const TMaybe<NACLib::TUserToken>& user = {}, ui64 requestId = 0
    )
        : Database(database)
        , Partitions(partitions)
        , User(user)
        , MaxWaitTimeMs(maxWaitTimeMs)
        , TotalMaxBytes(totalMaxBytes)
        , RequestId(requestId)
    {}
};

NActors::IActor* CreatePQFetchRequestActor(const TFetchRequestSettings& settings, const NActors::TActorId& schemeCache,
                                           const NActors::TActorId& requester);

} // namespace NKikimr::NPQ
