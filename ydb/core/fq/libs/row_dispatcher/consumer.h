#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/yql/dq/actors/compute/retry_queue.h>
#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>
#include <util/generic/queue.h>

namespace NFq {

struct Consumer  {

    Consumer(
        NActors::TActorId readActorId,
        NActors::TActorId selfId,
        ui64 eventQueueId,
        NFq::NRowDispatcherProto::TEvAddConsumer& proto)
        : ReadActorId(readActorId)
        , SourceParams(proto.GetSource())
        , PartitionId(proto.GetPartitionId())
        , Offset(proto.HasOffset() ? TMaybe<ui64>(proto.GetOffset()) : TMaybe<ui64>())
        , StartingMessageTimestampMs(proto.GetStartingMessageTimestampMs())
        , Proto(proto) {

        EventsQueue.Init("txId", selfId, selfId, eventQueueId, /* KeepAlive */ true);
        EventsQueue.OnNewRecipientId(readActorId);
    }

    NActors::TActorId ReadActorId;

    NYql::NPq::NProto::TDqPqTopicSource SourceParams;
    ui64 PartitionId;
    TString Token;
    bool AddBearerToToken;
    TMaybe<ui64> Offset;
    ui64 StartingMessageTimestampMs;

    NYql::NDq::TRetryEventsQueue EventsQueue;
    TQueue<std::pair<ui64, TString>> Buffer;

    NFq::NRowDispatcherProto::TEvAddConsumer Proto;
};
} // namespace NFq
