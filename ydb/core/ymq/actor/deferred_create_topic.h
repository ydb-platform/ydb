#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr::NSQS {

struct TTransactionCounters;

/// Creates PersQueue group under the queue version directory, then sets TopicCreated in metadata.
NActors::IActor* CreateDeferredCreateTopicActor(
    const NActors::TActorId& sqsServiceId,
    TString userName,
    TString queueName,
    TString folderId,
    bool isFifo,
    ui64 version,
    ui32 tablesFormat,
    TIntrusivePtr<TTransactionCounters> transactionCounters
);

} // namespace NKikimr::NSQS
